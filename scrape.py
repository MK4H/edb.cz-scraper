import random
import re
import sys
import csv
import time
import json
from pathlib import Path
from typing import List, Optional, Dict

import bs4
import httpx

import argparse


class HTTPSession:
    def __init__(self, client: httpx.Client, delay: float):
        self.c = client
        self.delay = delay

    def delayed_get(self, url: str, params=None) -> httpx.Response:
        time.sleep(self.delay)
        return self.get(url, params)

    def get(self, url: str, params=None) -> httpx.Response:
        return self.c.get(url, params=params)


def scrape_contacts_v1(contacts_page: bs4.BeautifulSoup) -> List[str]:
    contact_table = contacts_page.find(class_="contact-table")
    email_spans = contact_table.find_all(attrs={"itemprop": "email"})
    # TODO: Maybe include the employee emails

    company_emails = [email_ref["href"] for email_span in email_spans for email_ref in email_span.find_all(name="a")]

    return company_emails


def scrape_contacts_v2(contacts_page: bs4.BeautifulSoup) -> List[str]:
    div_contacts = contacts_page.find(id="divContacts")
    email_spans = div_contacts.find_all(attrs={"itemprop": "email"})
    company_emails = [email_ref["href"] for email_span in email_spans for email_ref in email_span.find_all(name="a")]

    return company_emails


def scrape_contacts_v3(contacts_page: bs4.BeautifulSoup) -> List[str]:
    if contacts_page.find(string="Živnost subjektu byla ukončena.") is not None:
        return []

    name_elem = contacts_page.find(id="h1Nadpis", attrs={"itemprop": "legalName"})
    contacts_div = name_elem.parent.parent
    email_anchors = contacts_div.find_all(name="a", href=re.compile("^mailto:"))
    emails = [anchor["href"] for anchor in email_anchors]
    return emails


class Company:
    def __init__(self, name: str, group: "Group", contacts_url: str, emails: List[str]):
        assert (name is not None)
        self.name = name
        self.group = group
        self.contacts_url = contacts_url
        self.emails = emails

    @classmethod
    def scrape_company(cls, session: HTTPSession, contact_url: str, group: "Group") -> "Company":
        resp = session.delayed_get(contact_url)
        if resp.status_code != httpx.codes.OK:
            # TODO: Log error
            raise Exception(f"Contact request failed with {resp.status_code}, url {resp.url}, {resp.request.url} {resp.request.headers}")

        # There are three types of contact pages for some reason, so we need to distinguish between them
        #   and scrape each one differently
        content = bs4.BeautifulSoup(resp.text, "lxml")
        name_elem = content.find(id="h3CompanyName")
        if name_elem is not None:
            name = name_elem.string
            emails = scrape_contacts_v1(content)
            return Company(name, group, contact_url, emails)

        div_contacts = content.find(id="divContacts")
        if div_contacts is not None:
            header_elem = div_contacts.find(name="h1")
            if header_elem is None:
                raise Exception(f"ERROR: Matched invalid v2 format, missing H1: {contact_url}")
            name_elem = header_elem.find(name="span")
            if name_elem is None:
                raise Exception(f"ERROR: Matched invalid v2 format, missing name span: {contact_url}")

            name = name_elem.string
            emails = scrape_contacts_v2(content)
            return Company(name, group, contact_url, emails)

        name_elem = content.find(id="h1Nadpis", attrs={"itemprop": "legalName"})
        if name_elem is not None:
            if len(name_elem.contents) == 1:
                name = name_elem.string
            else:
                # TODO: Include the possible </br> elements
                name = " ".join(name_elem.stripped_strings)
            emails = scrape_contacts_v3(content)
            return Company(name, group, contact_url, emails)

        raise Exception(f"Unknown format for {contact_url}")

    def to_csv(self, csv_out: csv.DictWriter):
        for email in self.emails:
            # TODO: Properly parse URL
            if email.startswith("mailto:"):
                email = email[len("mailto:"):]
            csv_out.writerow({"name": self.name, "email": email, "group": self.group.name, "url": self.contacts_url})


class Group:
    def __init__(self, name: str, url: str, session: HTTPSession, group_cache: Dict[str, int]):
        self.name = name
        self.url = url
        self.session = session
        self.num_pages: Optional[int] = group_cache.get(name, None)

    def _get_company_divs(self, page: int) -> bs4.element.ResultSet:
        resp = self.session.delayed_get(self.url, params={"p": page})
        if resp.status_code != httpx.codes.OK:
            raise Exception(f"{self.name}: Page {page} request failed with {resp.status_code}, url {resp.url}")

        content = bs4.BeautifulSoup(resp.text, "lxml")
        firmy_div = content.find(id="divFirmy")
        if firmy_div is None:
            raise Exception(f"{self.name}: Page {page} malformed, no divFirmy, url {self.url}")

        return firmy_div.find_all(name="div", attrs={"itemtype": "https://schema.org/Organization"})

    def get_num_companies_on_page(self, page: int) -> int:
        return len(self._get_company_divs(page))

    def get_company_urls_on_page(self, page: int) -> List[str]:
        company_divs = self._get_company_divs(page)
        company_urls = []
        for div in company_divs:
            contact_url_elem = div.find(name="a", href=re.compile("/kontakt$"))
            if contact_url_elem is None:
                print(f"No contact url found in {div.prettify()}", file=sys.stderr)
                return company_urls

            company_urls.append(contact_url_elem["href"])

        return company_urls

    def get_num_pages(self) -> int:
        if self.num_pages is not None:
            return self.num_pages

        max_page_lower = 1
        max_page_upper = 50

        # Find upper bound for max page
        while self.get_num_companies_on_page(max_page_upper) > 0:
            max_page_upper += 50

        iter = 0
        while max_page_lower <= max_page_upper:
            mid_page = (max_page_upper + max_page_lower) // 2
            if self.get_num_companies_on_page(mid_page) > 0:
                max_page_lower = mid_page + 1
            else:
                max_page_upper = mid_page - 1

            iter += 1
            # Just in case we fucked up
            if iter > 200:
                raise Exception(f"Too many iterations of the binary search: {iter}, Group: {self.url}, Stuck on: [{max_page_lower}, {max_page_upper}]")

        self.num_pages = max_page_upper
        return self.num_pages

    def get_random_company(self) -> Company:
        num_pages = self.get_num_pages()
        page = random.randint(1, num_pages)
        company_urls = self.get_company_urls_on_page(page)
        if len(company_urls) == 0:
            raise Exception(f"{self.name} changed number of pages between requests, chosen page {page} does not exist")

        company_url = random.choice(company_urls)
        return Company.scrape_company(self.session, company_url, self)


class Sampler:
    def __init__(self, session: HTTPSession, groups: List[Group], num_retries: int = 10):
        self.session = session
        self.groups = groups
        self.num_retries = num_retries

    @classmethod
    def scrape_groups(cls, session: HTTPSession, group_cache: Path) -> "Sampler":
        resp = session.delayed_get("https://www.edb.cz/katalog-firem/")
        if resp.status_code != httpx.codes.OK:
            raise Exception(f"Failed to get catalog: {resp.status_code}, {resp.text}")

        catalog_page = bs4.BeautifulSoup(resp.text, "lxml")
        columns = catalog_page.find_all(name="div", class_="col")
        groups = [child for column in columns for child in column.find_all(name="h3")]
        try:
            with group_cache.open("r") as f:
                group_map = json.load(f)
        except IOError:
            group_map = {}

        return Sampler(session, [Group(group.string, group.a["href"], session, group_map) for group in groups])

    def get_sample(self) -> Optional[Company]:
        group = random.choice(self.groups)
        for retry in range(self.num_retries):
            try:
                company = group.get_random_company()
                if len(company.emails) != 0:
                    return company
                else:
                    print(f"Sample failed as company {company.name} has no emails", file=sys.stderr)
                    if retry + 1 == self.num_retries:
                        print(f"Sample failed as group {group.name} has too many companies without emails")
                        return None
            # We sometimes get a timeout, so wait a minute and retry
            except httpx.TimeoutException as e:
                print(f"Sample failed due to timeout {e}, waiting a minute and retrying", file=sys.stderr)
                time.sleep(60)
            except Exception as e:
                print(f"Sample failed due to {e}", file=sys.stderr)

        raise Exception(f"Failed in all {self.num_retries} retries")

    def store_group_cache(self, path: Path):
        group_cache = {}
        for group in self.groups:
            if group.num_pages is not None:
                group_cache[group.name] = group.num_pages

        with path.open("w") as f:
            json.dump(group_cache, f)


def run_sampling(requested_samples: int, append: bool, output_path: Path, requests_per_second: int, group_cache: Path):
    output_exists = output_path.exists()
    with output_path.open("a" if append else "w", newline="") as email_out:
        email_out_csv = csv.DictWriter(email_out, fieldnames=["name", "email", "group", "url"])
        if not output_exists or not append:
            email_out_csv.writeheader()

        with httpx.Client() as client:
            sampler = Sampler.scrape_groups(HTTPSession(client, 1 / requests_per_second), group_cache)
            try:
                collected_samples = 0
                while collected_samples < requested_samples:
                    print(f"[{collected_samples}]: ", end="")
                    company = sampler.get_sample()
                    if company is None:
                        continue
                    company.to_csv(email_out_csv)
                    collected_samples += 1
                    print(f"{company.name} with {len(company.emails)} email{'' if len(company.emails) == 1 else 's'}")
            finally:
                sampler.store_group_cache(group_cache)


def main():
    parser = argparse.ArgumentParser(description="Sampling of https://www.edb.cz/katalog-firem/")
    parser.add_argument("-a", "--append", action="store_true", help="If output should be appended to existing file")
    parser.add_argument("-o", "--output", type=Path, default="contacts.csv", help="Output file path")
    parser.add_argument("-l", "--limit", type=int, default=10, help="Max number of HTTP requests per second")
    parser.add_argument("-g", "--group_cache", type=Path, default="groups.json", help="Cache file with number of pages in each group to try load and store when ending")
    parser.add_argument("num_samples", type=int, help="Number of samples to gather")

    args = parser.parse_args()
    run_sampling(args.num_samples, args.append, args.output, args.limit, args.group_cache)


if __name__ == "__main__":
    main()
