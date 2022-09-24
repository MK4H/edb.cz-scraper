import re
import csv

from typing import List

import bs4
import httpx

from http_session import HTTPSession


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


mailto_regex = re.compile("^mailto:")
def scrape_contacts_v3(contacts_page: bs4.BeautifulSoup) -> List[str]:
    if contacts_page.find(string="Živnost subjektu byla ukončena.") is not None:
        return []

    name_elem = contacts_page.find(id="h1Nadpis", attrs={"itemprop": "legalName"})
    contacts_div = name_elem.parent.parent
    email_anchors = contacts_div.find_all(name="a", href=mailto_regex)
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
