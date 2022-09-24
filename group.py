import random
import re
import sys
import csv
import json
from pathlib import Path
from typing import List, Optional, Dict, Set

import bs4
import httpx

import argparse

from http_session import HTTPSession
from company import Company

COMPANIES_PER_PAGE = 25

contacts_regex = re.compile("/kontakt$")
class Group:
    def __init__(self, name: str, url: str, session: HTTPSession, group_cache: Dict[str, int]):
        self.name = name
        self.url = url
        self.session = session
        self.num_pages: Optional[int] = group_cache.get(name, None)
        # Validate the cache, checking if the number of pages did not change from previous run
        if self.num_pages is not None and self.get_num_companies_on_page(self.num_pages) == 0:
            self.num_pages = None

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

    def get_company_urls_on_page(self, page: int, exclude_set: Set[str]) -> List[str]:
        company_divs = self._get_company_divs(page)
        company_urls = []
        for div in company_divs:
            contact_url_elem = div.find(name="a", href=contacts_regex)
            if contact_url_elem is None:
                print(f"No contact url found in {div.prettify()}", file=sys.stderr)
                return company_urls

            company_url = contact_url_elem["href"]
            if company_url not in exclude_set:
                company_urls.append(company_url)

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

    def get_random_company(self, exclude_set: Set[str]) -> Optional[Company]:
        num_pages = self.get_num_pages()
        page = random.randint(1, num_pages)
        company_urls = self.get_company_urls_on_page(page, exclude_set)
        if len(company_urls) != 0:
            company_url = random.choice(company_urls)
            return Company.scrape_company(self.session, company_url, self)
        else:
            return None

    def get_total_companies(self) -> int:
        num_last_page_companies = self.get_num_companies_on_page(self.get_num_pages())

        return (self.get_num_pages() - 1) * COMPANIES_PER_PAGE + num_last_page_companies


def scrape_groups(session: HTTPSession, group_cache: Path) -> List[Group]:
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

    return [Group(group.string, group.a["href"], session, group_map) for group in groups]


def num_total_groups(group_cache_path: Path):
    with httpx.Client() as client:
        session = HTTPSession(client, 0)
        groups = scrape_groups(session, group_cache_path)
        print(len(groups))


def num_cached_groups(group_cache_path: Path):
    with group_cache_path.open("r") as f:
        group_cache = json.load(f)
        print(len(group_cache))


num_fnc = {
        "total": num_total_groups,
        "cached": num_cached_groups
    }


def _num_groups(args: argparse.Namespace):
    assert args.type in num_fnc
    num_fnc[args.type](args.group_cache)


def get_group_size(group_cache_path: Path, output_path: Path, requests_per_second: int, fill_cache: bool):
    with httpx.Client() as client:
        session = HTTPSession(client, 1 / requests_per_second)
        groups = scrape_groups(session, group_cache_path)
        if not fill_cache:
            groups = [group for group in groups if group.num_pages is not None]

        group_sizes = []
        for idx, group in enumerate(groups):
            print(f"[{idx + 1}/{len(groups)}]: {group.name}")
            group_sizes.append({"group": group.name, "num_companies": group.get_total_companies()})

        with output_path.open("w") as f:
            out_csv = csv.DictWriter(f, fieldnames=["group", "num_companies"])
            for group_size in group_sizes:
                out_csv.writerow(group_size)

        if fill_cache:
            group_cache = {}
            for group in groups:
                if group.num_pages is not None:
                    group_cache[group.name] = group.num_pages

            with group_cache_path.open("w") as f:
                json.dump(group_cache, f)


def _get_group_size(args: argparse.Namespace):
    get_group_size(args.group_cache, args.output, args.limit, args.fill)


def main():
    parser = argparse.ArgumentParser(description="Info for groups from https://www.edb.cz/katalog-firem/")
    parser.add_argument("-l", "--limit", type=int, default=10, help="Max number of HTTP requests per second")
    parser.add_argument("-g", "--group_cache", type=Path, default="groups.json", help="Cache file with number of pages in each group to try load and store when ending")

    subparsers = parser.add_subparsers()
    num_groups_parser = subparsers.add_parser("num")
    num_groups_parser.add_argument("type", choices=num_fnc.keys(), help="The type of group to count")
    num_groups_parser.set_defaults(action=_num_groups)

    group_size_parser = subparsers.add_parser("size")
    group_size_parser.add_argument("-f", "--fill", action="store_true", help="If uncached groups should be filled, otherwise they will be filtered out")
    group_size_parser.add_argument("-o", "--output", type=Path, default="group_size.csv", help="Output file path")
    group_size_parser.set_defaults(action=_get_group_size)

    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
