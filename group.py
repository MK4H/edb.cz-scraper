import random
import re
import sys
import csv
import json
import time
from pathlib import Path
from typing import List, Optional, Dict, Set

import bs4
import httpx

import argparse

from http_session import HTTPSession
from company import Company

COMPANIES_PER_PAGE = 25

class GroupId:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url

contacts_regex = re.compile("/kontakt$")
class Group:
    def __init__(self, name: str, url: str, session: HTTPSession, group_cache: Dict[str, int]):
        self.name = name
        self.url = url
        self.session = session
        self.num_pages: Optional[int] = group_cache.get(name, None)

    def validate_num_pages(self) -> bool:
        while True:
            try:
                # Validate the cache, checking if the number of pages did not change from previous run
                if self.num_pages is not None and self._is_num_pages_valid(self.num_pages):
                    return True
                else:
                    self.num_pages = None
                    return False
            except httpx.TimeoutException:
                print("Error: Validation request timeout, retrying")
                time.sleep(60)

    def _is_num_pages_valid(self, expected_num_pages: int) -> bool:
        return self.get_num_companies_on_page(expected_num_pages) != 0 and self.get_num_companies_on_page(expected_num_pages + 1) == 0

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
    try:
        with group_cache.open("r") as f:
            group_map = json.load(f)
    except IOError:
        group_map = {}

    group_ids = list_group_ids(session)
    return [Group(group_id.name, group_id.url, session, group_map) for group_id in group_ids]


def list_group_ids(session: HTTPSession) -> List[GroupId]:
    resp = session.delayed_get("https://www.edb.cz/katalog-firem/")
    if resp.status_code != httpx.codes.OK:
        raise Exception(f"Failed to get catalog: {resp.status_code}, {resp.text}")

    catalog_page = bs4.BeautifulSoup(resp.text, "lxml")
    columns = catalog_page.find_all(name="div", class_="col")
    groups = [child for column in columns for child in column.find_all(name="h3")]
    return [GroupId(group.string, group.a["href"]) for group in groups]


def num_site_groups(requests_per_second: int):
    with httpx.Client() as client:
        session = HTTPSession(client, 1 / requests_per_second)
        groups = list_group_ids(session)
        print(len(groups))


def num_cached_groups(group_cache_path: Path):
    with group_cache_path.open("r") as f:
        group_cache = json.load(f)
        print(len(group_cache))


def _num_site_groups(args: argparse.Namespace):
    num_site_groups(args.limit)


def _num_cached_groups(args: argparse.Namespace):
    num_cached_groups(args.group_cache)


def get_groups(session: HTTPSession, group_cache_path: Path, with_cache_size_only: bool) -> List[Group]:
    groups = scrape_groups(session, group_cache_path)
    if with_cache_size_only:
        return [group for group in groups if group.num_pages is not None]
    else:
        return groups


def fill_group_cache(groups: List[Group], group_cache_path: Path):
    group_cache = {}
    for idx, group in enumerate(groups):
        print(f"[{idx + 1}/{len(groups)}] Filling {group.name}")
        filled = False
        while not filled:
            try:
                group_cache[group.name] = group.get_num_pages()
                filled = True
            except httpx.TimeoutException:
                print("Error: Request timeout when filling cache, retrying")
                time.sleep(60)

    with group_cache_path.open("w", encoding="utf8") as f:
        json.dump(group_cache, f, ensure_ascii=False)


def get_group_size(group_cache_path: Path, output_path: Path, requests_per_second: int, fill_cache: bool, validate_cache: bool):
    with httpx.Client() as client:
        session = HTTPSession(client, 1 / requests_per_second)
        groups = get_groups(session, group_cache_path, not fill_cache)

        if validate_cache:
            for idx, group in enumerate(groups):
                print(f"[{idx + 1}/{len(groups)}]: Validating {group.name}")
                group.validate_num_pages()

        group_sizes = []
        for idx, group in enumerate(groups):
            print(f"[{idx + 1}/{len(groups)}]: {group.name}")
            num_companies = None
            while num_companies is None:
                try:
                    num_companies = group.get_total_companies()
                except httpx.TimeoutException:
                    print("Error: Request timeout when getting number of companies in a group, retrying")

            group_sizes.append({"group": group.name, "num_companies": group.get_total_companies()})

        with output_path.open("w") as f:
            out_csv = csv.DictWriter(f, fieldnames=["group", "num_companies"])
            for group_size in group_sizes:
                out_csv.writerow(group_size)

        if fill_cache:
            fill_group_cache(groups, group_cache_path)


def _get_group_size(args: argparse.Namespace):
    get_group_size(args.group_cache, args.output, args.limit, args.fill, args.validate)


def validate_group_cache(group_cache_path: Path, requests_per_second: int, fill_cache: bool):
    with httpx.Client() as client:
        session = HTTPSession(client, 1 / requests_per_second)
        groups = get_groups(session, group_cache_path, not fill_cache)
        for idx, group in enumerate(groups):
            was_cached = group.num_pages is not None
            is_valid = group.validate_num_pages()
            progress = f"[{idx + 1}/{len(groups)}]"
            if is_valid:
                print(f"{progress} Valid: {group.name}")
            elif was_cached:
                print(f"{progress} Invalid: {group.name}")
            else:
                print(f"{progress} Not cached: {group.name}")

        if fill_cache:
            fill_group_cache(groups, group_cache_path)


def _validate_group_cache(args: argparse.Namespace):
    validate_group_cache(args.group_cache, args.limit, args.fill)


def main():
    parser = argparse.ArgumentParser(description="Info for groups from https://www.edb.cz/katalog-firem/")
    parser.add_argument("-l", "--limit", type=int, default=10, help="Max number of HTTP requests per second")
    parser.add_argument("-g", "--group_cache", type=Path, default="groups.json", help="Cache file with number of pages in each group to try load and store when ending")

    subparsers = parser.add_subparsers()
    num_groups_parser = subparsers.add_parser("num")

    num_groups_subparsers = num_groups_parser.add_subparsers()
    cached_group_parser = num_groups_subparsers.add_parser("cached", help="The number of groups in group cache")
    cached_group_parser.set_defaults(action=_num_cached_groups)

    site_group_parser = num_groups_subparsers.add_parser("site", help="List the number of groups on the website itself")
    site_group_parser.set_defaults(action=_num_site_groups)

    group_cache_validation_parser = subparsers.add_parser("validate")
    group_cache_validation_parser.add_argument("-f", "--fill", action="store_true", help="Fill cache for all empty and invalid groups")
    group_cache_validation_parser.set_defaults(action=_validate_group_cache)

    group_size_parser = subparsers.add_parser("size")
    group_size_parser.add_argument("-f", "--fill", action="store_true", help="If uncached groups should be filled, otherwise they will be filtered out")
    group_size_parser.add_argument("-v", "--validate", action="store_true", help="Cached group sizes will be validated before use")
    group_size_parser.add_argument("-o", "--output", type=Path, default="group_size.csv", help="Output file path")
    group_size_parser.set_defaults(action=_get_group_size)

    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
