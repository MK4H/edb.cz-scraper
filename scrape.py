import random
import re
import sys
import csv
import time
import json
from pathlib import Path
from typing import List, Optional, Dict, Set

import bs4
import httpx

import argparse

from http_session import HTTPSession
from group import Group
from company import Company


class Sampler:
    def __init__(
            self,
            session: HTTPSession,
            groups: List[Group],
            exclude_set: Set[str],
            num_retries: int = 10
    ):
        self.session = session
        self.groups = groups
        self.exclude_set = exclude_set
        self.num_retries = num_retries

    @classmethod
    def scrape_groups(cls, session: HTTPSession, group_cache: Path, exclude_set: Set[str]) -> "Sampler":
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

        return Sampler(
            session,
            [Group(group.string, group.a["href"], session, group_map) for group in groups],
            exclude_set
        )

    def get_sample(self) -> Optional[Company]:
        group = random.choice(self.groups)
        for retry in range(self.num_retries):
            try:
                company = group.get_random_company()
                # We may have hit a page with all companies excluded
                if company is None:
                    continue

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


def load_exclude_set(exclude: Optional[Path]) -> Set[str]:
    if exclude is None:
        return set()
    with exclude.open("r") as f:
        reader = csv.DictReader(f)
        return set((row["url"] for row in reader))


def run_sampling(
        requested_samples: int,
        append: bool,
        output_path: Path,
        requests_per_second: int,
        group_cache: Path,
        exclude: Optional[Path]
):
    output_exists = output_path.exists()
    exclude_set = load_exclude_set(exclude)
    with output_path.open("a" if append else "w", newline="") as email_out:
        email_out_csv = csv.DictWriter(email_out, fieldnames=["name", "email", "group", "url"])
        if not output_exists or not append:
            email_out_csv.writeheader()

        with httpx.Client() as client:
            sampler = Sampler.scrape_groups(HTTPSession(client, 1 / requests_per_second), group_cache, exclude_set)
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
    parser.add_argument("-e", "--exclude", type=Path, help="CSV file containing companies to exclude")
    parser.add_argument("num_samples", type=int, help="Number of samples to gather")

    args = parser.parse_args()
    run_sampling(args.num_samples, args.append, args.output, args.limit, args.group_cache, args.exclude)


if __name__ == "__main__":
    main()
