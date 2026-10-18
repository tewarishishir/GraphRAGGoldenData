# SPDX-License-Identifier: MIT
"""
Generate a fully synthetic sample dataset in raw_data/.

All names, emails, phone numbers, addresses, IDs and financial figures are
randomly generated. Emails use example.com, websites use the reserved
.example TLD, and phone numbers use the fictional 555-01xx range.

The data is shaped to exercise the golden-record pipeline:
- Company clusters contain duplicate rows from different source systems,
  with gaps that the golden merge fills from sibling rows.
- Projects link to companies via COMPANY_ID_SRC; some projects are
  duplicated under the same DEDUP_CLUSTER_ID.
- Contacts link to companies via COMPANY_SOURCE_ID.

Run from project root:
    python scripts/generate_synthetic_data.py
    python golden_records.py
"""
import argparse
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import COMPANIES_CSV, PROJECTS_CSV, CONTACTS_CSV, RAW_DATA_DIR

SOURCE_SYSTEMS = ["crm", "erp", "platform_app"]

NAME_PREFIXES = [
    "Apex", "Summit", "Ironwood", "Bluestone", "Granite", "Keystone", "Cedar", "Harbor",
    "Northgate", "Redline", "Silverleaf", "Trident", "Vantage", "Westfield", "Cobalt",
    "Evergreen", "Falcon", "Horizon", "Landmark", "Meridian", "Pioneer", "Riverbend",
    "Sterling", "Timberline", "Unity", "Beacon", "Crescent", "Frontier", "Oakridge", "Pinnacle",
]
NAME_SUFFIXES = [
    "Builders", "Construction", "Contractors", "Engineering", "Development", "Structures",
    "Concrete", "Electric", "Mechanical", "Group", "Partners", "Steel Works",
]
LEGAL_FORMS = ["Inc", "LLC", "Co", "Ltd"]

CITIES = [
    ("Austin", "Texas"), ("Denver", "Colorado"), ("Phoenix", "Arizona"), ("Seattle", "Washington"),
    ("Atlanta", "Georgia"), ("Chicago", "Illinois"), ("Boston", "Massachusetts"),
    ("Nashville", "Tennessee"), ("Portland", "Oregon"), ("Charlotte", "North Carolina"),
    ("San Diego", "California"), ("Columbus", "Ohio"), ("Minneapolis", "Minnesota"),
    ("Salt Lake City", "Utah"), ("Tampa", "Florida"),
]
STREETS = ["Main St", "Oak Ave", "Industrial Blvd", "Commerce Dr", "Park Rd", "Lakeview Way", "Market St", "Elm St"]

COMPANY_TYPES = ["General Contractor", "Specialty Contractor", "Owner", "Architect / Engineer", "Developer"]
VERTICALS = ["Commercial", "Residential", "Healthcare", "Education", "Infrastructure", "Industrial"]
ACV_RANGES = [(0.0, 1.0, "<$1M"), (1.0, 5.0, "$1M-$5M"), (5.0, 20.0, "$5M-$20M")]

PROJECT_KINDS = [
    "Medical Office Building", "Elementary School", "Mixed-Use Tower", "Distribution Center",
    "Parking Structure", "Hospital Expansion", "Apartment Complex", "Bridge Rehabilitation",
    "Data Center", "Retail Center", "Library Renovation", "Water Treatment Plant",
]
PROJECT_STAGES = ["Pre-Construction", "Course of Construction", "Post-Construction", "Bidding", "Warranty"]
PROJECT_TYPES = ["New Construction", "Renovation", "Expansion", "Tenant Improvement"]
CONTRACT_TYPES = ["Lump Sum", "Cost Plus", "GMP", "Design-Build"]

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery", "Quinn", "Drew",
    "Sam", "Robin", "Cameron", "Reese", "Hayden", "Parker", "Rowan", "Emerson", "Skyler", "Logan",
]
LAST_NAMES = [
    "Rivera", "Chen", "Patel", "Okafor", "Nguyen", "Schmidt", "Garcia", "Kowalski", "Haddad", "Larsen",
    "Moreau", "Tanaka", "Silva", "Novak", "Brennan", "Iyer", "Mendez", "Fischer", "Abara", "Lindqvist",
]
JOB_TITLES = [
    "Project Manager", "Superintendent", "Estimator", "Project Engineer", "Owner",
    "VP of Operations", "Controller", "Safety Manager", "Procurement Lead", "Field Engineer",
]


def _phone(rng: random.Random) -> str:
    return f"+1-{rng.randint(200, 989)}-555-01{rng.randint(0, 99):02d}"


def _slug(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _maybe(rng: random.Random, value: str, p_blank: float) -> str:
    return "" if rng.random() < p_blank else value


def generate_companies(rng: random.Random, n_clusters: int) -> pd.DataFrame:
    rows = []
    used_names = set()
    next_id = 100000
    for _ in range(n_clusters):
        while True:
            base = f"{rng.choice(NAME_PREFIXES)} {rng.choice(NAME_SUFFIXES)}"
            if base not in used_names:
                used_names.add(base)
                break
        city, state = rng.choice(CITIES)
        lo, hi, acv_range = rng.choices(ACV_RANGES, weights=[5, 4, 2])[0]
        acv_usd = round(rng.uniform(lo, hi), 3)
        attrs = {
            "COMPANY_NAME": f"{base} {rng.choice(LEGAL_FORMS)}",
            "COMPANY_WEBSITE": f"https://www.{_slug(base)}.example",
            "COMPANY_PHONE_NUMBER": _phone(rng),
            "STREET_NAME": f"{rng.randint(10, 9999)} {rng.choice(STREETS)}",
            "CITY_NAME": city,
            "STATE_NAME": state,
            "COUNTRY_NAME": "United States",
            "COUNTRY_CODE": "US",
            "POSTAL_CODE": f"{rng.randint(10000, 99999)}",
            "COMPANY_TYPE": rng.choice(COMPANY_TYPES),
            "VERTICAL": rng.choice(VERTICALS),
            "ACV_RANGE": acv_range,
            "ACV_USD": f"{acv_usd}",
            "ESTIMATED_ACV": f"{int(acv_usd * 1_000_000 * rng.uniform(0.8, 1.3))}",
            "PREDICTED_ACV": f"{round(acv_usd * rng.uniform(0.9, 1.1), 3)}",
            "ANNUAL_REVENUE": f"{rng.randint(5, 900) * 1_000_000}",
        }

        cluster_size = rng.choices([1, 2, 3], weights=[5, 3, 2])[0]
        parent_id = f"CMP-{next_id}"
        systems = rng.sample(SOURCE_SYSTEMS, cluster_size)
        for i, system in enumerate(systems):
            source_id = f"CMP-{next_id}"
            next_id += 1
            row = {
                "SOURCE_ID": source_id,
                "DEDUP_PARENT_ID": parent_id,
                "COMPANY_ID_SRC": f"{system.upper()}-{rng.randint(1_000_000, 9_999_999)}",
                **attrs,
                "SOURCE_SYSTEM": system,
                "IS_DELETED": "false",
            }
            if i == 0:
                # Canonical row has gaps so the golden merge fills them from siblings.
                for col in ("COMPANY_WEBSITE", "COMPANY_PHONE_NUMBER", "VERTICAL"):
                    if cluster_size > 1 and rng.random() < 0.4:
                        row[col] = ""
            else:
                # Duplicates: slight name variation and sparser attributes.
                row["COMPANY_NAME"] = rng.choice([base, base.upper(), attrs["COMPANY_NAME"]])
                for col in ("STREET_NAME", "POSTAL_CODE", "ACV_USD", "ESTIMATED_ACV", "ANNUAL_REVENUE"):
                    row[col] = _maybe(rng, row[col], 0.5)
            rows.append(row)
    return pd.DataFrame(rows)


def generate_projects(rng: random.Random, companies: pd.DataFrame, n_projects: int) -> pd.DataFrame:
    company_ids = companies["COMPANY_ID_SRC"].tolist()
    # Skew so some companies have many projects and the "2+ projects" queries return results.
    weights = [rng.choice([1, 1, 2, 4, 8]) for _ in company_ids]
    rows = []
    today = date(2026, 1, 1)
    for i in range(n_projects):
        company_id = rng.choices(company_ids, weights=weights)[0]
        city, state = rng.choice(CITIES)
        start = today + timedelta(days=rng.randint(-900, 400))
        end = start + timedelta(days=rng.randint(180, 900))
        value = rng.randint(1, 250) * 100_000
        source_id = f"PRJ-{200000 + i}"
        cluster_id = source_id
        if rows and rng.random() < 0.1:
            # Duplicate of an earlier project from another source system.
            dup = rng.choice(rows)
            cluster_id = dup["DEDUP_CLUSTER_ID"]
            name, company_id = dup["PROJECT_NAME"], dup["COMPANY_ID_SRC"]
        else:
            name = f"{city} {rng.choice(PROJECT_KINDS)}"
            if rng.random() < 0.5:
                name += f" Phase {rng.randint(1, 3)}"
        rows.append({
            "SOURCE_ID": source_id,
            "PROJECT_ID_SRC": f"P-{rng.randint(1_000_000, 9_999_999)}",
            "COMPANY_ID_SRC": company_id,
            "PROJECT_NAME": name,
            "DESCRIPTION": f"Synthetic sample project: {name.lower()}.",
            "PROJECT_STAGE_STANDARD": rng.choice(PROJECT_STAGES),
            "PROJECT_TYPE": rng.choice(PROJECT_TYPES),
            "CONTRACT_TYPE": rng.choice(CONTRACT_TYPES),
            "ESTIMATED_VALUE": f"{value}",
            "CURRENCY": "USD",
            "ESTIMATED_VALUE_USD": f"{value}",
            "SQUARE_FEET": f"{rng.randint(5, 500) * 1000}",
            "STREET_NAME": f"{rng.randint(10, 9999)} {rng.choice(STREETS)}",
            "CITY_NAME": city,
            "STATE_NAME": state,
            "POSTAL_CODE": f"{rng.randint(10000, 99999)}",
            "COUNTRY_NAME": "United States",
            "SOURCE_SYSTEM": rng.choice(SOURCE_SYSTEMS),
            "IS_ACTIVE": rng.choices(["true", "false"], weights=[7, 3])[0],
            "ESTIMATED_START_DATE": start.isoformat(),
            "ESTIMATED_COMPLETION_DATE": end.isoformat(),
            "DEDUP_CLUSTER_ID": cluster_id,
        })
    return pd.DataFrame(rows)


def generate_contacts(rng: random.Random, companies: pd.DataFrame, n_contacts: int) -> pd.DataFrame:
    company_rows = companies[["SOURCE_ID", "COMPANY_NAME", "COMPANY_WEBSITE"]].to_dict("records")
    rows = []
    for i in range(n_contacts):
        company = rng.choice(company_rows)
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES)
        email_user = f"{first}.{last}{rng.randint(1, 99)}".lower()
        rows.append({
            "SOURCE_ID": f"CNT-{300000 + i}",
            "COMPANY_SOURCE_ID": company["SOURCE_ID"],
            "COMPANY": company["COMPANY_NAME"],
            "FULL_NAME": f"{first} {last}",
            "FIRST_NAME": first,
            "LAST_NAME": last,
            "CITY": city,
            "STATE_NAME": state,
            "COUNTRY_NAME": "United States",
            "EMAIL_ADDRESS": _maybe(rng, f"{email_user}@example.com", 0.15),
            "PHONE_NUMBER": _maybe(rng, _phone(rng), 0.3),
            "JOB_TITLE": rng.choice(JOB_TITLES),
            "SOURCE_SYSTEM": rng.choice(SOURCE_SYSTEMS),
            "IS_ACTIVE": rng.choices(["true", "false"], weights=[9, 1])[0],
            "IS_VALID_CONTACT": "true",
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic sample data in raw_data/.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clusters", type=int, default=30, help="Number of company clusters.")
    parser.add_argument("--projects", type=int, default=150)
    parser.add_argument("--contacts", type=int, default=400)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    companies = generate_companies(rng, args.clusters)
    projects = generate_projects(rng, companies, args.projects)
    contacts = generate_contacts(rng, companies, args.contacts)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    companies.to_csv(COMPANIES_CSV, index=False)
    projects.to_csv(PROJECTS_CSV, index=False)
    contacts.to_csv(CONTACTS_CSV, index=False)
    print(f"Companies: {len(companies)} rows in {companies['DEDUP_PARENT_ID'].nunique()} clusters -> {COMPANIES_CSV}")
    print(f"Projects:  {len(projects)} rows -> {PROJECTS_CSV}")
    print(f"Contacts:  {len(contacts)} rows -> {CONTACTS_CSV}")


if __name__ == "__main__":
    main()
