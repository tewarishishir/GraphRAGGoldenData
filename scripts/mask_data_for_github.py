"""
Mask and pseudonymize raw_data and golden CSVs for safe GitHub publication.

- Renames Procore-related columns: PROCORE_ID -> SOURCE_ID, etc.
- Replaces SOURCE_SYSTEM values: procore_app -> platform_app, etc.
- Deterministic PII pseudonymization (same value -> same pseudonym across all files).
- Use MASK_SALT env var for reproducible runs (default: fixed salt in script).

Run from project root: python scripts/mask_data_for_github.py
Writes masked CSVs in place. Back up originals before first run.
"""
import hashlib
import os
import sys
from pathlib import Path

import pandas as pd

# Project root (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    RAW_DATA_DIR,
    OUTPUT_DIR,
    COMPANIES_CSV,
    PROJECTS_CSV,
    CONTACTS_CSV,
)

# Column renames (Procore -> generic)
COLUMN_RENAMES = {
    "PROCORE_ID": "SOURCE_ID",
    "DEDUP_PARENT_PROCORE_ID": "DEDUP_PARENT_ID",
    "COMPANY_PROCORE_ID": "COMPANY_SOURCE_ID",
}

# SOURCE_SYSTEM value replacements
SOURCE_SYSTEM_REPLACEMENTS = {
    "procore_app": "platform_app",
    "procore_app_vendors": "platform_app_vendors",
}

# PII columns by entity (values will be deterministically pseudonymized)
PII_CONTACTS = [
    "FULL_NAME", "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE_NAME",
    "COUNTRY_NAME", "POSTAL_CODE", "EMAIL_ADDRESS", "PHONE_NUMBER", "MOBILE_PHONE", "COMPANY",
    "WEBSITE",  # may contain emails or vendor URLs
]
PII_COMPANIES = [
    "COMPANY_NAME", "STREET_NAME", "CITY_NAME", "STATE_NAME", "COUNTRY_NAME",
    "POSTAL_CODE", "COMPANY_PHONE_NUMBER", "COMPANY_WEBSITE",
]
PII_PROJECTS = [
    "PROJECT_NAME", "DESCRIPTION", "STREET_NAME", "CITY_NAME", "STATE_NAME",
    "POSTAL_CODE", "COUNTRY_NAME",
]

# ID columns to pseudonymize (deterministic so links stay valid)
ID_COLUMNS = ["SOURCE_ID", "DEDUP_PARENT_ID", "COMPANY_SOURCE_ID", "DEDUP_CLUSTER_ID"]


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False, dtype=str, keep_default_na=False)


def _get_salt() -> str:
    return os.environ.get("MASK_SALT", "graphrag-mask-2025")


def _pseudonymize_value(value: str, salt: str, prefix: str, cache: dict) -> str:
    """Deterministic pseudonym: same value -> same pseudonym. Uses prefix for readability."""
    if not value or not str(value).strip():
        return ""
    key = (prefix, str(value).strip())
    if key not in cache:
        h = hashlib.sha256((salt + key[1]).encode()).hexdigest()[:10]
        cache[key] = f"{prefix}_{h}"
    return cache[key]


def _apply_column_renames(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for old, new in COLUMN_RENAMES.items():
        if old in out.columns:
            out = out.rename(columns={old: new})
    return out


def _apply_source_system_replacements(df: pd.DataFrame) -> pd.DataFrame:
    if "SOURCE_SYSTEM" not in df.columns:
        return df
    out = df.copy()
    for old_val, new_val in SOURCE_SYSTEM_REPLACEMENTS.items():
        out["SOURCE_SYSTEM"] = out["SOURCE_SYSTEM"].replace(old_val, new_val)
    return out


def _replace_procore_in_values(df: pd.DataFrame) -> pd.DataFrame:
    """Replace any remaining 'procore' (case-insensitive) with 'platform' in all string columns."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object or str(out[col].dtype) == "string":
            out[col] = out[col].astype(str).str.replace("procore", "platform", case=False, regex=False)
    return out


def _pseudonymize_columns(
    df: pd.DataFrame,
    pii_columns: list[str],
    salt: str,
    cache: dict,
) -> pd.DataFrame:
    out = df.copy()
    for col in pii_columns:
        if col not in out.columns:
            continue
        prefix = col.lower()[:8] if len(col) >= 8 else col.lower()
        out[col] = out[col].apply(
            lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, prefix, cache)
        )
    return out


def _pseudonymize_id_columns(df: pd.DataFrame, salt: str, cache: dict) -> pd.DataFrame:
    """Deterministic pseudonymization for ID columns so referential integrity is preserved."""
    out = df.copy()
    for col in ID_COLUMNS:
        if col not in out.columns:
            continue
        out[col] = out[col].apply(
            lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "id", cache)
        )
    return out


def mask_companies(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_COMPANIES, salt, cache)
    df = _pseudonymize_id_columns(df, salt, cache)
    df = _replace_procore_in_values(df)
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_projects(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_PROJECTS, salt, cache)
    df = _pseudonymize_id_columns(df, salt, cache)
    df = _replace_procore_in_values(df)
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_contacts(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_CONTACTS, salt, cache)
    df = _pseudonymize_id_columns(df, salt, cache)
    df = _replace_procore_in_values(df)
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_golden_companies(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_COMPANIES, salt, cache)
    # Golden has golden_company_id, cluster_id - pseudonymize ID-like columns present
    for c in ["golden_company_id", "cluster_id"] + [x for x in ID_COLUMNS if x in df.columns]:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "id", cache)
            )
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_golden_projects(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_PROJECTS, salt, cache)
    for c in ["golden_project_id", "company_cluster_id"] + [x for x in ID_COLUMNS if x in df.columns]:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "id", cache)
            )
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_golden_contacts(path: Path, salt: str, cache: dict) -> None:
    df = _read_csv(path)
    df = _apply_column_renames(df)
    df = _apply_source_system_replacements(df)
    df = _pseudonymize_columns(df, PII_CONTACTS, salt, cache)
    for c in ["golden_contact_id", "company_cluster_id"] + [x for x in ID_COLUMNS if x in df.columns]:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "id", cache)
            )
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def mask_link_table(path: Path, salt: str, cache: dict, has_project_name: bool) -> None:
    df = _read_csv(path)
    # Rename PROCORE_ID if present
    df = _apply_column_renames(df)
    # Pseudonymize golden IDs so they match golden_companies/projects/contacts
    for col in ["golden_company_id", "golden_project_id", "golden_contact_id", "SOURCE_ID"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "id", cache)
            )
    if has_project_name and "PROJECT_NAME" in df.columns:
        df["PROJECT_NAME"] = df["PROJECT_NAME"].apply(
            lambda v: _pseudonymize_value(str(v) if pd.notna(v) else "", salt, "project", cache)
        )
    df.to_csv(path, index=False)
    print(f"Masked {path.name}")


def _sanitize_urls_and_procore(path: Path, salt: str, cache: dict) -> None:
    """One-off: replace procore->platform and pseudonymize any cell containing @ (email/URL PII)."""
    df = _read_csv(path)
    df = _replace_procore_in_values(df)
    for col in df.columns:
        if col != "WEBSITE" and "WEBSITE" not in col and "EMAIL" not in col and "EMAIL_ADDRESS" not in col:
            continue
        # Pseudonymize values that look like email or URL with @
        def _mask_if_at(v):
            s = str(v).strip() if pd.notna(v) else ""
            if s and "@" in s:
                return _pseudonymize_value(s, salt, "url", cache)
            return s
        df[col] = df[col].apply(_mask_if_at)
    df.to_csv(path, index=False)
    print(f"Sanitized {path.name}")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Mask raw_data for GitHub (PII + platform renames).")
    parser.add_argument("--replace-procore-only", action="store_true", help="Only replace 'procore' with 'platform' in all CSVs (raw + golden).")
    parser.add_argument("--sanitize-urls", action="store_true", help="Pseudonymize WEBSITE/email-like values containing @ (use with --replace-procore-only on already-masked data).")
    args = parser.parse_args()

    salt = _get_salt()
    cache: dict = {}

    if args.replace_procore_only or args.sanitize_urls:
        # Operate on existing raw + golden without re-pseudonymizing IDs
        for path in [COMPANIES_CSV, PROJECTS_CSV, CONTACTS_CSV]:
            if path.exists():
                df = _read_csv(path)
                df = _replace_procore_in_values(df)
                if args.sanitize_urls:
                    for col in df.columns:
                        if "WEBSITE" in col or "EMAIL" in col:
                            df[col] = df[col].apply(
                                lambda v: _pseudonymize_value(str(v).strip(), salt, "url", cache) if (v and "@" in str(v)) else (str(v).strip() if pd.notna(v) else "")
                            )
                df.to_csv(path, index=False)
                print(f"Sanitized {path.name}")
        out_dir = Path(OUTPUT_DIR)
        for name in ["golden_companies.csv", "golden_projects.csv", "golden_contacts.csv", "company_project_links.csv", "company_contact_links.csv"]:
            p = out_dir / name
            if p.exists():
                df = _read_csv(p)
                df = _replace_procore_in_values(df)
                if args.sanitize_urls and name in ("golden_companies.csv", "golden_contacts.csv"):
                    for col in df.columns:
                        if "WEBSITE" in col or "EMAIL" in col:
                            df[col] = df[col].apply(
                                lambda v: _pseudonymize_value(str(v).strip(), salt, "url", cache) if (v and "@" in str(v)) else (str(v).strip() if pd.notna(v) else "")
                            )
                df.to_csv(p, index=False)
                print(f"Sanitized {p.name}")
        print("Sanitize complete.")
        return

    # Mask raw data only. Then run golden_records pipeline to regenerate golden from masked raw.
    if COMPANIES_CSV.exists():
        mask_companies(COMPANIES_CSV, salt, cache)
    if PROJECTS_CSV.exists():
        mask_projects(PROJECTS_CSV, salt, cache)
    if CONTACTS_CSV.exists():
        mask_contacts(CONTACTS_CSV, salt, cache)

    print("Raw data masking complete. Run golden_records pipeline to regenerate golden from masked raw.")


if __name__ == "__main__":
    main()
