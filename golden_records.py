"""
Create golden data records for companies, projects, and contacts per cluster.

- One golden company per company cluster (DEDUP_PARENT_ID).
- Golden projects and contacts are linked to company cluster.
- Rule-based consolidation (optionally extend with LLM for name/address cleanup).
"""
import pandas as pd
from pathlib import Path
from config import (
    COMPANIES_CSV,
    PROJECTS_CSV,
    CONTACTS_CSV,
    OUTPUT_DIR,
    GOLDEN_COMPANIES_CSV,
    GOLDEN_PROJECTS_CSV,
    GOLDEN_CONTACTS_CSV,
    COMPANY_PROJECT_LINKS_CSV,
    COMPANY_CONTACT_LINKS_CSV,
)


def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV with flexible parsing for messy fields."""
    return pd.read_csv(path, low_memory=False, dtype=str, keep_default_na=False)


def _best_non_null(series: pd.Series) -> str:
    """First non-empty string in a series."""
    for v in series.dropna():
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
    return ""


def build_golden_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """
    One golden company per cluster (DEDUP_PARENT_ID).
    Prefer the canonical record (SOURCE_ID == DEDUP_PARENT_ID); else merge non-null from cluster.
    """
    cluster_col = "DEDUP_PARENT_ID"
    if cluster_col not in companies.columns:
        raise ValueError(f"Companies must have {cluster_col}")

    # Use cluster id as golden company id
    companies = companies.copy()
    companies["company_cluster_id"] = companies[cluster_col]

    # Prefer canonical row (self-parent)
    canonical = companies[companies["SOURCE_ID"] == companies[cluster_col]]
    non_canonical = companies[companies["SOURCE_ID"] != companies[cluster_col]]

    golden_list = []
    merge_cols = [
        "COMPANY_NAME", "COMPANY_WEBSITE", "STREET_NAME", "CITY_NAME", "STATE_NAME",
        "COUNTRY_NAME", "COUNTRY_CODE", "POSTAL_CODE", "COMPANY_PHONE_NUMBER",
        "COMPANY_TYPE", "VERTICAL", "ACV_RANGE", "ACV_USD", "SOURCE_SYSTEM",
        "ESTIMATED_ACV", "PREDICTED_ACV", "ANNUAL_REVENUE",
    ]
    merge_cols = [c for c in merge_cols if c in companies.columns]

    for cluster_id, group in companies.groupby("company_cluster_id"):
        base = canonical[canonical["company_cluster_id"] == cluster_id]
        if not base.empty:
            row = base.iloc[0].to_dict()
        else:
            row = group.iloc[0].to_dict()

        # Merge: fill missing from other rows in cluster
        for col in merge_cols:
            if col not in row or not str(row.get(col, "")).strip():
                vals = group[col].replace("", pd.NA).dropna()
                if len(vals):
                    row[col] = str(vals.iloc[0]).strip()
        row["golden_company_id"] = cluster_id
        row["cluster_id"] = cluster_id
        golden_list.append(row)

    golden = pd.DataFrame(golden_list)
    # Keep all columns from raw (golden has them from base row + merged merge_cols)
    return golden


def build_company_id_to_cluster_mapping(companies: pd.DataFrame) -> pd.DataFrame:
    """Build mapping from COMPANY_ID_SRC / COMPANY_ID_MONOLITH / SOURCE_ID to company_cluster_id."""
    companies = companies.copy()
    companies["company_cluster_id"] = companies["DEDUP_PARENT_ID"]

    mappings = []
    for _, row in companies.iterrows():
        cid = row["company_cluster_id"]
        if row.get("COMPANY_ID_SRC") and str(row["COMPANY_ID_SRC"]).strip():
            mappings.append({"company_id_src": str(row["COMPANY_ID_SRC"]).strip(), "company_cluster_id": cid})
        if row.get("COMPANY_ID_MONOLITH") and str(row["COMPANY_ID_MONOLITH"]).strip():
            mappings.append({"company_id_monolith": str(row["COMPANY_ID_MONOLITH"]).strip(), "company_cluster_id": cid})
        mappings.append({"company_source_id": row["SOURCE_ID"], "company_cluster_id": cid})

    return pd.DataFrame(mappings).drop_duplicates()


def build_golden_projects(
    projects: pd.DataFrame,
    company_mapping: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Golden projects: one record per project (or per DEDUP_CLUSTER_ID if we want project clusters).
    Link each project to company_cluster_id via COMPANY_ID_SRC / COMPANY_ID_MONOLITH.
    """
    projects = projects.copy()

    # Map project's company id to company cluster (one cluster per id)
    if "company_id_src" in company_mapping.columns:
        by_src = company_mapping.dropna(subset=["company_id_src"]).drop_duplicates(subset=["company_id_src"]).set_index("company_id_src")["company_cluster_id"]
    else:
        by_src = pd.Series(dtype=object)
    if "company_id_monolith" in company_mapping.columns:
        by_monolith = company_mapping.dropna(subset=["company_id_monolith"]).drop_duplicates(subset=["company_id_monolith"]).set_index("company_id_monolith")["company_cluster_id"]
    else:
        by_monolith = pd.Series(dtype=object)

    # Vectorized lookup: try COMPANY_ID_SRC then COMPANY_ID_MONOLITH
    src_col = projects["COMPANY_ID_SRC"].astype(str).str.strip() if "COMPANY_ID_SRC" in projects.columns else pd.Series("", index=projects.index)
    mon_col = projects["COMPANY_ID_MONOLITH"].astype(str).str.strip() if "COMPANY_ID_MONOLITH" in projects.columns else pd.Series("", index=projects.index)
    clusters = pd.Series(index=projects.index, dtype=object)
    if len(by_src):
        clusters = src_col.map(by_src)
    if len(by_monolith):
        from_mon = mon_col.map(by_monolith)
        clusters = clusters.fillna(from_mon)
    projects["company_cluster_id"] = clusters

    # Golden projects: keep one row per project (SOURCE_ID or DEDUP_CLUSTER_ID)
    proj_id_col = "SOURCE_ID" if "SOURCE_ID" in projects.columns else projects.columns[0]
    project_cluster_col = "DEDUP_CLUSTER_ID" if "DEDUP_CLUSTER_ID" in projects.columns else None

    golden_projects = projects.copy()
    golden_projects["golden_project_id"] = (golden_projects[project_cluster_col] if project_cluster_col else golden_projects[proj_id_col]).fillna(golden_projects[proj_id_col])

    # Links: company_cluster_id <-> project
    link_cols = ["company_cluster_id", "golden_project_id", proj_id_col]
    if "PROJECT_NAME" in golden_projects.columns:
        link_cols.append("PROJECT_NAME")
    links = golden_projects[["company_cluster_id", "golden_project_id", proj_id_col]].dropna(subset=["company_cluster_id"]).drop_duplicates()
    if "PROJECT_NAME" in golden_projects.columns:
        links = links.merge(golden_projects[[proj_id_col, "PROJECT_NAME"]].drop_duplicates(), on=proj_id_col, how="left")
    links = links.rename(columns={"company_cluster_id": "golden_company_id"})

    # Keep all columns from raw (golden_projects is projects + golden_project_id, company_cluster_id)
    golden_projects_out = golden_projects.drop_duplicates(subset=["golden_project_id"], keep="first")

    return golden_projects_out, links


def build_golden_contacts(
    contacts: pd.DataFrame,
    company_mapping: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Golden contacts: one record per contact, linked to company cluster via COMPANY_SOURCE_ID.
    """
    contacts = contacts.copy()
    source_to_cluster = company_mapping.dropna(subset=["company_source_id"]).set_index("company_source_id")["company_cluster_id"]

    def cluster_for_contact(pid):
        if pd.isna(pid) or not str(pid).strip():
            return None
        return source_to_cluster.get(str(pid).strip())

    contacts["company_cluster_id"] = contacts["COMPANY_SOURCE_ID"].map(lambda x: cluster_for_contact(x))

    contact_id_col = "SOURCE_ID" if "SOURCE_ID" in contacts.columns else contacts.columns[0]
    contacts["golden_contact_id"] = contacts[contact_id_col]

    links = contacts[["company_cluster_id", "golden_contact_id", contact_id_col]].dropna(subset=["company_cluster_id"]).drop_duplicates()
    links = links.rename(columns={"company_cluster_id": "golden_company_id"})

    # Keep all columns from raw (contacts has golden_contact_id, company_cluster_id added)
    golden_contacts_out = contacts.drop_duplicates(subset=["golden_contact_id"], keep="first")

    return golden_contacts_out, links


def run_pipeline(
    companies_path: Path = COMPANIES_CSV,
    projects_path: Path = PROJECTS_CSV,
    contacts_path: Path = CONTACTS_CSV,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """Run full golden records pipeline and write outputs."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    companies = _read_csv(companies_path)
    projects = _read_csv(projects_path)
    contacts = _read_csv(contacts_path)

    golden_companies = build_golden_companies(companies)
    company_mapping = build_company_id_to_cluster_mapping(companies)

    golden_projects, company_project_links = build_golden_projects(projects, company_mapping)
    golden_contacts, company_contact_links = build_golden_contacts(contacts, company_mapping)

    golden_companies.to_csv(output_dir / "golden_companies.csv", index=False)
    golden_projects.to_csv(output_dir / "golden_projects.csv", index=False)
    golden_contacts.to_csv(output_dir / "golden_contacts.csv", index=False)
    company_project_links.to_csv(output_dir / "company_project_links.csv", index=False)
    company_contact_links.to_csv(output_dir / "company_contact_links.csv", index=False)

    print(f"Golden companies: {len(golden_companies)}")
    print(f"Golden projects: {len(golden_projects)} (linked: {company_project_links['golden_company_id'].notna().sum()})")
    print(f"Golden contacts: {len(golden_contacts)} (linked: {company_contact_links['golden_company_id'].notna().sum()})")
    print(f"Outputs written to {output_dir}")


if __name__ == "__main__":
    run_pipeline()
