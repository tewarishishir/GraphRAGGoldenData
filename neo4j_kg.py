"""
Load golden companies, projects, and contacts into Neo4j as a Knowledge Graph.

Entities: Company, Project, Contact
Relationships:
  Company -[:EMPLOYS]-> Contact
  Company -[:PARTICIPATES_IN]-> Project
  Contact -[:ASSIGNED_TO]-> Project (when project assignment data exists)

Use with Neo4j Desktop free version: create a DB, set NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD.
"""
import pandas as pd
from pathlib import Path
from typing import Optional

from config import (
    OUTPUT_DIR,
    GOLDEN_COMPANIES_CSV,
    GOLDEN_PROJECTS_CSV,
    GOLDEN_CONTACTS_CSV,
    COMPANY_PROJECT_LINKS_CSV,
    COMPANY_CONTACT_LINKS_CSV,
    NEO4J_URI,
    NEO4J_USER,
    NEO4J_PASSWORD,
    NEO4J_DATABASE,
)


def _session_database():
    """Use NEO4J_DATABASE from env (e.g. Aura instance id 99517c3a)."""
    return NEO4J_DATABASE


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False, dtype=str, keep_default_na=False)


def _safe_str(val) -> str:
    if pd.isna(val) or val is None:
        return ""
    s = str(val).strip()
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", " ") if s else ""


def load_knowledge_graph(
    golden_dir: Optional[Path] = None,
    uri: str = NEO4J_URI,
    user: str = NEO4J_USER,
    password: str = NEO4J_PASSWORD,
    clear_first: bool = True,
) -> None:
    """
    Create Neo4j graph: Company, Project, Contact nodes and EMPLOYS, PARTICIPATES_IN, ASSIGNED_TO relationships.
    """
    try:
        from neo4j import GraphDatabase
    except ImportError:
        raise ImportError("Install neo4j: pip install neo4j")

    if not password:
        raise ValueError("Set NEO4J_PASSWORD (or pass password=) to connect to Neo4j.")

    golden_dir = Path(golden_dir or OUTPUT_DIR)
    driver = GraphDatabase.driver(uri, auth=(user, password))

    def run_query(tx, cypher: str, **params):
        tx.run(cypher, **params)

    with driver.session(database=_session_database()) as session:
        if clear_first:
            session.execute_write(lambda tx: run_query(tx, "MATCH (n) DETACH DELETE n"))
            print("Cleared existing graph.")

        # Optional: create uniqueness constraints (Neo4j 4.4+)
        for label, prop in [("Company", "golden_company_id"), ("Project", "golden_project_id"), ("Contact", "golden_contact_id")]:
            try:
                session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE")
            except Exception:
                pass

        # Load golden companies
        companies_path = golden_dir / "golden_companies.csv"
        if not companies_path.exists():
            raise FileNotFoundError(f"Run golden_records pipeline first. Missing {companies_path}")
        companies = _read_csv(companies_path)
        for _, row in companies.iterrows():
            cid = _safe_str(row.get("golden_company_id", ""))
            if not cid:
                continue
            name = _safe_str(row.get("COMPANY_NAME", ""))
            website = _safe_str(row.get("COMPANY_WEBSITE", ""))
            city = _safe_str(row.get("CITY_NAME", ""))
            country = _safe_str(row.get("COUNTRY_NAME", ""))
            acv = _safe_str(row.get("ACV_USD", ""))
            estimated_acv = _safe_str(row.get("ESTIMATED_ACV", ""))
            session.run(
                """
                MERGE (c:Company {golden_company_id: $id})
                SET c.name = $name, c.website = $website, c.city = $city, c.country = $country,
                    c.acv_usd = $acv, c.estimated_acv = $estimated_acv
                """,
                id=cid, name=name, website=website, city=city, country=country, acv=acv, estimated_acv=estimated_acv,
            )
        print(f"Loaded {len(companies)} companies.")

        # Load golden projects
        projects_path = golden_dir / "golden_projects.csv"
        projects = _read_csv(projects_path)
        for _, row in projects.iterrows():
            pid = _safe_str(row.get("golden_project_id", ""))
            if not pid:
                continue
            name = _safe_str(row.get("PROJECT_NAME", ""))
            stage = _safe_str(row.get("PROJECT_STAGE_STANDARD", ""))
            value = _safe_str(row.get("ESTIMATED_VALUE_USD", ""))
            is_active = _safe_str(row.get("IS_ACTIVE", ""))
            session.run(
                """
                MERGE (p:Project {golden_project_id: $id})
                SET p.name = $name, p.stage = $stage, p.estimated_value_usd = $value, p.is_active = $is_active
                """,
                id=pid, name=name, stage=stage, value=value, is_active=is_active,
            )
        print(f"Loaded {len(projects)} projects.")

        # Load golden contacts
        contacts_path = golden_dir / "golden_contacts.csv"
        contacts = _read_csv(contacts_path)
        for _, row in contacts.iterrows():
            cid = _safe_str(row.get("golden_contact_id", ""))
            if not cid:
                continue
            name = _safe_str(row.get("FULL_NAME", ""))
            email = _safe_str(row.get("EMAIL_ADDRESS", ""))
            title = _safe_str(row.get("JOB_TITLE", ""))
            session.run(
                """
                MERGE (c:Contact {golden_contact_id: $id})
                SET c.full_name = $name, c.email = $email, c.job_title = $title
                """,
                id=cid, name=name, email=email, title=title,
            )
        print(f"Loaded {len(contacts)} contacts.")

        # Company -[:PARTICIPATES_IN]-> Project
        links_proj = golden_dir / "company_project_links.csv"
        if links_proj.exists():
            df = _read_csv(links_proj)
            for _, row in df.iterrows():
                comp_id = _safe_str(row.get("golden_company_id", ""))
                proj_id = _safe_str(row.get("golden_project_id", ""))
                if comp_id and proj_id:
                    session.run(
                        """
                        MATCH (c:Company {golden_company_id: $cid})
                        MATCH (p:Project {golden_project_id: $pid})
                        MERGE (c)-[:PARTICIPATES_IN]->(p)
                        """,
                        cid=comp_id, pid=proj_id,
                    )
            print(f"Created PARTICIPATES_IN from {len(df)} links.")

        # Company -[:EMPLOYS]-> Contact
        links_cont = golden_dir / "company_contact_links.csv"
        if links_cont.exists():
            df = _read_csv(links_cont)
            for _, row in df.iterrows():
                comp_id = _safe_str(row.get("golden_company_id", ""))
                cont_id = _safe_str(row.get("golden_contact_id", ""))
                if comp_id and cont_id:
                    session.run(
                        """
                        MATCH (c:Company {golden_company_id: $cid})
                        MATCH (x:Contact {golden_contact_id: $cont_id})
                        MERGE (c)-[:EMPLOYS]->(x)
                        """,
                        cid=comp_id, cont_id=cont_id,
                    )
            print(f"Created EMPLOYS from {len(df)} links.")

    driver.close()
    print("Knowledge graph loaded into Neo4j.")


if __name__ == "__main__":
    import os
    try:
        from dotenv import load_dotenv
        from pathlib import Path
        load_dotenv(Path(__file__).resolve().parent / ".env", override=True)
    except ImportError:
        pass
    load_knowledge_graph(password=os.getenv("NEO4J_PASSWORD", NEO4J_PASSWORD), clear_first=True)
