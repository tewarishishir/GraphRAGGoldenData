"""Paths and Neo4j connection settings."""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", override=True)
except ImportError:
    pass

# Data paths (relative to project root)
DATA_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = DATA_DIR / "raw_data"
COMPANIES_CSV = RAW_DATA_DIR / "Companies.csv"
PROJECTS_CSV = RAW_DATA_DIR / "Projects.csv"
CONTACTS_CSV = RAW_DATA_DIR / "Contacts.csv"

# Golden output paths
OUTPUT_DIR = DATA_DIR / "golden"
GOLDEN_COMPANIES_CSV = OUTPUT_DIR / "golden_companies.csv"
GOLDEN_PROJECTS_CSV = OUTPUT_DIR / "golden_projects.csv"
GOLDEN_CONTACTS_CSV = OUTPUT_DIR / "golden_contacts.csv"
COMPANY_PROJECT_LINKS_CSV = OUTPUT_DIR / "company_project_links.csv"
COMPANY_CONTACT_LINKS_CSV = OUTPUT_DIR / "company_contact_links.csv"

# Neo4j: driver uses GraphDatabase.driver(URI, auth=(USER, PASSWORD))
# Aura uses NEO4J_USERNAME; we accept NEO4J_USERNAME or NEO4J_USER
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")  # Aura: set to instance id in .env

# Ollama (local LLM for text-to-Cypher and summary)
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
