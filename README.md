# Golden Data & Knowledge Graph (Neo4j)

This project builds **golden records** for companies, projects, and contacts from clustered source data, then loads them into a **Neo4j Knowledge Graph** so users can run natural-language-style queries (e.g., "companies and users who have two projects").

## Overview (from diagram)

1. **Clustered entities** (Companies, Projects, Contacts) → **Rule-based + optional LLM** → **Golden entities**
2. **Golden entities** → **Knowledge Graph (Neo4j)** with nodes: Company, Project, Contact and relationships: EMPLOYS, PARTICIPATES_IN, ASSIGNED_TO
3. **Users** query the graph (Cypher / QA) to get answers

**→ [Design flow (for developers)](docs/DESIGN_FLOW.md)** — End-to-end architecture, data pipeline, runtime query flow (Ask vs Cypher), component map, and Neo4j graph model with Mermaid diagrams.

**→ [Conference presentation](presentation/)** — Slide deck (reveal.js) for talks and demos. Open `presentation/index.html` in a browser or run `python -m http.server 8080` inside `presentation/`.

## Data

Raw input files live in **`raw_data/`**:

- **raw_data/Companies.csv**: Company records with `DEDUP_PARENT_ID` as cluster id (companies with the same parent are one cluster).
- **raw_data/Projects.csv**: Projects with `COMPANY_ID_SRC` linking to companies; `DEDUP_CLUSTER_ID` for project dedup.
- **raw_data/Contacts.csv**: Contacts with `COMPANY_SOURCE_ID` linking to companies.

## Setup

```bash
cd /path/to/GoldenData
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

For Neo4j (free version):

**Option A — Local (Neo4j Desktop or Community)**

1. Install [Neo4j Desktop](https://neo4j.com/download/) or Neo4j Community.
2. Create a database and **start** it (green play button).
3. Set connection (default: `bolt://127.0.0.1:7687`, user `neo4j`, and your password):

```bash
export NEO4J_URI=bolt://127.0.0.1:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=your_password
```

**Option B — Cloud (Neo4j Aura Free)**

Create a free instance at [neo4j.com/cloud/aura](https://neo4j.com/cloud/aura). **Wait ~60 seconds** after creation (or check [console.neo4j.io](https://console.neo4j.io)) before connecting. Then set (Aura gives these in the instance details):

```bash
export NEO4J_URI=neo4j+s://xxxxx.databases.neo4j.io
export NEO4J_USERNAME=xxxxx
export NEO4J_PASSWORD=your_aura_password
```

Or use a `.env` file (see `.env.example` for variable names). The app also accepts `NEO4J_USER` if you prefer. Do not commit `.env`; it is in `.gitignore`.

**Connection refused (errno 61)?** Neo4j is not running. Start your local DB in Neo4j Desktop, or use Option B and set `NEO4J_URI` to your Aura instance. The default URI uses `127.0.0.1` to avoid IPv6 connection issues.

**Aura AuthError / "authentication failed" (Unauthorized)?**  
1. **Reset the password** — In [console.neo4j.io](https://console.neo4j.io) open your instance → find **"Reset DBMS password"** (or Connection details) → set a **new password** and copy it → in `.env` set `NEO4J_PASSWORD=<new password>`.  
2. **Username** — Run `python test_neo4j_connection.py`; it tries both `neo4j` and your instance id. If it connects with one, it will tell you — set that in `.env` as `NEO4J_USERNAME=...`.  
3. Confirm the instance is **Running** and you waited ~60s after creation.

## Masking data for publication (e.g. GitHub)

To publish the repo with anonymized data, run the masking script on raw data, then rebuild golden:

```bash
python scripts/mask_data_for_github.py   # masks raw_data (PII + platform renames)
python golden_records.py                 # regenerate golden from masked raw
```

The script renames Procore-related columns to generic names (`SOURCE_ID`, `DEDUP_PARENT_ID`, `COMPANY_SOURCE_ID`), replaces `SOURCE_SYSTEM` values, and deterministically pseudonymizes PII. Use the same `MASK_SALT` (env) when regenerating if you need reproducibility.

## Pipeline

### 1. Build golden records (no Neo4j required)

```bash
python golden_records.py
```

This will:

- **Golden companies**: One record per company cluster (`DEDUP_PARENT_ID`). Prefers the canonical row (where `SOURCE_ID == DEDUP_PARENT_ID`) and merges non-null fields from the cluster.
- **Golden projects**: One record per project, with `company_cluster_id` set by mapping `COMPANY_ID_SRC` / `COMPANY_ID_MONOLITH` to the company cluster.
- **Golden contacts**: One record per contact, with `company_cluster_id` from `COMPANY_SOURCE_ID` → company cluster.
- Write **golden/**:
  - `golden_companies.csv`
  - `golden_projects.csv`
  - `golden_contacts.csv`
  - `company_project_links.csv` (golden_company_id ↔ golden_project_id)
  - `company_contact_links.csv` (golden_company_id ↔ golden_contact_id)

### 2. Load Knowledge Graph into Neo4j

```bash
python neo4j_kg.py
```

Creates:

- **Nodes**: `Company`, `Project`, `Contact` (with ids and key attributes).
- **Relationships**:
  - `Company -[:PARTICIPATES_IN]-> Project`
  - `Company -[:EMPLOYS]-> Contact`

(Contact -[:ASSIGNED_TO]-> Project can be added when assignment data is available.)

### 3. Run full pipeline (golden + Neo4j)

```bash
python run_pipeline.py
```

Use `--golden-only` to skip Neo4j load; use `--no-clear` to append to an existing graph instead of clearing it.

## Example queries (Neo4j Browser)

After loading, open Neo4j Browser and run queries from **example_queries.cypher**, e.g.:

- **Companies and contacts who have at least two projects**

```cypher
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count >= 2
MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, project_count, collect(contact.full_name) AS contacts
ORDER BY project_count DESC;
```

- **Five companies with active projects and ACV > 5 million**

```cypher
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WHERE p.is_active = 'true' AND c.acv_usd <> '' AND toFloat(c.acv_usd) > 5
WITH c, collect(p.name) AS projects
RETURN c.name, c.acv_usd, projects
LIMIT 5;
```

## Web UI — talk to your graph

A small web app lets you run Cypher queries and view results (table + optional graph).

1. Set **NEO4J_PASSWORD** (and optionally NEO4J_URI, NEO4J_USERNAME).
2. Start the app:

```bash
python app.py
# or: flask --app app run
```

3. Open **http://localhost:5000** in a browser.
4. Pick an example question or type Cypher in the box and click **Run**. Results show as a table; if the query returns graph data (nodes/relationships), a **Graph** tab appears.

The UI shows connection status (Neo4j connected / not connected) at the top.

### Ask in natural language (Ollama)

You can ask questions in plain English instead of writing Cypher. The app uses **Ollama** (local LLM) to turn your question into Cypher, run it on Neo4j, and show a short summary plus the result table.

1. **Install and run Ollama** (e.g. [ollama.com](https://ollama.com)):
   ```bash
   ollama serve
   ollama pull llama3.2
   ```
2. Optional env (see **config.py**): `OLLAMA_MODEL` (default `llama3.2`), `OLLAMA_HOST` (default `http://localhost:11434`).
3. In the UI, use the **“Ask in natural language”** panel: type a question (e.g. “Which companies have 2+ projects?”) and click **Ask**. You’ll see the generated Cypher, a 2–3 sentence summary, and the result table. The Cypher is also prefilled in the Cypher box so you can edit and **Run** it yourself if you like.

If Ollama isn’t running or the model isn’t pulled, the app will return a clear error (e.g. “Ollama unavailable. Start Ollama and pull the model.”).

## Config

Edit **config.py** to change:

- Input CSV paths (`raw_data/Companies.csv`, `raw_data/Projects.csv`, `raw_data/Contacts.csv`)
- Output directory (`golden/`)
- Neo4j connection (`NEO4J_URI`, `NEO4J_USERNAME` or `NEO4J_USER`, `NEO4J_PASSWORD`; env overrides)
- Ollama for Ask feature (`OLLAMA_MODEL`, `OLLAMA_HOST`; env overrides)

## Optional: LLM enrichment

The diagram mentions "Rule(spark) + LLM". The current code is rule-based only. You can extend `golden_records.py` (e.g. in `build_golden_companies`) to call an LLM for name/address normalization or disambiguation before writing golden records.
