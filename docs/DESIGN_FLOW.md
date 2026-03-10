# Golden Data — Design Flow

This document describes the end-to-end design and data flow so developers can understand how the system works.

---

## 1. High-level architecture

```mermaid
flowchart TB
    subgraph inputs["Raw data"]
        A[Companies.csv]
        B[Projects.csv]
        C[Contacts.csv]
    end

    subgraph pipeline["Golden pipeline"]
        G[golden_records.py]
    end

    subgraph golden["Golden outputs"]
        D[golden_companies.csv]
        E[golden_projects.csv]
        F[golden_contacts.csv]
        L1[company_project_links.csv]
        L2[company_contact_links.csv]
    end

    subgraph load["Load into graph"]
        N[neo4j_kg.py]
    end

    subgraph graph["Neo4j Knowledge Graph"]
        KG[(Company, Project, Contact\nEMPLOYS, PARTICIPATES_IN)]
    end

    subgraph app["Web app"]
        FLASK[app.py + index.html]
    end

    subgraph llm["Optional LLM"]
        OLLAMA[Ollama]
        RAG[graph_rag.py]
    end

    A --> G
    B --> G
    C --> G
    G --> D
    G --> E
    G --> F
    G --> L1
    G --> L2
    D --> N
    E --> N
    F --> N
    L1 --> N
    L2 --> N
    N --> KG
    KG <--> FLASK
    FLASK <--> RAG
    RAG <--> OLLAMA
```

**Summary:** Raw CSVs → **golden_records.py** → Golden CSVs + link tables → **neo4j_kg.py** → Neo4j. The **Flask app** queries Neo4j (direct Cypher or via **graph_rag.py** + Ollama for natural language).

---

## 2. Data pipeline flow (offline)

```mermaid
flowchart LR
    subgraph raw["raw_data/"]
        R1[Companies.csv]
        R2[Projects.csv]
        R3[Contacts.csv]
    end

    subgraph golden_records["golden_records.py"]
        direction TB
        M1[build_golden_companies]
        M2[build_company_id_to_cluster_mapping]
        M3[build_golden_projects]
        M4[build_golden_contacts]
    end

    subgraph golden_dir["golden/"]
        O1[golden_companies.csv]
        O2[golden_projects.csv]
        O3[golden_contacts.csv]
        O4[company_project_links.csv]
        O5[company_contact_links.csv]
    end

    R1 --> M1
    R1 --> M2
    M1 --> O1
    M2 --> M3
    M2 --> M4
    R2 --> M3
    R3 --> M4
    M3 --> O2
    M3 --> O4
    M4 --> O3
    M4 --> O5
```

**Steps:**

1. **Companies** → One golden company per cluster (`DEDUP_PARENT_ID`). Canonical row preferred; merge non-null from cluster. All raw columns kept; `ESTIMATED_ACV` etc. merged.
2. **Mapping** → `company_id_src` / `company_source_id` → `company_cluster_id`.
3. **Projects** → One row per project; `company_cluster_id` from mapping. All raw columns kept.
4. **Contacts** → One row per contact; `company_cluster_id` from `COMPANY_SOURCE_ID`. All raw columns kept.
5. **Links** → `company_project_links.csv` and `company_contact_links.csv` for Neo4j relationships.

**Commands:**  
`python golden_records.py` → writes `golden/`.  
`python neo4j_kg.py` → reads `golden/`, loads into Neo4j.

---

## 3. Runtime flow: how a query is answered

Two ways the user can query: **Ask (natural language)** and **Run Cypher**.

### 3a. Ask (natural language)

```mermaid
sequenceDiagram
    participant U as User
    participant UI as index.html
    participant API as app.py /api/ask
    participant RAG as graph_rag.py
    participant OLLAMA as Ollama
    participant NEO as Neo4j

    U->>UI: Type question, click Ask
    UI->>API: POST /api/ask { question }
    API->>RAG: get_cypher_from_question(question)
    RAG->>OLLAMA: Chat (schema + question → Cypher)
    OLLAMA-->>RAG: Cypher string
    RAG-->>API: cypher
    API->>NEO: Run Cypher
    NEO-->>API: columns, rows, (graph)
    API->>RAG: get_summary(question, columns, rows)
    RAG->>OLLAMA: Chat (summarize result)
    OLLAMA-->>RAG: summary text
    RAG-->>API: summary
    API-->>UI: { cypher, columns, rows, summary, graph? }
    UI-->>U: Table + summary + optional graph
```

- **graph_rag.py** turns the question into read-only Cypher using the graph schema and an example, then (optionally) summarizes the result via Ollama.
- **app.py** runs the Cypher on Neo4j and returns table + summary + optional graph for the UI.

### 3b. Run Cypher (direct)

```mermaid
sequenceDiagram
    participant U as User
    participant UI as index.html
    participant API as app.py /api/query
    participant NEO as Neo4j

    U->>UI: Enter Cypher, click Run (or pick example)
    UI->>API: POST /api/query { cypher }
    API->>NEO: Run Cypher
    NEO-->>API: columns, rows, (graph)
    API-->>UI: { columns, rows, graph? }
    UI-->>U: Table + optional graph
```

- No LLM. **app.py** executes the Cypher and returns the result.

---

## 4. Component map

| Component | Role |
|-----------|------|
| **config.py** | Paths (raw_data, golden), Neo4j URI/user/password, Ollama host/model. |
| **golden_records.py** | Build golden companies/projects/contacts and link tables from raw CSVs. |
| **neo4j_kg.py** | Load golden CSVs into Neo4j: Company, Project, Contact nodes; PARTICIPATES_IN, EMPLOYS edges. |
| **graph_rag.py** | Text→Cypher (Ollama + schema + example); result→summary (Ollama). Validates read-only Cypher. |
| **app.py** | Flask: serves index.html, /api/status, /api/examples, /api/query, /api/ask, /api/ollama-status, /api/graph. |
| **index.html** | UI: Ask input, Cypher editor, examples dropdown, results table, graph tab, status. |
| **static/images/** | Platform logo placeholder, construction hero image. |

---

## 5. Graph model (Neo4j)

```mermaid
erDiagram
    Company ||--o{ Project : "PARTICIPATES_IN"
    Company ||--o{ Contact : "EMPLOYS"

    Company {
        string golden_company_id PK
        string name
        string city
        string country
        string website
        string acv_usd
        string estimated_acv
    }

    Project {
        string golden_project_id PK
        string name
        string stage
        string estimated_value_usd
        string is_active
    }

    Contact {
        string golden_contact_id PK
        string full_name
        string email
        string job_title
    }
```

- **Company** and **Project** are linked by **PARTICIPATES_IN** (from `company_project_links.csv`).
- **Company** and **Contact** are linked by **EMPLOYS** (from `company_contact_links.csv`).

---

## 6. Quick reference: run order

1. **One-time / when raw data or golden logic changes**  
   `python golden_records.py`  
   → Produces `golden/*.csv`.

2. **Load or reload the graph**  
   `python neo4j_kg.py`  
   → Clears and reloads Neo4j from `golden/`.

3. **Run the app**  
   `python app.py` or `flask --app app run`  
   → Open the UI, use Ask or Cypher.

4. **Optional (for Ask)**  
   Install and run Ollama; pull a model (e.g. `ollama pull llama3.2`).  
   If Ollama is not running, the app still works with direct Cypher and example queries.
