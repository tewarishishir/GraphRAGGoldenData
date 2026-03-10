"""
Flask app to query the Neo4j Knowledge Graph from a web UI.
Run: flask --app app run
"""
import os
import json
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory

# Load .env from project root with override=True so fresh values always win
try:
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parent
    load_dotenv(_root / ".env", override=True)
except ImportError:
    pass

from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE

app = Flask(__name__, static_folder="static", static_url_path="")
BASE = Path(__file__).resolve().parent


def _run_cypher(cypher: str):
    """
    Execute Cypher and return (columns, rows, graph_dict or None).
    Raises on Neo4j error.
    """
    driver = get_driver()
    if not driver:
        raise RuntimeError("Neo4j not configured (set NEO4J_PASSWORD)")
    with driver.session(database=_session_database()) as session:
        result = session.run(cypher)
        records = list(result)
        keys = result.keys() if records else []

    rows = []
    graph_nodes = {}
    graph_edges = []

    for rec in records:
        row = {}
        for key in keys:
            val = rec[key]
            if hasattr(val, "__iter__") and not isinstance(val, (str, dict)):
                try:
                    val = list(val)
                except Exception:
                    pass
            if hasattr(val, "items"):
                val = dict(val)
            if type(val).__name__ == "Node":
                node_id = str(getattr(val, "element_id", None) or getattr(val, "id", id(val)))
                labels = list(val.labels) if hasattr(val, "labels") else []
                label = labels[0] if labels else "Node"
                props = dict(val) if hasattr(val, "__iter__") and not isinstance(val, (str, bytes)) else {}
                if not isinstance(props, dict):
                    props = {}
                graph_nodes[node_id] = {"id": node_id, "label": label, "props": props, "title": props.get("name") or props.get("full_name") or node_id}
                row[key] = props.get("name") or props.get("full_name") or node_id
            elif type(val).__name__ == "Relationship":
                rel_id = str(getattr(val, "element_id", None) or id(val))
                sn = getattr(val, "start_node", None)
                en = getattr(val, "end_node", None)
                start_id = str(getattr(sn, "element_id", None) or getattr(sn, "id", "")) if sn else None
                end_id = str(getattr(en, "element_id", None) or getattr(en, "id", "")) if en else None
                if start_id and end_id:
                    graph_edges.append({"id": rel_id, "from": start_id, "to": end_id, "label": type(val).__name__})
                row[key] = type(val).__name__
            else:
                row[key] = val
        rows.append(row)

    graph = None
    if graph_nodes or graph_edges:
        graph = {"nodes": list(graph_nodes.values()), "edges": graph_edges}
    return keys, rows, graph

# Re-load .env with override=True so process env vars are always up to date
def _reload_env():
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE / ".env", override=True)
    except ImportError:
        pass

def _session_database():
    _reload_env()
    return os.getenv("NEO4J_DATABASE") or "neo4j"

def get_driver():
    _reload_env()
    uri = os.getenv("NEO4J_URI") or NEO4J_URI
    user = os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER") or NEO4J_USER
    password = os.getenv("NEO4J_PASSWORD") or NEO4J_PASSWORD
    if not password:
        return None
    from neo4j import GraphDatabase
    return GraphDatabase.driver(uri, auth=(user, password))


# Predefined example questions (label -> Cypher)
# Schema: Company (name, golden_company_id, acv_usd, estimated_acv), Project (name, is_active), Contact (full_name)
# Relationships: (Company)-[:PARTICIPATES_IN]->(Project), (Company)-[:EMPLOYS]->(Contact)
EXAMPLE_QUERIES = [
    {
        "id": "graph-stats",
        "label": "Graph stats (counts) — always returns one row",
        "cypher": """OPTIONAL MATCH (c:Company) WITH count(c) AS companies
OPTIONAL MATCH (p:Project) WITH companies, count(p) AS projects
OPTIONAL MATCH (x:Contact) WITH companies, projects, count(x) AS contacts
OPTIONAL MATCH ()-[r:PARTICIPATES_IN]->() WITH companies, projects, contacts, count(r) AS participates_in
OPTIONAL MATCH ()-[e:EMPLOYS]->() WITH companies, projects, contacts, participates_in, count(e) AS employs
RETURN companies, projects, contacts, participates_in, employs""",
    },
    {
        "id": "list-companies",
        "label": "List companies (sample)",
        "cypher": """MATCH (c:Company) RETURN c.name AS company_name, c.golden_company_id, c.city, c.country, c.acv_usd, c.estimated_acv LIMIT 20""",
    },
    {
        "id": "list-projects",
        "label": "List projects (sample)",
        "cypher": """MATCH (p:Project) RETURN p.name AS project_name, p.golden_project_id, p.stage, p.is_active LIMIT 20""",
    },
    {
        "id": "companies-contacts-two-projects",
        "label": "Companies and contacts who have 2+ projects",
        "cypher": """MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count >= 2
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, c.golden_company_id, project_count,
       collect(DISTINCT contact.full_name) AS contacts
ORDER BY project_count DESC""",
    },
    {
        "id": "companies-active-acv-5m",
        "label": "Companies with active projects and ACV > 5",
        "cypher": """MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WHERE toLower(trim(toString(coalesce(p.is_active, '')))) = 'true'
  AND c.acv_usd IS NOT NULL AND c.acv_usd <> '' AND toFloat(c.acv_usd) > 5
WITH c, collect(p.name) AS projects
RETURN c.name AS company_name, c.acv_usd, projects
LIMIT 10""",
    },
    {
        "id": "companies-estimated-acv",
        "label": "Companies with estimated_acv > 1M",
        "cypher": """MATCH (c:Company)
WHERE c.estimated_acv IS NOT NULL AND c.estimated_acv <> '' AND toFloat(c.estimated_acv) > 1000000
RETURN c.name AS company_name, c.golden_company_id, c.estimated_acv, c.city, c.country
ORDER BY toFloat(c.estimated_acv) DESC
LIMIT 20""",
    },
    {
        "id": "companies-exactly-two-projects",
        "label": "Companies with exactly two projects",
        "cypher": """MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count = 2
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, project_count,
       collect(DISTINCT contact.full_name) AS contacts""",
    },
    {
        "id": "explore-graph",
        "label": "Explore graph (sample)",
        "cypher": """MATCH (c:Company)-[r:PARTICIPATES_IN|EMPLOYS]-(n)
RETURN c, r, n
LIMIT 50""",
    },
]


@app.route("/")
def index():
    return send_from_directory(BASE, "index.html")


@app.route("/index.html")
def index_html():
    return send_from_directory(BASE, "index.html")


def _neo4j_error_message(e, include_detail=True):
    """Return a short, actionable message for common Neo4j connection errors."""
    raw = str(e)
    msg = raw.lower()
    if "connection refused" in msg or "errno 61" in msg or "errno 111" in msg:
        return (
            "Neo4j is not running or not reachable. "
            "Start Neo4j (e.g. Neo4j Desktop) or set NEO4J_URI to a running instance (e.g. Neo4j Aura)."
        )
    if "password" in msg or "auth" in msg or "credentials" in msg or "unauthorized" in msg:
        hint = (
            "Neo4j authentication failed. For Aura: open https://console.neo4j.io, "
            "check the instance is Running, and use the password from the instance details (or reset it there)."
        )
        if include_detail and raw and raw != msg:
            return f"{hint} [Neo4j: {raw}]"
        return hint
    return raw


def _connection_info():
    """Current connection params (for debugging auth issues)."""
    _reload_env()
    return {
        "uri": os.getenv("NEO4J_URI") or NEO4J_URI,
        "username": os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER") or NEO4J_USER,
        "database": os.getenv("NEO4J_DATABASE") or "neo4j",
    }

@app.route("/api/status")
def status():
    """Check if Neo4j is configured and reachable."""
    try:
        driver = get_driver()
        if not driver:
            return jsonify({"ok": False, "error": "NEO4J_PASSWORD not set", "connection": _connection_info()})
        db = _session_database()
        with driver.session(database=db) as session:
            session.run("RETURN 1")
        driver.close()
        return jsonify({"ok": True, "connection": {"database": db}})
    except Exception as e:
        info = _connection_info()
        return jsonify({
            "ok": False,
            "error": _neo4j_error_message(e),
            "detail": str(e),
            "connection": info,
        })


@app.route("/api/examples")
def examples():
    """Return list of example questions (id, label, cypher)."""
    return jsonify(EXAMPLE_QUERIES)


@app.route("/api/query", methods=["POST"])
def query():
    """Run a Cypher query and return table rows + optional graph data."""
    body = request.get_json() or {}
    cypher = (body.get("cypher") or "").strip()
    if not cypher:
        return jsonify({"error": "Missing cypher"}), 400

    try:
        driver = get_driver()
        if not driver:
            return jsonify({"error": "Neo4j not configured (set NEO4J_PASSWORD)"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    try:
        keys, rows, graph = _run_cypher(cypher)
        out = {"columns": keys, "rows": rows}
        if graph:
            out["graph"] = graph
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e), "columns": [], "rows": []}), 200


@app.route("/api/ollama-status", methods=["GET"])
def ollama_status():
    """Check if Ollama is reachable and the configured model is available."""
    import graph_rag
    try:
        client = graph_rag._get_client()
        client.list()
        return jsonify({"ok": True})
    except Exception as e:
        msg = graph_rag.OLLAMA_UNREACHABLE_MSG if graph_rag._is_ollama_connection_error(e) else str(e)
        return jsonify({"ok": False, "error": msg}), 200


@app.route("/api/ask", methods=["POST"])
def ask():
    """Natural-language question → Ollama Cypher → Neo4j → table + summary."""
    body = request.get_json() or {}
    question = (body.get("question") or "").strip()
    if not question:
        return jsonify({"error": "Missing question"}), 400

    import graph_rag

    try:
        cypher = graph_rag.get_cypher_from_question(question)
    except Exception as e:
        err_msg = graph_rag.OLLAMA_UNREACHABLE_MSG if graph_rag._is_ollama_connection_error(e) else str(e)
        return jsonify({"error": err_msg}), 200

    try:
        keys, rows, graph = _run_cypher(cypher)
    except Exception as e:
        return jsonify({"error": str(e), "cypher": cypher}), 200

    try:
        summary = graph_rag.get_summary(question, keys, rows)
    except Exception as e:
        summary = "Summary unavailable: " + str(e)

    out = {"cypher": cypher, "columns": keys, "rows": rows, "summary": summary}
    if graph:
        out["graph"] = graph
    return jsonify(out)


@app.route("/api/graph")
def graph_sample():
    """Return a small subgraph for visualization (companies, projects, contacts)."""
    cypher = """
    MATCH (c:Company)-[r:PARTICIPATES_IN]->(p:Project)
    WITH c, p, r LIMIT 25
    MATCH (c)-[e:EMPLOYS]->(contact:Contact)
    WITH collect(DISTINCT {id: elementId(c), label: 'Company', name: c.name}) AS companies,
         collect(DISTINCT {id: elementId(p), label: 'Project', name: p.name}) AS projects,
         collect(DISTINCT {id: elementId(contact), label: 'Contact', name: contact.full_name}) AS contacts,
         collect(DISTINCT {from: elementId(c), to: elementId(p), type: 'PARTICIPATES_IN'}) AS part,
         collect(DISTINCT {from: elementId(c), to: elementId(contact), type: 'EMPLOYS'}) AS emp
    RETURN companies, projects, contacts, part, emp
    """
    # Simpler: return nodes and edges
    # id() works in Neo4j 4 and 5; elementId() only in 5
    cypher = """
    MATCH (c:Company)-[r:PARTICIPATES_IN]->(p:Project)
    RETURN id(c) AS cid, c.name AS cname,
           id(p) AS pid, p.name AS pname,
           id(r) AS rid
    LIMIT 30
    """
    try:
        driver = get_driver()
        if not driver:
            return jsonify({"error": "Neo4j not configured"}), 503
        with driver.session(database=_session_database()) as session:
            result = session.run(cypher)
            records = list(result)
        nodes = []
        edges = []
        seen_n = set()
        for rec in records:
            cid, cname, pid, pname, rid = rec.get("cid"), rec.get("cname"), rec.get("pid"), rec.get("pname"), rec.get("rid")
            if cid and cid not in seen_n:
                seen_n.add(cid)
                nodes.append({"id": cid, "label": "Company", "title": cname or cid})
            if pid and pid not in seen_n:
                seen_n.add(pid)
                nodes.append({"id": pid, "label": "Project", "title": pname or pid})
            if rid and cid and pid:
                edges.append({"id": rid, "from": cid, "to": pid, "label": "PARTICIPATES_IN"})
        return jsonify({"nodes": nodes, "edges": edges})
    except Exception as e:
        return jsonify({"error": str(e), "nodes": [], "edges": []})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
