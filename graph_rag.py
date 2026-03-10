"""
LLM + Graph RAG: natural-language question → Cypher (via Ollama) → Neo4j → table + summary.
"""
import re
import os

# Schema for the knowledge graph (injected into LLM system prompt)
GRAPH_SCHEMA = """
Node labels and properties:
- Company: name (string), golden_company_id (string), city, country, website, acv_usd (string, numeric), estimated_acv (string, numeric)
- Project: name (string), golden_project_id (string), stage, is_active (string "true"/"false"), estimated_value_usd (string)
- Contact: full_name (string), golden_contact_id (string), email, job_title

Relationships:
- (Company)-[:PARTICIPATES_IN]->(Project)
- (Company)-[:EMPLOYS]->(Contact)

Property disambiguation (critical):
- ESTIMATED_ACV and acv_usd are DIFFERENT. When the user asks about "estimated ACV", "ESTIMATED_ACV", or "estimated_acv" use c.estimated_acv. When they ask only about "ACV" or "acv" without "estimated" use c.acv_usd.

Syntax rules (critical):
- Always use a DOT to access properties: c.name, p.is_active, contact.full_name. Never write "c name" or "c name" — it must be c.name.
- For "count of related nodes" or "X or more projects": MATCH the relationship, then use WITH variable, count(...) AS n WHERE n >= N.
- IN (list) expects a literal list or subquery result, e.g. WHERE x IN [1,2,3]. Do not write WHERE c.golden_company_id IN (p.golden_project_id) — Company and Project are different nodes; for "companies with 2+ projects" use WITH c, count(p) AS project_count WHERE project_count >= 2.

Rules:
- Generate READ-ONLY Cypher only (no CREATE, MERGE, DELETE, SET, REMOVE).
- Use OPTIONAL MATCH when the question might have no related nodes (e.g. contacts).
- Prefer LIMIT (e.g. 50) to avoid large results.
- RETURN only scalar values, aggregates, or lists (e.g. collect(...)); avoid returning raw node/relationship objects.
- For is_active use: toLower(trim(toString(coalesce(p.is_active, '')))) = 'true'
- For numeric acv_usd (when user says ACV only, not estimated): c.acv_usd IS NOT NULL AND c.acv_usd <> '' AND toFloat(c.acv_usd) > N
- For numeric estimated_acv (when user says estimated ACV or ESTIMATED_ACV): c.estimated_acv IS NOT NULL AND c.estimated_acv <> '' AND toFloat(c.estimated_acv) > N
"""

# Examples so the model uses the right property and pattern.
CYPHER_EXAMPLE_PROJECTS = """
Example — question: "Companies and contacts who have 2 or more projects"
Correct Cypher:
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count >= 2
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, c.golden_company_id, project_count, collect(DISTINCT contact.full_name) AS contacts
ORDER BY project_count DESC
LIMIT 50
"""

CYPHER_EXAMPLE_ESTIMATED_ACV = """
Example — question: "Companies with estimated ACV (ESTIMATED_ACV) > 50000?"
Correct Cypher (use c.estimated_acv, NOT c.acv_usd):
MATCH (c:Company)
WHERE c.estimated_acv IS NOT NULL AND c.estimated_acv <> '' AND toFloat(c.estimated_acv) > 50000
RETURN c.name AS company_name, c.golden_company_id, c.estimated_acv
ORDER BY toFloat(c.estimated_acv) DESC
LIMIT 50
"""

CYPHER_SYSTEM_PROMPT = f"""You are a Neo4j Cypher expert. Given the following graph schema, generate exactly one Cypher query that answers the user's question.

{GRAPH_SCHEMA}
{CYPHER_EXAMPLE_PROJECTS}
{CYPHER_EXAMPLE_ESTIMATED_ACV}

Output only the Cypher query, no markdown code blocks, no explanation. Use correct Cypher syntax: variable.property with a dot. When the user asks about "estimated ACV" or "ESTIMATED_ACV" use c.estimated_acv (not c.acv_usd). Do not include CREATE, MERGE, DELETE, SET, or REMOVE."""

SUMMARY_SYSTEM_PROMPT = """You summarize query results in 2-3 short sentences. Be factual and concise. If the table is empty, say so clearly."""

# Write keywords that must not appear in generated Cypher
WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|MERGE|DELETE|SET|REMOVE|DROP)\b",
    re.IGNORECASE,
)

# Allowed Cypher starters (read-only)
ALLOWED_STARTERS = ("MATCH", "OPTIONAL MATCH", "RETURN", "CALL", "WITH", "UNWIND")


def _response_content(response) -> str:
    """Extract message content from ollama chat response (dict or object)."""
    if response is None:
        return ""
    if isinstance(response, dict):
        msg = response.get("message")
        return (msg.get("content") if isinstance(msg, dict) else "") or ""
    msg = getattr(response, "message", None)
    return getattr(msg, "content", None) or ""


def _get_client():
    from ollama import Client
    host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    return Client(host=host)


OLLAMA_UNREACHABLE_MSG = (
    "Ollama is not running or not reachable. "
    "Install from https://ollama.com/download, then run: ollama serve && ollama pull llama3.2"
)


def _is_ollama_connection_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        "connection" in msg
        or "refused" in msg
        or "failed to connect" in msg
        or "connecterror" in msg
        or "ollama.com" in msg
    )


def _raise_ollama_connection_error(e: Exception) -> None:
    if _is_ollama_connection_error(e):
        raise RuntimeError(OLLAMA_UNREACHABLE_MSG) from e
    raise


def _strip_cypher(raw: str) -> str:
    """Remove markdown code blocks and trim to get a single Cypher string."""
    text = raw.strip()
    # Remove ```cypher ... ``` or ``` ... ```
    for pattern in (r"```(?:cypher)?\s*\n?(.*?)\n?```", r"```\s*(.*?)```"):
        m = re.search(pattern, text, re.DOTALL)
        if m:
            text = m.group(1).strip()
    return text.strip()


def _is_read_only_cypher(cypher: str) -> bool:
    if not cypher or not cypher.strip():
        return False
    upper = cypher.strip().upper()
    if WRITE_KEYWORDS.search(cypher):
        return False
    first_word = upper.split()[0] if upper.split() else ""
    return first_word in ("MATCH", "OPTIONAL", "RETURN", "CALL", "WITH", "UNWIND")


def get_cypher_from_question(question: str, model: str | None = None, timeout: int = 60) -> str:
    """
    Use Ollama to generate a single Cypher query from a natural-language question.
    Raises ValueError if the model returns something that is not read-only Cypher.
    """
    model = model or os.getenv("OLLAMA_MODEL", "llama3.2")
    try:
        client = _get_client()
        response = client.chat(
            model=model,
            messages=[
                {"role": "system", "content": CYPHER_SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
        )
        content = _response_content(response)
        cypher = _strip_cypher(content)
        if not cypher:
            raise ValueError("LLM returned empty Cypher.")
        if not _is_read_only_cypher(cypher):
            raise ValueError(
                "Generated query is not read-only or invalid. Only MATCH/OPTIONAL MATCH/RETURN/CALL/WITH/UNWIND are allowed."
            )
        return cypher
    except Exception as e:
        _raise_ollama_connection_error(e)


def get_summary(
    question: str,
    columns: list,
    rows: list[dict],
    model: str | None = None,
    max_rows: int = 20,
    timeout: int = 30,
) -> str:
    """
    Use Ollama to generate a 2-3 sentence summary of the query result.
    """
    model = model or os.getenv("OLLAMA_MODEL", "llama3.2")
    truncated = rows[:max_rows]
    table_desc = f"Columns: {columns}. Number of rows: {len(rows)}."
    if truncated:
        table_desc += f" First {len(truncated)} rows: {truncated}"
    else:
        table_desc += " The query returned no rows."

    try:
        client = _get_client()
        response = client.chat(
            model=model,
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                {"role": "user", "content": f"User asked: {question}\n\n{table_desc}\n\nSummarize in 2-3 sentences."},
            ],
        )
        content = _response_content(response)
        return content.strip() or "No summary generated."
    except Exception as e:
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            return "Summary skipped: Ollama unavailable."
        return f"Summary skipped: {e}"
