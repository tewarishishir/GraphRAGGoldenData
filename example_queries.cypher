// Example Cypher queries for the Knowledge Graph (Neo4j)
// Schema: Company (name, golden_company_id, acv_usd), Project (name, is_active), Contact (full_name)
// Relationships: (Company)-[:PARTICIPATES_IN]->(Project), (Company)-[:EMPLOYS]->(Contact)

// ---------------------------------------------------------------------------
// Graph stats — always returns one row (use OPTIONAL MATCH so empty graph still returns 0s)
// ---------------------------------------------------------------------------
OPTIONAL MATCH (c:Company) WITH count(c) AS companies
OPTIONAL MATCH (p:Project) WITH companies, count(p) AS projects
OPTIONAL MATCH (x:Contact) WITH companies, projects, count(x) AS contacts
OPTIONAL MATCH ()-[r:PARTICIPATES_IN]->() WITH companies, projects, contacts, count(r) AS participates_in
OPTIONAL MATCH ()-[e:EMPLOYS]->() WITH companies, projects, contacts, participates_in, count(e) AS employs
RETURN companies, projects, contacts, participates_in, employs;


// ---------------------------------------------------------------------------
// List companies (sample)
// ---------------------------------------------------------------------------
MATCH (c:Company) RETURN c.name AS company_name, c.golden_company_id, c.city, c.country, c.acv_usd LIMIT 20;


// ---------------------------------------------------------------------------
// List projects (sample)
// ---------------------------------------------------------------------------
MATCH (p:Project) RETURN p.name AS project_name, p.golden_project_id, p.stage, p.is_active LIMIT 20;


// ---------------------------------------------------------------------------
// Companies and users (contacts) who have two or more projects
// ---------------------------------------------------------------------------
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count >= 2
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, c.golden_company_id, project_count,
       collect(DISTINCT contact.full_name) AS contacts
ORDER BY project_count DESC;


// ---------------------------------------------------------------------------
// Companies with active projects and ACV > 5 (is_active and acv_usd safe)
// ---------------------------------------------------------------------------
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WHERE toLower(trim(toString(coalesce(p.is_active, '')))) = 'true'
  AND c.acv_usd IS NOT NULL AND c.acv_usd <> '' AND toFloat(c.acv_usd) > 5
RETURN c.name, c.acv_usd, collect(p.name) AS projects
ORDER BY toFloat(c.acv_usd) DESC
LIMIT 10;


// ---------------------------------------------------------------------------
// Companies with exactly two projects
// ---------------------------------------------------------------------------
MATCH (c:Company)-[:PARTICIPATES_IN]->(p:Project)
WITH c, count(p) AS project_count
WHERE project_count = 2
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c.name AS company_name, project_count,
       collect(DISTINCT contact.full_name) AS contacts;


// ---------------------------------------------------------------------------
// All relationships from a company (use $company_id in app)
// ---------------------------------------------------------------------------
MATCH (c:Company {golden_company_id: $company_id})
OPTIONAL MATCH (c)-[:PARTICIPATES_IN]->(p:Project)
OPTIONAL MATCH (c)-[:EMPLOYS]->(contact:Contact)
RETURN c, collect(DISTINCT p) AS projects, collect(DISTINCT contact) AS contacts;


// ---------------------------------------------------------------------------
// Individual counts (for Neo4j Browser)
// ---------------------------------------------------------------------------
MATCH (c:Company) RETURN count(c) AS companies;
MATCH (p:Project) RETURN count(p) AS projects;
MATCH (c:Contact) RETURN count(c) AS contacts;
MATCH ()-[r:PARTICIPATES_IN]->() RETURN count(r) AS participates_in;
MATCH ()-[r:EMPLOYS]->() RETURN count(r) AS employs;
