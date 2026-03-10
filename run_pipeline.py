#!/usr/bin/env python3
"""
Run the full pipeline: golden records -> Neo4j Knowledge Graph.

1. Reads raw_data/Companies.csv, raw_data/Projects.csv, raw_data/Contacts.csv
2. Builds golden companies (one per cluster), golden projects, golden contacts
3. Writes golden/*.csv and link tables
4. Optionally loads graph into Neo4j (set NEO4J_PASSWORD)
"""
import argparse
import os
import sys
from pathlib import Path

# Project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from golden_records import run_pipeline as run_golden


def main():
    parser = argparse.ArgumentParser(description="Golden data + Neo4j KG pipeline")
    parser.add_argument("--golden-only", action="store_true", help="Only build golden records, do not load Neo4j")
    parser.add_argument("--no-clear", action="store_true", help="Do not clear Neo4j before load (append)")
    args = parser.parse_args()

    print("Step 1: Building golden records...")
    run_golden()
    print("Done.\n")

    if args.golden_only:
        print("Skipping Neo4j (--golden-only). To load graph: python neo4j_kg.py")
        return

    pwd = os.getenv("NEO4J_PASSWORD", "")
    if not pwd:
        print("Set NEO4J_PASSWORD to load into Neo4j. Skipping graph load.")
        return

    print("Step 2: Loading Knowledge Graph into Neo4j...")
    from neo4j_kg import load_knowledge_graph
    load_knowledge_graph(clear_first=not args.no_clear)
    print("Pipeline complete. Use example_queries.cypher in Neo4j Browser.")


if __name__ == "__main__":
    main()
