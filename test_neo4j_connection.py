#!/usr/bin/env python3
"""
Test Neo4j connection and print the exact error if it fails.
Run from project root: python test_neo4j_connection.py
"""
import os
import re
import sys

# Load .env before importing config
try:
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parent / ".env", override=True)
except ImportError:
    pass

from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE

def _extract_instance_id(uri):
    m = re.match(r"neo4j\+s://([a-f0-9]+)\.databases\.neo4j\.io", uri)
    return m.group(1) if m else None

def main():
    print("NEO4J_URI:", NEO4J_URI)
    print("NEO4J_USER (from .env):", NEO4J_USER)
    print("NEO4J_PASSWORD:", "***" if NEO4J_PASSWORD else "(not set)")
    print()

    if not NEO4J_PASSWORD:
        print("ERROR: NEO4J_PASSWORD is not set (check .env)")
        sys.exit(1)

    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("ERROR: Install the neo4j driver: pip install neo4j")
        sys.exit(1)

    db = NEO4J_DATABASE
    print("Using database (NEO4J_DATABASE):", db)

    # For Aura, try both NEO4J_USER and the other common form (instance id or "neo4j")
    usernames_to_try = [NEO4J_USER]
    if NEO4J_URI.startswith("neo4j+s://"):
        instance_id = _extract_instance_id(NEO4J_URI)
        if instance_id and instance_id != NEO4J_USER:
            usernames_to_try.append(instance_id)
        if "neo4j" not in usernames_to_try:
            usernames_to_try.append("neo4j")

    last_error = None
    for username in usernames_to_try:
        print("Trying username:", username, "... ", end="", flush=True)
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(username, NEO4J_PASSWORD))
            driver.verify_connectivity()
            with driver.session(database=db) as session:
                r = session.run("RETURN 1 AS n")
                print("OK. Query result:", r.single()["n"])
            driver.close()
            print()
            print("Connection successful with username:", username)
            print("Update your .env with: NEO4J_USERNAME=" + username)
            return
        except Exception as e:
            last_error = e
            print("failed.")
            if type(e).__name__ == "AuthError":
                continue
            break

    print()
    print("CONNECTION FAILED (all usernames tried)")
    print("Exception type:", type(last_error).__name__)
    print("Message:", last_error)
    print()
    print("--- Fix Aura AuthError ---")
    print("1. Open https://console.neo4j.io and open your instance.")
    print("2. Find 'Connection details' or 'Reset DBMS password'.")
    print("3. Click 'Reset DBMS password', set a NEW password, and copy it.")
    print("4. In .env set:  NEO4J_PASSWORD=<paste the new password>")
    print("5. Run this script again.")
    sys.exit(1)

if __name__ == "__main__":
    main()
