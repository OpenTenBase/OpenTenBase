#!/usr/bin/env python3
"""Cheap pre-commit guard for the generated extension SQL."""
from pathlib import Path
import sys

sql = Path(__file__).parents[1].joinpath("pg_graph_search--1.0.0.sql").read_text()
for name in ("_norm", "plan", "install", "uninstall"):
    if f"FUNCTION graph_search.{name}" not in sql:
        print("missing meta-function:", name)
        sys.exit(1)
if "search_path" in sql.lower():
    print("generated API must use schema-qualified names, not search_path")
    sys.exit(1)
print("ok: extension assembly has required meta-functions and no search_path dependency")

