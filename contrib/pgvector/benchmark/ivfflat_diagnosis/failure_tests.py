#!/usr/bin/env python3
"""Run the three non-destructive first-stage IVFFlat build error cases."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common.config import metric_config, metric_names
from common.db import connect_dsn


CASES = {"empty_table": 100, "few_rows_large_lists": 3162, "invalid_lists": 0}


def run_case(dsn: str, name: str, metric: str, dimension: int) -> dict:
    notices = []
    with connect_dsn(dsn, autocommit=True) as conn:
        conn.add_notice_handler(lambda diagnostic: notices.append(str(diagnostic.message_primary)))
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS baseline_failure CASCADE")
            cur.execute(f"CREATE TABLE baseline_failure (id integer, embedding vector({dimension}))")
            if name == "few_rows_large_lists":
                cur.execute(f"INSERT INTO baseline_failure SELECT i, array_fill(i::real, ARRAY[{dimension}])::vector FROM generate_series(1, 10) i")
            try:
                cur.execute(f"CREATE INDEX baseline_failure_idx ON baseline_failure USING ivfflat "
                            f"(embedding {metric_config(metric)['opclass']}) WITH (lists={CASES[name]})")
                success, error = True, None
            except Exception as exc:
                success, error = False, str(exc)
            cur.execute("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('baseline_failure_idx')")
            row = cur.fetchone(); index_state = None if row is None else {"valid": row[0]}
            cur.execute("DROP TABLE baseline_failure CASCADE")
    return {"case": name, "metric": metric, "success": success, "notices": notices,
            "error": error, "index_state": index_state}


def run_all(dsn: str, metric: str, dimension: int) -> list[dict]:
    return [run_case(dsn, name, metric, dimension) for name in CASES]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--metric", choices=metric_names(), required=True)
    parser.add_argument("--dimension", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = run_all(args.dsn, args.metric, args.dimension)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(json.dumps(row) for row in results) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
