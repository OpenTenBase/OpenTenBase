#!/usr/bin/env python3
"""Initialize/start the isolated cluster and create the benchmark database."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.config import PGDATA, PGHOST, PGLOG, PGPORT, PROJECT_DATABASES
from common.db import (connect_dsn, ensure_cluster, ensure_database,
                       ensure_server_running)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare the local PostgreSQL cluster used by the benchmarks."
    )
    parser.parse_args()

    pgdata = ensure_cluster()
    ensure_server_running(pgdata)
    dsn = ensure_database("build")
    with connect_dsn(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

    print(json.dumps({
        "pgdata": str(PGDATA.resolve()),
        "host": PGHOST,
        "port": PGPORT,
        "logfile": str(PGLOG.resolve()),
        "database": PROJECT_DATABASES["build"],
        "status": "ready",
    }, indent=2))


if __name__ == "__main__":
    main()
