"""PostgreSQL cluster, benchmark database, table, and vector-loading helpers."""

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Iterator

import numpy as np
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .config import (PGBIN, PGDATA, PGHOST, PGLOG, PGPORT, PGUSER,
                     PROJECT_DATABASES)

TABLE_NAME = "vector_base"
INDEX_NAME = "vector_base_embedding_idx"


def checked_name(name: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"unsafe SQL identifier: {name}")
    return name


def _binary(name: str) -> str:
    candidates = []
    if PGBIN:
        candidates.append(PGBIN / name)
    found = shutil.which(name)
    if found:
        candidates.append(Path(found))
    candidates.append(Path("/usr/local/pgsql/bin") / name)
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError(f"PostgreSQL command {name!r} was not found; set PGBIN")


def _run(command: list[str], description: str, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, text=True, capture_output=True)
    if check and result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"{description} failed (exit {result.returncode}): {detail}")
    return result


def ensure_cluster(pgdata: Path = PGDATA) -> Path:
    pgdata = Path(pgdata).resolve()
    if (pgdata / "PG_VERSION").is_file():
        return pgdata
    if pgdata.exists() and any(pgdata.iterdir()):
        raise RuntimeError(f"PGDATA exists but is not an initialized cluster: {pgdata}")
    pgdata.parent.mkdir(parents=True, exist_ok=True)
    _run([_binary("initdb"), "-D", str(pgdata)], f"initdb for {pgdata}")
    if not (pgdata / "PG_VERSION").is_file():
        raise RuntimeError(f"initdb completed without creating {pgdata / 'PG_VERSION'}")
    return pgdata


def ensure_server_running(pgdata: Path = PGDATA, logfile: Path = PGLOG,
                          host: str = PGHOST, port: int = PGPORT) -> None:
    pgdata = Path(pgdata).resolve()
    status = _run([_binary("pg_ctl"), "-D", str(pgdata), "status"],
                  f"pg_ctl status for {pgdata}", check=False)
    if status.returncode == 0:
        return
    logfile = Path(logfile).resolve()
    logfile.parent.mkdir(parents=True, exist_ok=True)
    options = ["-p", str(port)]
    if host.startswith("/"):
        Path(host).mkdir(parents=True, exist_ok=True)
        options.extend(["-k", host])
    _run([_binary("pg_ctl"), "-D", str(pgdata), "-l", str(logfile),
          "-o", " ".join(options), "start"], f"starting PostgreSQL from {pgdata}")


def _dsn_for(database: str, dsn: str | None = None) -> str:
    values = conninfo_to_dict(dsn or "")
    values["dbname"] = database
    values.setdefault("host", PGHOST)
    values.setdefault("port", str(PGPORT))
    if PGUSER:
        values.setdefault("user", PGUSER)
    return make_conninfo(**values)


def ensure_database(project: str, dsn: str | None = None) -> str:
    try:
        database = PROJECT_DATABASES[project]
    except KeyError as exc:
        raise ValueError(f"unknown benchmark project {project!r}; choose build") from exc
    maintenance_dsn = _dsn_for("postgres", dsn)
    with psycopg.connect(maintenance_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    return _dsn_for(database, dsn)


def project_dsn(project: str, dsn: str | None = None) -> str:
    """Return a connection string for an existing project database."""
    try:
        database = PROJECT_DATABASES[project]
    except KeyError as exc:
        raise ValueError(f"unknown benchmark project {project!r}; choose build") from exc
    return _dsn_for(database, dsn)


def connect(project: str, dsn: str | None = None, **kwargs):
    return psycopg.connect(project_dsn(project, dsn), **kwargs)


def connect_dsn(dsn: str, **kwargs):
    """Connect to an already project-scoped DSN (used by monitor threads)."""
    return psycopg.connect(dsn, **kwargs)


def prepare_table(conn, dimension: int, recreate: bool = True,
                  table: str = TABLE_NAME) -> None:
    table = checked_name(table)
    if dimension < 1:
        raise ValueError("dimension must be positive")
    identifier = sql.Identifier(table)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        if recreate:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(identifier))
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} "
                            "(id integer PRIMARY KEY, embedding vector({}) NOT NULL)")
                    .format(identifier, sql.Literal(dimension)))
    conn.commit()


def vector_text(vector: np.ndarray) -> str:
    return "[" + ",".join(map(str, vector.tolist())) + "]"


def vector_texts(vectors: np.ndarray) -> Iterator[str]:
    for vector in vectors:
        yield vector_text(vector)


def load_vectors(conn, vectors: np.ndarray, limit: int = 0,
                 dimension: int | None = None, table: str = TABLE_NAME) -> int:
    table = checked_name(table)
    dimension = dimension or vectors.shape[1]
    if vectors.ndim != 2 or vectors.shape[1] != dimension:
        raise ValueError(f"expected vectors with dimension {dimension}, got {vectors.shape}")
    if limit:
        vectors = vectors[:limit]
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(INDEX_NAME)))
        cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(table)))
        with cur.copy(sql.SQL("COPY {} (id, embedding) FROM STDIN").format(sql.Identifier(table))) as copy:
            for row_id, text in enumerate(vector_texts(vectors)):
                copy.write_row((row_id, text))
                if row_id and row_id % 100000 == 0:
                    print(f"loaded {row_id}/{len(vectors)}", flush=True)
        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table)))
    conn.commit()
    return len(vectors)


def versions(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        postgres = cur.fetchone()[0]
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
    return {"postgresql": postgres, "pgvector": row[0] if row else None}


def server_settings(conn) -> dict:
    names = (
        "block_size", "data_directory", "log_directory", "log_temp_files",
        "maintenance_io_concurrency", "maintenance_work_mem",
        "max_parallel_maintenance_workers", "max_parallel_workers",
        "max_worker_processes", "port", "shared_buffers", "track_io_timing",
        "work_mem",
    )
    return fetch_settings(conn, names)


def fetch_settings(conn, names) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s)", (list(names),))
        return {name: f"{setting}{unit or ''}" for name, setting, unit in cur.fetchall()}


def relation_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
        return bool(cur.fetchone()[0])


def function_exists(conn, signature: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regprocedure(%s) IS NOT NULL", (signature,))
        return bool(cur.fetchone()[0])
