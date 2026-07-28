#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.
"""Run the reproducible OpenTenBase benchmark matrix."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence

from analyze_results import SUMMARY_FIELDS, analyze, parse_pgbench_output


ROOT = Path(__file__).resolve().parent
READ_WORKLOADS = {
    "point_read",
    "aggregate",
    "join_colocated",
    "join_replicated",
    "join_redistributed",
}
KNOWN_WORKLOADS = READ_WORKLOADS | {"write", "short_tx"}


def load_config(path: Path) -> Dict[str, object]:
    config = json.loads(path.read_text(encoding="utf-8"))
    required_sections = ("connection", "cluster", "data", "run", "workloads")
    missing = [name for name in required_sections if name not in config]
    if missing:
        raise ValueError(f"missing config sections: {', '.join(missing)}")

    connection = config["connection"]
    data = config["data"]
    run = config["run"]
    cluster = config["cluster"]
    if not isinstance(connection, dict) or not isinstance(data, dict):
        raise ValueError("connection and data must be JSON objects")
    if not isinstance(run, dict) or not isinstance(cluster, dict):
        raise ValueError("run and cluster must be JSON objects")
    for field in ("host", "port", "user", "database"):
        if field not in connection:
            raise ValueError(f"connection.{field} is required")
    for field in ("account_count", "order_count", "event_count"):
        if int(data.get(field, 0)) <= 0:
            raise ValueError(f"data.{field} must be positive")
    if int(data["account_count"]) <= 1000:
        raise ValueError("data.account_count must be greater than 1000")
    clients = run.get("clients")
    if not isinstance(clients, list) or not clients or any(int(item) <= 0 for item in clients):
        raise ValueError("run.clients must be a non-empty list of positive integers")
    sample_rate = float(run.get("latency_sample_rate", 0.1))
    if not 0 < sample_rate <= 1:
        raise ValueError("run.latency_sample_rate must be in (0, 1]")
    workloads = config["workloads"]
    if not isinstance(workloads, list) or not workloads:
        raise ValueError("workloads must be a non-empty list")
    unknown = set(str(item) for item in workloads) - KNOWN_WORKLOADS
    if unknown:
        raise ValueError(f"unknown workloads: {', '.join(sorted(unknown))}")
    group = str(cluster.get("node_group", ""))
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", group):
        raise ValueError("cluster.node_group must be an unquoted SQL identifier")
    if "password" in connection:
        raise ValueError("do not store passwords in config; use PGPASSWORD or .pgpass")
    return config


def find_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise FileNotFoundError(f"required tool not found on PATH: {name}")
    return path


def psql_connection_args(config: Mapping[str, object]) -> List[str]:
    connection = config["connection"]
    assert isinstance(connection, dict)
    return [
        "-h",
        str(connection["host"]),
        "-p",
        str(connection["port"]),
        "-U",
        str(connection["user"]),
        "-d",
        str(connection["database"]),
    ]


def pgbench_connection_args(config: Mapping[str, object]) -> List[str]:
    """Build arguments compatible with the PostgreSQL 10-era pgbench CLI."""

    connection = config["connection"]
    assert isinstance(connection, dict)
    return [
        "-h",
        str(connection["host"]),
        "-p",
        str(connection["port"]),
        "-U",
        str(connection["user"]),
    ]


def psql_variables(config: Mapping[str, object]) -> List[str]:
    data = config["data"]
    cluster = config["cluster"]
    assert isinstance(data, dict) and isinstance(cluster, dict)
    values = {
        "account_count": int(data["account_count"]),
        "order_count": int(data["order_count"]),
        "event_count": int(data["event_count"]),
        "group_name": str(cluster["node_group"]),
    }
    result: List[str] = []
    for key, value in values.items():
        result.extend(("-v", f"{key}={value}"))
    return result


def run_process(
    command: Sequence[str], output: Path, keep_going: bool = False
) -> subprocess.CompletedProcess[str]:
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    process = subprocess.run(
        list(command),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=os.environ.copy(),
        check=False,
    )
    header = (
        f"# started_utc={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(started))}\n"
        f"# elapsed_seconds={time.time() - started:.3f}\n"
        f"# exit_code={process.returncode}\n"
        f"# command={' '.join(command)}\n\n"
    )
    output.write_text(header + process.stdout, encoding="utf-8")
    if process.returncode and not keep_going:
        raise RuntimeError(f"command failed ({process.returncode}); see {output}")
    return process


def psql_file_command(
    psql: str,
    config: Mapping[str, object],
    sql_file: Path,
    tuples_only: bool = False,
) -> List[str]:
    command = [
        psql,
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        *psql_connection_args(config),
    ]
    if tuples_only:
        command.extend(("-A", "-t", "-F", ","))
    command.extend(psql_variables(config))
    command.extend(("-f", str(sql_file)))
    return command


def capture_version(tool: str) -> str:
    process = subprocess.run(
        [tool, "--version"],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return process.stdout.strip()


def parse_shard_count(text: str) -> Optional[int]:
    match = re.search(r"benchmark_shard_count=(\d+)", text)
    return int(match.group(1)) if match else None


def source_commit() -> str:
    override = os.environ.get("OPENTENBASE_COMMIT")
    if override:
        return override
    process = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return process.stdout.strip() if process.returncode == 0 else "unknown"


def write_environment(
    output_dir: Path,
    config: Mapping[str, object],
    psql: str,
    pgbench: Optional[str],
) -> None:
    environment = dict(config)
    environment["tools"] = {
        "python": sys.version.split()[0],
        "psql": capture_version(psql),
        "pgbench": capture_version(pgbench) if pgbench else "not found",
    }
    environment["run_started_utc"] = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
    )
    environment["source_commit"] = source_commit()
    (output_dir / "environment.json").write_text(
        json.dumps(environment, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_preflight(
    psql: str, config: Mapping[str, object], output_dir: Path, keep_going: bool
) -> None:
    process = run_process(
        psql_file_command(psql, config, ROOT / "sql" / "preflight.sql"),
        output_dir / "raw" / "preflight.txt",
        keep_going,
    )
    if "OpenTenBase" not in process.stdout and not keep_going:
        raise RuntimeError("target does not identify itself as OpenTenBase")
    shard_count = parse_shard_count(process.stdout)
    if shard_count is None and not keep_going:
        raise RuntimeError("preflight did not return a sharding map count")
    if shard_count == 0 and not keep_going:
        raise RuntimeError(
            "configured node group has no sharding map; run "
            "CREATE SHARDING GROUP TO GROUP <group> as an administrator"
        )


def run_setup(
    psql: str, config: Mapping[str, object], output_dir: Path, keep_going: bool
) -> None:
    for name in ("schema.sql", "load.sql"):
        run_process(
            psql_file_command(psql, config, ROOT / "sql" / name),
            output_dir / "raw" / name.replace(".sql", ".txt"),
            keep_going,
        )


def collect_database_evidence(
    psql: str, config: Mapping[str, object], output_dir: Path, keep_going: bool
) -> None:
    run_process(
        psql_file_command(psql, config, ROOT / "sql" / "explain.sql"),
        output_dir / "raw" / "explain.txt",
        keep_going,
    )
    process = run_process(
        psql_file_command(
            psql, config, ROOT / "sql" / "distribution.sql", tuples_only=True
        ),
        output_dir / "raw" / "distribution.txt",
        keep_going,
    )
    rows = [
        line
        for line in process.stdout.splitlines()
        if line and not line.startswith("#") and line.count(",") == 2
    ]
    distribution = output_dir / "distribution.csv"
    distribution.write_text(
        "table_name,node_id,row_count\n" + "\n".join(rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def pgbench_command(
    pgbench: str,
    config: Mapping[str, object],
    workload: str,
    clients: int,
    duration: int,
    log_prefix: Optional[Path] = None,
) -> List[str]:
    run = config["run"]
    data = config["data"]
    assert isinstance(run, dict) and isinstance(data, dict)
    jobs = min(int(run.get("jobs", clients)), clients)
    mode = str(run.get("mode", "prepared"))
    command = [
        pgbench,
        *pgbench_connection_args(config),
        "-n",
        "-M",
        mode,
        "-c",
        str(clients),
        "-j",
        str(jobs),
        "-T",
        str(duration),
        "-r",
        "-f",
        str(ROOT / "workloads" / f"{workload}.sql"),
    ]
    if log_prefix is not None:
        command.extend(
            (
                "-l",
                f"--log-prefix={log_prefix}",
                f"--sampling-rate={float(run.get('latency_sample_rate', 0.1))}",
            )
        )
    for name in ("account_count", "order_count", "event_count"):
        command.extend(("-D", f"{name}={int(data[name])}"))
    connection = config["connection"]
    assert isinstance(connection, dict)
    command.append(str(connection["database"]))
    return command


def latency_percentiles(log_prefix: Path) -> Dict[str, float]:
    """Read sampled pgbench logs and return nearest-rank latency percentiles."""

    values: List[float] = []
    for path in sorted(log_prefix.parent.glob(log_prefix.name + ".*")):
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                fields = line.split()
                if len(fields) >= 3 and fields[2].isdigit():
                    values.append(int(fields[2]) / 1000)
    values.sort()

    def percentile(fraction: float) -> float:
        if not values:
            return 0.0
        rank = max(1, int(len(values) * fraction + 0.999999999))
        return values[min(rank - 1, len(values) - 1)]

    return {
        "latency_p50_ms": percentile(0.50),
        "latency_p95_ms": percentile(0.95),
        "latency_p99_ms": percentile(0.99),
        "latency_sample_count": len(values),
    }


def warmup(
    pgbench: str,
    config: Mapping[str, object],
    output_dir: Path,
    keep_going: bool,
) -> None:
    run = config["run"]
    assert isinstance(run, dict)
    seconds = int(run.get("warmup_seconds", 0))
    if seconds <= 0:
        return
    for workload in config["workloads"]:
        workload = str(workload)
        if workload not in READ_WORKLOADS:
            continue
        run_process(
            pgbench_command(pgbench, config, workload, 1, seconds),
            output_dir / "raw" / f"warmup-{workload}.txt",
            keep_going,
        )


def run_matrix(
    pgbench: str,
    config: Mapping[str, object],
    output_dir: Path,
    keep_going: bool,
) -> None:
    run = config["run"]
    assert isinstance(run, dict)
    duration = int(run["duration_seconds"])
    summary_path = output_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for workload in config["workloads"]:
            for clients_value in run["clients"]:
                workload = str(workload)
                clients = int(clients_value)
                jobs = min(int(run.get("jobs", clients)), clients)
                relative_log = Path("raw") / f"{workload}-c{clients}.txt"
                transaction_prefix = (
                    output_dir / "raw" / f"txlog-{workload}-c{clients}"
                )
                process = run_process(
                    pgbench_command(
                        pgbench,
                        config,
                        workload,
                        clients,
                        duration,
                        transaction_prefix,
                    ),
                    output_dir / relative_log,
                    keep_going=True,
                )
                parsed = parse_pgbench_output(process.stdout)
                percentiles = latency_percentiles(transaction_prefix)
                writer.writerow(
                    {
                        "workload": workload,
                        "clients": clients,
                        "jobs": jobs,
                        "duration_s": duration,
                        "transactions": parsed["transactions"] or 0,
                        "failed": parsed["failed"] or 0,
                        "latency_avg_ms": parsed["latency_avg_ms"] or 0,
                        "latency_stddev_ms": parsed["latency_stddev_ms"] or 0,
                        **percentiles,
                        "tps": parsed["tps"] or 0,
                        "exit_code": process.returncode,
                        "raw_log": relative_log.as_posix(),
                    }
                )
                handle.flush()
                if process.returncode and not keep_going:
                    raise RuntimeError(
                        f"pgbench failed for {workload} at c={clients}; "
                        f"see {output_dir / relative_log}"
                    )


def prepare_output(path: Optional[Path]) -> Path:
    output = path or Path(
        "benchmark-results",
        time.strftime("%Y%m%d-%H%M%S", time.localtime()),
    )
    output.mkdir(parents=True, exist_ok=True)
    (output / "raw").mkdir(exist_ok=True)
    (output / "host_metrics").mkdir(exist_ok=True)
    return output.resolve()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("preflight", "setup", "run", "analyze", "all")
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="record failed samples and continue the matrix",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    output_dir = prepare_output(args.output_dir)
    if args.action == "analyze":
        analyze(output_dir, output_dir / "report.md")
        print(f"report written to {output_dir / 'report.md'}")
        return 0

    psql = find_tool("psql")
    pgbench = shutil.which("pgbench")
    if args.action in ("run", "all") and pgbench is None:
        raise FileNotFoundError("required tool not found on PATH: pgbench")
    write_environment(output_dir, config, psql, pgbench)
    if args.action in ("preflight", "setup", "run", "all"):
        run_preflight(psql, config, output_dir, args.keep_going)
    if args.action in ("setup", "all"):
        run_setup(psql, config, output_dir, args.keep_going)
    if args.action in ("run", "all"):
        assert pgbench is not None
        collect_database_evidence(psql, config, output_dir, args.keep_going)
        warmup(pgbench, config, output_dir, args.keep_going)
        run_matrix(pgbench, config, output_dir, args.keep_going)
        analyze(output_dir, output_dir / "report.md")
    print(f"benchmark artifacts: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
