"""Create the manifest recorded with every baseline run."""

from datetime import datetime, timezone
import os
import platform
from pathlib import Path
import re

from .db import server_settings, versions
from .schema import SCHEMA_VERSION, utc_now


def machine_info() -> dict:
    memory_bytes = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        match = re.search(r"^MemTotal:\s+(\d+) kB", meminfo.read_text(), re.MULTILINE)
        if match:
            memory_bytes = int(match.group(1)) * 1024
    cpu = platform.processor()
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        match = re.search(r"^model name\s*:\s*(.+)$", cpuinfo.read_text(), re.MULTILINE)
        if match:
            cpu = match.group(1).strip()
    return {"os": platform.platform(), "python": platform.python_version(),
            "cpu": cpu or platform.machine(), "cpu_count": os.cpu_count(),
            "memory_bytes": memory_bytes}


def build_manifest(conn, experiment: str, **settings) -> dict:
    return {
        "schema_version": settings.pop("schema_version", SCHEMA_VERSION),
        "diagnosis_id": settings.pop("diagnosis_id", None),
        "timestamp_utc": utc_now(),
        "source": "project2",
        "availability": "available",
        "reason": None,
        "run_started_at": datetime.now(timezone.utc).isoformat(),
        "experiment": experiment,
        **versions(conn),
        "machine": machine_info(),
        "postgresql_settings": server_settings(conn),
        **settings,
    }
