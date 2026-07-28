#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.
"""Collect Linux host CPU, disk and network counters from /proc."""

from __future__ import annotations

import argparse
import csv
import os
import socket
import time
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Sequence, Tuple


FIELDS = (
    "timestamp",
    "host",
    "role",
    "cpu_pct",
    "iowait_pct",
    "disk_read_bps",
    "disk_write_bps",
    "net_rx_bps",
    "net_tx_bps",
)


def parse_cpu(text: str) -> Tuple[int, int, int]:
    line = next(line for line in text.splitlines() if line.startswith("cpu "))
    values = [int(value) for value in line.split()[1:]]
    values += [0] * (8 - len(values))
    idle = values[3]
    iowait = values[4]
    total = sum(values[:8])
    return total, idle, iowait


def parse_diskstats(text: str, devices: Optional[Iterable[str]] = None) -> Tuple[int, int]:
    selected = set(devices or ())
    read_sectors = 0
    written_sectors = 0
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 14:
            continue
        name = fields[2]
        if selected and name not in selected:
            continue
        if not selected and name.startswith(("loop", "ram", "fd", "sr", "dm-")):
            continue
        read_sectors += int(fields[5])
        written_sectors += int(fields[9])
    return read_sectors * 512, written_sectors * 512


def parse_netdev(text: str, interfaces: Optional[Iterable[str]] = None) -> Tuple[int, int]:
    selected = set(interfaces or ())
    received = 0
    transmitted = 0
    for line in text.splitlines():
        if ":" not in line:
            continue
        name, values = line.split(":", 1)
        name = name.strip()
        if selected and name not in selected:
            continue
        if not selected and name == "lo":
            continue
        fields = values.split()
        if len(fields) >= 9:
            received += int(fields[0])
            transmitted += int(fields[8])
    return received, transmitted


def _read(path: str) -> str:
    return Path(path).read_text(encoding="ascii")


def snapshot(
    devices: Optional[Iterable[str]] = None,
    interfaces: Optional[Iterable[str]] = None,
) -> Dict[str, int]:
    total, idle, iowait = parse_cpu(_read("/proc/stat"))
    disk_read, disk_write = parse_diskstats(_read("/proc/diskstats"), devices)
    net_rx, net_tx = parse_netdev(_read("/proc/net/dev"), interfaces)
    return {
        "cpu_total": total,
        "cpu_idle": idle,
        "cpu_iowait": iowait,
        "disk_read": disk_read,
        "disk_write": disk_write,
        "net_rx": net_rx,
        "net_tx": net_tx,
    }


def calculate_rates(
    before: Mapping[str, int], after: Mapping[str, int], seconds: float
) -> Dict[str, float]:
    total_delta = max(after["cpu_total"] - before["cpu_total"], 1)
    idle_delta = max(after["cpu_idle"] - before["cpu_idle"], 0)
    iowait_delta = max(after["cpu_iowait"] - before["cpu_iowait"], 0)
    return {
        "cpu_pct": max(
            0.0,
            min(
                100.0,
                (total_delta - idle_delta - iowait_delta) / total_delta * 100,
            ),
        ),
        "iowait_pct": max(0.0, min(100.0, iowait_delta / total_delta * 100)),
        "disk_read_bps": max(0.0, after["disk_read"] - before["disk_read"]) / seconds,
        "disk_write_bps": max(0.0, after["disk_write"] - before["disk_write"]) / seconds,
        "net_rx_bps": max(0.0, after["net_rx"] - before["net_rx"]) / seconds,
        "net_tx_bps": max(0.0, after["net_tx"] - before["net_tx"]) / seconds,
    }


def collect(
    output: Path,
    host: str,
    role: str,
    duration: float,
    interval: float,
    devices: Optional[Iterable[str]] = None,
    interfaces: Optional[Iterable[str]] = None,
) -> None:
    if os.name != "posix" or not Path("/proc/stat").exists():
        raise RuntimeError("host metric collection requires Linux /proc")
    output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + duration
    previous = snapshot(devices, interfaces)
    previous_time = time.monotonic()
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        while time.monotonic() < deadline:
            time.sleep(min(interval, max(deadline - time.monotonic(), 0)))
            current_time = time.monotonic()
            current = snapshot(devices, interfaces)
            rates = calculate_rates(previous, current, max(current_time - previous_time, 0.001))
            writer.writerow(
                {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "host": host,
                    "role": role,
                    **{key: f"{value:.3f}" for key, value in rates.items()},
                }
            )
            handle.flush()
            previous = current
            previous_time = current_time


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--host", default=socket.gethostname())
    parser.add_argument(
        "--role",
        required=True,
        choices=("cn", "dn", "gtm", "client", "combined"),
        help="use combined only when all database roles share one physical host",
    )
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--devices", nargs="*")
    parser.add_argument("--interfaces", nargs="*")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.duration <= 0 or args.interval <= 0:
        raise SystemExit("duration and interval must be positive")
    collect(
        args.output,
        args.host,
        args.role,
        args.duration,
        args.interval,
        args.devices,
        args.interfaces,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
