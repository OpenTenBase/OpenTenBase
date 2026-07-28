#!/usr/bin/env python3
# Copyright (c) 2026 OpenTenBase Authors
# Licensed under the BSD 3-Clause License.
"""Analyze OpenTenBase pgbench results without third-party dependencies."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence


SUMMARY_FIELDS = (
    "workload",
    "clients",
    "jobs",
    "duration_s",
    "transactions",
    "failed",
    "latency_avg_ms",
    "latency_stddev_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "latency_sample_count",
    "tps",
    "exit_code",
    "raw_log",
)


def _last_float(pattern: str, text: str) -> Optional[float]:
    values = re.findall(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    if not values:
        return None
    return float(values[-1].replace(",", ""))


def _last_int(pattern: str, text: str) -> Optional[int]:
    value = _last_float(pattern, text)
    return None if value is None else int(value)


def parse_pgbench_output(text: str) -> Dict[str, Optional[float]]:
    """Parse both PostgreSQL 10-era and recent pgbench summaries.

    If pgbench prints TPS both including and excluding connection setup, the last
    value is used. This matches the steady-state throughput used for comparisons.
    """

    processed = _last_int(
        r"number of transactions actually processed:\s*([\d,]+)", text
    )
    failed = _last_int(r"number of failed transactions:\s*([\d,]+)", text)
    latency = _last_float(r"latency average\s*=\s*([\d,.]+)\s*ms", text)
    latency_stddev = _last_float(r"latency stddev\s*=\s*([\d,.]+)\s*ms", text)
    tps = _last_float(r"^tps\s*=\s*([\d,.]+)", text)
    connection_ms = _last_float(
        r"initial connection time\s*=\s*([\d,.]+)\s*ms", text
    )
    return {
        "transactions": processed,
        "failed": 0 if failed is None else failed,
        "latency_avg_ms": latency,
        "latency_stddev_ms": latency_stddev,
        "tps": tps,
        "initial_connection_ms": connection_ms,
    }


def read_summary(path: Path) -> List[Dict[str, object]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows: List[Dict[str, object]] = []
        for raw in csv.DictReader(handle):
            row: Dict[str, object] = dict(raw)
            for field in (
                "clients",
                "jobs",
                "duration_s",
                "transactions",
                "failed",
                "latency_sample_count",
                "exit_code",
            ):
                row[field] = int(raw.get(field) or 0)
            for field in (
                "latency_avg_ms",
                "latency_stddev_ms",
                "latency_p50_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "tps",
            ):
                row[field] = float(raw.get(field) or 0)
            rows.append(row)
        return rows


def add_scaling_metrics(rows: Sequence[Mapping[str, object]]) -> List[Dict[str, object]]:
    """Add scaling efficiency and a conservative saturation marker."""

    by_workload: MutableMapping[str, List[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        by_workload[str(row["workload"])].append(row)

    enriched: List[Dict[str, object]] = []
    for workload in sorted(by_workload):
        group = sorted(by_workload[workload], key=lambda item: int(item["clients"]))
        base = next((row for row in group if float(row["tps"]) > 0), group[0])
        base_per_client = float(base["tps"]) / max(int(base["clients"]), 1)
        previous: Optional[Mapping[str, object]] = None
        for source in group:
            row = dict(source)
            per_client = float(row["tps"]) / max(int(row["clients"]), 1)
            row["scaling_efficiency"] = (
                per_client / base_per_client if base_per_client else 0.0
            )
            saturated = False
            if previous is not None and float(previous["tps"]) > 0:
                throughput_gain = float(row["tps"]) / float(previous["tps"]) - 1.0
                latency_ratio = float(row["latency_avg_ms"]) / max(
                    float(previous["latency_avg_ms"]), 0.000001
                )
                saturated = throughput_gain < 0.05 or (
                    throughput_gain < 0.20 and latency_ratio >= 2.0
                )
            row["saturated"] = saturated
            enriched.append(row)
            previous = source
    return enriched


def read_distribution(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    counts: MutableMapping[str, List[int]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            counts[row["table_name"]].append(int(row["row_count"]))
    result: Dict[str, float] = {}
    for table, values in counts.items():
        mean = statistics.fmean(values) if values else 0
        result[table] = max(values) / mean if mean else math.inf
    return result


def read_host_metrics(directory: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not directory.exists():
        return rows
    for path in sorted(directory.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for raw in csv.DictReader(handle):
                row: Dict[str, object] = dict(raw)
                for field in (
                    "cpu_pct",
                    "iowait_pct",
                    "disk_read_bps",
                    "disk_write_bps",
                    "net_rx_bps",
                    "net_tx_bps",
                ):
                    row[field] = float(raw.get(field) or 0)
                rows.append(row)
    return rows


def summarize_hosts(rows: Sequence[Mapping[str, object]]) -> Dict[str, Dict[str, float]]:
    by_role: MutableMapping[str, List[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        by_role[str(row.get("role", "unknown"))].append(row)
    summary: Dict[str, Dict[str, float]] = {}
    for role, group in by_role.items():
        summary[role] = {}
        for field in (
            "cpu_pct",
            "iowait_pct",
            "disk_read_bps",
            "disk_write_bps",
            "net_rx_bps",
            "net_tx_bps",
        ):
            values = [float(row[field]) for row in group]
            summary[role][f"avg_{field}"] = statistics.fmean(values)
            summary[role][f"max_{field}"] = max(values)
    return summary


def build_hypotheses(
    rows: Sequence[Mapping[str, object]],
    host_summary: Mapping[str, Mapping[str, float]],
    skew: Mapping[str, float],
    network_mbps: Optional[float],
) -> List[str]:
    hypotheses: List[str] = []
    failed = sum(int(row["failed"]) for row in rows)
    processed = sum(int(row["transactions"]) for row in rows)
    if failed:
        rate = failed / max(failed + processed, 1) * 100
        hypotheses.append(
            f"连接、锁或 SQL 错误需要优先排查：记录到 {failed} 个失败事务"
            f"（约 {rate:.3f}%）。"
        )

    skewed = {name: ratio for name, ratio in skew.items() if ratio >= 1.20}
    if skewed:
        detail = "、".join(f"`{name}`={ratio:.2f}" for name, ratio in skewed.items())
        hypotheses.append(
            f"数据倾斜可能放大最忙 DN 的开销：max/mean 行数比为 {detail}。"
        )

    if any(bool(row.get("saturated")) for row in rows):
        cn_cpu = host_summary.get("cn", {}).get("avg_cpu_pct", 0)
        dn_cpu = host_summary.get("dn", {}).get("avg_cpu_pct", 0)
        all_iowait = max(
            (metrics.get("max_iowait_pct", 0) for metrics in host_summary.values()),
            default=0,
        )
        if cn_cpu >= 85 and dn_cpu < 70:
            hypotheses.append(
                f"CN 可能先饱和：CN 平均 CPU {cn_cpu:.1f}%，"
                f"DN 平均 CPU {dn_cpu:.1f}%，且吞吐已出现拐点。"
            )
        if dn_cpu >= 85:
            hypotheses.append(
                f"DN 计算可能是限制：DN 平均 CPU {dn_cpu:.1f}%，且吞吐已出现拐点。"
            )
        if all_iowait >= 10:
            hypotheses.append(
                f"磁盘等待可能影响吞吐：采样中的最高 iowait 为 {all_iowait:.1f}%。"
            )
        if network_mbps:
            limit_bps = network_mbps * 1_000_000 / 8
            maximum = max(
                (
                    max(
                        metrics.get("max_net_rx_bps", 0),
                        metrics.get("max_net_tx_bps", 0),
                    )
                    for metrics in host_summary.values()
                ),
                default=0,
            )
            utilization = maximum / limit_bps
            if utilization >= 0.70:
                hypotheses.append(
                    f"网络可能接近容量上限：单向峰值约占配置带宽的"
                    f" {utilization * 100:.1f}%。"
                )
        if not hypotheses:
            hypotheses.append(
                "吞吐已出现拐点，但现有资源证据不足以定位；下一步应检查执行计划、"
                "等待事件、连接池和锁。"
            )

    if not hypotheses:
        hypotheses.append(
            "当前样本没有触发自动阈值；这不等于没有瓶颈，应结合执行计划和更高并发复跑。"
        )
    return hypotheses


def _format_number(value: object, digits: int = 2) -> str:
    return f"{float(value):.{digits}f}"


def generate_report(
    rows: Sequence[Mapping[str, object]],
    environment: Mapping[str, object],
    host_summary: Mapping[str, Mapping[str, float]],
    skew: Mapping[str, float],
) -> str:
    network_mbps = (
        environment.get("cluster", {}).get("network_mbps")
        if isinstance(environment.get("cluster"), dict)
        else None
    )
    hypotheses = build_hypotheses(rows, host_summary, skew, network_mbps)
    lines = [
        "# OpenTenBase 基准性能测试报告",
        "",
        "> 本报告由原始 pgbench、数据库和主机指标自动生成。自动判断是待验证假设，"
        "不是已证明的因果结论。",
        "",
        "## 运行环境",
        "",
        "```json",
        json.dumps(environment, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## 性能结果",
        "",
        "| workload | clients | TPS/QPS | 平均延迟(ms) | P95(ms) | P99(ms) | 失败 | 扩展效率 | 饱和信号 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in sorted(rows, key=lambda item: (str(item["workload"]), int(item["clients"]))):
        lines.append(
            "| {workload} | {clients} | {tps} | {latency} | {p95} | {p99} | {failed} | {efficiency} | {saturated} |".format(
                workload=row["workload"],
                clients=row["clients"],
                tps=_format_number(row["tps"]),
                latency=_format_number(row["latency_avg_ms"]),
                p95=_format_number(row["latency_p95_ms"]),
                p99=_format_number(row["latency_p99_ms"]),
                failed=row["failed"],
                efficiency=f"{float(row.get('scaling_efficiency', 0)) * 100:.1f}%",
                saturated="是" if row.get("saturated") else "否",
            )
        )

    lines.extend(["", "## 数据分布", ""])
    if skew:
        lines.extend(
            [
                "| 表 | max/mean 行数比 | 判断 |",
                "| --- | ---: | --- |",
            ]
        )
        for table, ratio in sorted(skew.items()):
            lines.append(
                f"| `{table}` | {ratio:.3f} | "
                f"{'需排查倾斜' if ratio >= 1.20 else '均衡'} |"
            )
    else:
        lines.append("未找到 `distribution.csv`，无法判断 DN 数据倾斜。")

    lines.extend(["", "## 主机资源摘要", ""])
    if host_summary:
        lines.extend(
            [
                "| 角色 | 平均 CPU | 最高 iowait | 峰值读 | 峰值写 | 峰值收 | 峰值发 |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for role, metrics in sorted(host_summary.items()):
            lines.append(
                f"| {role} | {metrics['avg_cpu_pct']:.1f}% | "
                f"{metrics['max_iowait_pct']:.1f}% | "
                f"{metrics['max_disk_read_bps'] / 1024 / 1024:.1f} MiB/s | "
                f"{metrics['max_disk_write_bps'] / 1024 / 1024:.1f} MiB/s | "
                f"{metrics['max_net_rx_bps'] / 1024 / 1024:.1f} MiB/s | "
                f"{metrics['max_net_tx_bps'] / 1024 / 1024:.1f} MiB/s |"
            )
    else:
        lines.append("未找到 `host_metrics/*.csv`；CN/DN/网络/磁盘判断只能保留为待验证项。")

    lines.extend(["", "## 瓶颈假设与下一步", ""])
    lines.extend(f"- {item}" for item in hypotheses)
    lines.extend(
        [
            "",
            "## 可复核证据",
            "",
            "- `summary.csv`：结构化请求层结果。",
            "- `raw/*.txt`：pgbench 原始输出、预检和执行计划。",
            "- `distribution.csv`：按 `xc_node_id` 的行数。",
            "- `host_metrics/*.csv`：CN、DN、GTM 主机资源采样。",
            "",
        ]
    )
    return "\n".join(lines)


def analyze(input_dir: Path, output: Path) -> None:
    summary_path = input_dir / "summary.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"missing benchmark summary: {summary_path}")
    rows = add_scaling_metrics(read_summary(summary_path))
    environment_path = input_dir / "environment.json"
    environment = (
        json.loads(environment_path.read_text(encoding="utf-8"))
        if environment_path.exists()
        else {"warning": "environment.json missing"}
    )
    skew = read_distribution(input_dir / "distribution.csv")
    hosts = summarize_hosts(read_host_metrics(input_dir / "host_metrics"))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(generate_report(rows, environment, hosts, skew), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    output = args.output or args.input_dir / "report.md"
    analyze(args.input_dir, output)
    print(f"report written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
