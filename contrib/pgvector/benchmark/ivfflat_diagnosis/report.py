"""P0/P1 human-readable reports derived only from collected facts."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Iterable

from common.run_store import RunStore


def _value(value: Any, suffix: str = "") -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, float):
        return f"{value:.3f}{suffix}"
    return f"{value}{suffix}"


def _mib(value: int | float | None) -> str:
    if value is None:
        return "unavailable"
    mib = value / 1024 / 1024
    return f"{mib:,.0f} MiB" if mib.is_integer() else f"{mib:,.2f} MiB"


def _integer(value: int | None) -> str:
    return "unavailable" if value is None else f"{value:,}"


def _decimal(value: int | float | None, digits: int, suffix: str = "") -> str:
    return "unavailable" if value is None else f"{value:.{digits}f}{suffix}"


def _setting_mib(value: Any) -> str:
    if value is None:
        return "unavailable"
    match = re.fullmatch(r"\s*([0-9.]+)\s*(B|kB|MB|GB|TB)?\s*", str(value), re.I)
    if not match:
        return str(value)
    number = float(match.group(1))
    unit = (match.group(2) or "B").lower()
    factors = {"b": 1 / 1024 / 1024, "kb": 1 / 1024,
               "mb": 1, "gb": 1024, "tb": 1024 * 1024}
    mib = number * factors[unit]
    return f"{mib:,.0f} MiB" if mib.is_integer() else f"{mib:,.2f} MiB"


def _summed_rss(build: dict[str, Any]) -> int | float | None:
    return build.get(
        "process_group_peak_summed_rss_bytes",
        build.get("process_group_peak_rss_bytes"),
    )


def _escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(headers: Iterable[str], rows: Iterable[Iterable[Any]]) -> str:
    headers = list(headers)
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    lines.extend("| " + " | ".join(_escape(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def _metric_value(summary: dict[str, Any], name: str) -> Any:
    metric = (summary.get("metrics") or {}).get(name) or {}
    return metric.get("value") if metric.get("availability") == "available" else None


def _distribution_value(summary: dict[str, Any], name: str) -> Any:
    metric = (summary.get("distribution") or {}).get(name) or {}
    return metric.get("value") if metric.get("availability") == "available" else None


def _structure_result_block(build: dict[str, Any], *, heading: bool) -> str:
    """Render collected P1 structure facts, or explain why they do not exist."""
    title = f"### lists={build.get('lists')}\n\n" if heading else ""
    summary = build.get("structure_summary") or {}
    structure_status = build.get("structure_status")
    if structure_status == "success" and summary.get("availability") == "available":
        exact = "Yes" if summary.get("scan_scope") == "full" else "No (sampled scan)"
        return f"""{title}```text
status                    success
scan scope                {summary.get('scan_scope', 'unavailable')}
sample rate               {_decimal(summary.get('sample_rate'), 3)}
scanned / declared lists  {_integer(summary.get('scanned_lists'))} / {_integer(summary.get('declared_lists'))}
exact aggregate metrics   {exact}
physical indexed tuples   {_integer(_metric_value(summary, 'physical_indexed_tuples'))}
empty lists / ratio       {_integer(_metric_value(summary, 'empty_lists'))} / {_decimal(_metric_value(summary, 'empty_ratio'), 3)}
list count min / mean     {_integer(_distribution_value(summary, 'min'))} / {_decimal(_distribution_value(summary, 'mean'), 2)}
list count P50 / P95      {_decimal(_distribution_value(summary, 'p50'), 2)} / {_decimal(_distribution_value(summary, 'p95'), 2)}
list count max            {_integer(_distribution_value(summary, 'max'))}
CV / Gini                 {_decimal(_metric_value(summary, 'cv'), 3)} / {_decimal(_metric_value(summary, 'gini'), 3)}
max / average             {_decimal(_metric_value(summary, 'max_over_avg'), 3)}
P95 / P50                 {_decimal(_metric_value(summary, 'p95_over_p50'), 3)}
top-N tuple share         {_decimal(_metric_value(summary, 'top_n_tuple_share'), 3)}
page fill ratio           {_decimal(_metric_value(summary, 'fill_ratio'), 3)}
coverage ratio            {_decimal(_metric_value(summary, 'coverage_ratio'), 3)}
index amplification       {_decimal(_metric_value(summary, 'amplification'), 3)}
```
"""

    reason = (
        summary.get("reason") or build.get("reason") or build.get("error")
        or "Structure collection did not run because CREATE INDEX did not succeed."
    )
    memory = build.get("memory_estimate") or {}
    memory_lines = ""
    if memory.get("estimated_pgvector_checked_memory_bytes") is not None:
        memory_lines = (
            "\nEstimated pgvector checked memory: "
            f"{_mib(memory.get('estimated_pgvector_checked_memory_bytes'))}  \n"
            "Available maintenance_work_mem: "
            f"{_mib(memory.get('memory_limit_bytes'))}"
        )
    return f"""{title}Status: NOT RUN

Reason: {reason}  
Build status: `{build.get('status')}`; database status: `{build.get('database_status')}`{memory_lines}
"""


def write_build_report(store: RunStore, prefix: Path, build: dict[str, Any],
                       preflight: dict[str, Any], memory: dict[str, Any],
                       aggregate: dict[str, Any] | None) -> None:
    phases = (aggregate or {}).get("phase_summaries", {})
    phase_table = _table(
        ("Phase", "Approx. time", "CPU", "Read", "Write", "Peak RSS", "Wait samples"),
        (
            (
                name,
                _value(data.get("wall_time_ms"), " ms"),
                _value(data.get("cpu_seconds"), " s"),
                _value(data.get("proc_read_bytes"), " B"),
                _value(data.get("proc_write_bytes"), " B"),
                _mib(data.get("peak_rss_bytes")),
                sum(data.get("wait_samples", {}).values()),
            )
            for name, data in phases.items()
        ),
    ) if phases else "_No reliable phase samples were available._"
    waits = sorted(
        (
            (name, wait, count)
            for name, data in phases.items()
            for wait, count in data.get("wait_samples", {}).items()
        ),
        key=lambda item: item[2],
        reverse=True,
    )
    wait_text = ", ".join(f"{phase}: {wait} × {count}" for phase, wait, count in waits[:5])
    workers = build.get("discovered_worker_pids") or []
    metadata = build.get("index_metadata", {})
    pg_delta = (aggregate or {}).get("build_run_pg_delta", {})
    pg_read = pg_delta.get("pg_read_bytes", {})
    pg_write = pg_delta.get("pg_write_bytes", {})
    wal = pg_delta.get("wal_bytes", {})
    conclusion = (
        "CREATE INDEX succeeded and core index metadata is available."
        if build.get("database_status") == "success"
        and metadata.get("availability") == "available"
        else build.get("error") or build.get("reason") or "Build did not complete successfully."
    )
    skip_reason = build.get("reason") or build.get("error")
    execution_note = (
        f"\n- Build/collection reason: {skip_reason}"
        if skip_reason else ""
    )
    structure_block = _structure_result_block(build, heading=False)
    report = f"""# Build Run

- Build ID: `{build.get('build_id')}`
- lists / actual rows / eligible vectors: `{build.get('lists')}` / `{build.get('actual_rows')}` / `{build.get('eligible_vectors')}`
- requested rows: `{build.get('requested_rows')}`
- status: `{build.get('status')}`; collection: `{build.get('collection_status')}`
- database status: `{build.get('database_status')}`{execution_note}
- CREATE INDEX time: `{_value(build.get('total_elapsed_ms'), ' ms')}`
- phase timing: sampling approximation, interval `{_value((aggregate or {}).get('sample_interval_seconds'), ' s')}`

## Phase breakdown

{phase_table}

## Resources and metadata

- Estimated pgvector checked memory: {_mib(memory.get('estimated_pgvector_checked_memory_bytes'))}
- maintenance_work_mem limit: {_mib(memory.get('memory_limit_bytes'))}
- Peak process-group PSS / summed RSS: {_mib(build.get('process_group_peak_pss_bytes'))} / {_mib(_summed_rss(build))}
- Leader PID: `{build.get('leader_pid')}`; worker PIDs: `{workers or 'none observed'}`
- Main wait/resource observations: {wait_text or 'no wait samples observed'}
- PG backend/build-run sampled read / write / WAL: `{_value(pg_read.get('value'), ' B')}` / `{_value(pg_write.get('value'), ' B')}` / `{_value(wal.get('value'), ' B')}`
- Index OID / relfilenode: `{metadata.get('index_oid')}` / `{metadata.get('relfilenode')}`
- Index size / pages: {_mib(metadata.get('index_bytes'))} / `{_value(metadata.get('index_pages'))}`
- Metadata availability: `{metadata.get('availability', 'unavailable')}`{': ' + metadata['reason'] if metadata.get('reason') else ''}

## Structure Summary

{structure_block}

## Conclusion

{conclusion}

Raw evidence is under [`raw/`](raw/).
"""
    store.write_text(prefix / "report.md", report)


def write_quality_report(store: RunStore, prefix: Path, quality: dict[str, Any]) -> None:
    report = f"""# Quality Run

- Quality Run ID: `{quality.get('quality_run_id')}`
- Build ID: `{quality.get('build_id')}`
- lists / probes / K: `{quality.get('lists')}` / `{quality.get('probes')}` / `{quality.get('k')}`
- Recall@K: `{_value(quality.get('recall_at_k'))}`
- Latency mean / P50 / P95 / P99 / stddev: `{_value(quality.get('mean_ms'), ' ms')}` / `{_value(quality.get('p50_ms'), ' ms')}` / `{_value(quality.get('p95_ms'), ' ms')}` / `{_value(quality.get('p99_ms'), ' ms')}` / `{_value(quality.get('stddev_ms'), ' ms')}`
- Single-client QPS: `{_value(quality.get('single_client_qps'))}`
- Natural planner used target IVFFlat index: `{_value(quality.get('used_ivfflat'))}`
- status: `{quality.get('status')}`
- error/reason: `{quality.get('error') or quality.get('invalid_reason') or quality.get('reason') or 'none'}`

Raw evidence and plans are under [`raw/`](raw/).
"""
    store.write_text(prefix / "report.md", report)


def _build_csv(build: dict[str, Any], report_path: Path) -> dict[str, Any]:
    metadata = build.get("index_metadata", {})
    return {
        "build_id": build.get("build_id"),
        "lists": build.get("lists"),
        "requested_rows": build.get("requested_rows"),
        "actual_rows": build.get("actual_rows"),
        "eligible_vectors": build.get("eligible_vectors"),
        "status": build.get("status"),
        "database_status": build.get("database_status"),
        "collection_status": build.get("collection_status"),
        "build_seconds": build.get("build_seconds"),
        "peak_summed_rss_bytes": _summed_rss(build),
        "peak_rss_bytes": _summed_rss(build),
        "peak_pss_bytes": build.get("process_group_peak_pss_bytes"),
        "index_oid": metadata.get("index_oid"),
        "relfilenode": metadata.get("relfilenode"),
        "index_bytes": metadata.get("index_bytes"),
        "index_pages": metadata.get("index_pages"),
        "report": report_path.as_posix(),
    }


def _quality_csv(quality: dict[str, Any], report_path: Path) -> dict[str, Any]:
    return {
        "quality_run_id": quality.get("quality_run_id"),
        "build_id": quality.get("build_id"),
        "lists": quality.get("lists"),
        "probes": quality.get("probes"),
        "k": quality.get("k"),
        "status": quality.get("status"),
        "recall_at_k": quality.get("recall_at_k"),
        "mean_ms": quality.get("mean_ms"),
        "p50_ms": quality.get("p50_ms"),
        "p95_ms": quality.get("p95_ms"),
        "p99_ms": quality.get("p99_ms"),
        "stddev_ms": quality.get("stddev_ms"),
        "single_client_qps": quality.get("single_client_qps"),
        "used_ivfflat": quality.get("used_ivfflat"),
        "report": report_path.as_posix(),
    }


def _phase_breakdown(build: dict[str, Any]) -> str:
    phases = build.get("phase_summaries") or {}
    if not phases:
        return "unavailable"
    return _table(
        ("phase", "approx. time", "CPU", "peak RSS", "wait samples"),
        (
            (
                name,
                _decimal(values.get("wall_time_ms"), 2, " ms"),
                _decimal(values.get("cpu_seconds"), 2, " s"),
                _mib(values.get("peak_rss_bytes")),
                sum(values.get("wait_samples", {}).values()),
            )
            for name, values in phases.items()
        ),
    )


def _build_result_block(build: dict[str, Any], *, heading: bool) -> str:
    metadata = build.get("index_metadata", {})
    title = f"### lists={build.get('lists')}\n\n" if heading else ""
    reason = build.get("reason") or build.get("error")
    reason_text = f"\nReason: {reason}\n" if reason else ""
    memory = build.get("memory_estimate") or {}
    memory_text = ""
    if build.get("status") == "skipped":
        memory_text = (
            "\nPreflight memory estimate: "
            f"{_mib(memory.get('estimated_pgvector_checked_memory_bytes'))}; "
            "maintenance_work_mem limit: "
            f"{_mib(memory.get('memory_limit_bytes'))}.\n"
        )
    return f"""{title}```text
lists        {build.get('lists')}
rows         {_integer(build.get('actual_rows'))}
status       {build.get('status')}
collection   {build.get('collection_status', 'unavailable')}
build time   {_decimal(build.get('build_seconds'), 2, ' s')}
summed RSS   {_mib(_summed_rss(build))}
index size   {_mib(metadata.get('index_bytes'))}
```
{reason_text}{memory_text}

Phase breakdown:

{_phase_breakdown(build)}

Source validation describes supported phases; runtime observation describes only
phases actually sampled in this Build Run. See raw/build.json for observation status.
"""


def _quality_result_block(quality: dict[str, Any], *, heading: bool) -> str:
    title = (
        f"### lists={quality.get('lists')}, probes={quality.get('probes')}\n\n"
        if heading else ""
    )
    used = (
        "Yes" if quality.get("used_ivfflat") is True
        else "No" if quality.get("used_ivfflat") is False else "unavailable"
    )
    return f"""{title}```text
lists               {quality.get('lists')}
probes              {quality.get('probes')}
Recall@{quality.get('k')}           {_decimal(quality.get('recall_at_k'), 3)}
P95                 {_decimal(quality.get('p95_ms'), 2, ' ms')}
Single-client QPS   {_decimal(quality.get('single_client_qps'), 2)}
IVFFlat used        {used}
```
"""


def _unavailable_metrics(manifest: dict[str, Any]) -> str:
    capabilities = manifest.get("capabilities", {}).get("capabilities", {})
    preferred = (
        ("pss", "PSS", "smaps_rollup unavailable"),
        ("io_timing", "IO timing", "track_io_timing=off"),
        ("statement_stats", "Statement statistics", "pg_stat_statements unavailable"),
        ("assign_progress", "Assign progress", "target-version semantics not validated"),
    )
    sections = [
        f"{title}  \n{description}"
        for name, title, description in preferred
        if name in capabilities and not capabilities[name].get("available")
    ]
    preferred_names = {item[0] for item in preferred}
    sections.extend(
        f"{name.replace('_', ' ').title()}\n{details.get('reason') or 'unavailable'}"
        for name, details in capabilities.items()
        if name not in preferred_names and not details.get("available")
    )
    return "\n\n".join(sections) if sections else "None."


def _p1_human_sections(
    p1: dict[str, Any] | None, pareto: dict[str, Any] | None,
    recommendations: list[dict[str, Any]] | None,
    validations: list[dict[str, Any]] | None,
) -> str:
    if not p1:
        return "## Joint Diagnosis\n\nP1 joint diagnosis was not executed."
    findings = p1.get("findings") or []
    triggered = [item for item in findings if item.get("status") == "triggered"]
    unknown = [item for item in findings if item.get("status") == "unknown"]
    finding_table = _table(
        ("rule", "status", "confidence", "finding"),
        (
            (item.get("rule_id"), item.get("status"), item.get("confidence"), item.get("summary"))
            for item in findings if item.get("status") in ("triggered", "unknown")
        ),
    ) if triggered or unknown else "No triggered or unknown findings."
    pareto_rows = (pareto or {}).get("candidates") or []
    pareto_table = _table(
        ("lists", "probes", "feasibility", "Pareto", "Recall", "P95"),
        (
            (
                item.get("lists"), item.get("probes"),
                (item.get("feasibility") or {}).get("status"),
                item.get("pareto_status"),
                _decimal((item.get("values") or {}).get("recall_at_k"), 3),
                _decimal((item.get("values") or {}).get("p95_ms"), 2, " ms"),
            )
            for item in pareto_rows
        ),
    ) if pareto_rows else "No comparable candidates."
    recommendation_rows = recommendations or []
    recommendation_table = _table(
        ("id", "status", "action", "current", "recommended"),
        (
            (
                item.get("recommendation_id"), item.get("status"), item.get("action"),
                item.get("current"), item.get("recommended"),
            )
            for item in recommendation_rows
        ),
    ) if recommendation_rows else (
        "No evidence-backed parameter recommendation was produced. "
        "Structure findings alone never imply a lists change."
    )
    validation_rows = validations or []
    validation_table = _table(
        ("recommendation", "status", "before", "after", "reason"),
        (
            (
                item.get("recommendation_id"), item.get("status"),
                item.get("before_candidate_id"), item.get("after_candidate_id"),
                "; ".join(item.get("reasons") or []) or "none",
            )
            for item in validation_rows
        ),
    ) if validation_rows else (
        "Not run. Recommendations remain `proposed`; use "
        "`--validate-recommendations` to request an explicit same-workload rerun."
    )
    return f"""## Joint Diagnosis

Ruleset: `{p1.get('ruleset_version')}`  
Triggered findings: {len(triggered)}; unknown findings: {len(unknown)}

{finding_table}

## Candidate Feasibility and Pareto

{pareto_table}

Pareto dimensions used: `{', '.join((pareto or {}).get('dimensions', {})) or 'none'}`.  
No globally best configuration is selected by Pareto alone.

## Recommendations

{recommendation_table}

## Validation

{validation_table}
"""


def write_root_reports(store: RunStore, manifest: dict[str, Any], summary: dict[str, Any],
                       builds: list[tuple[dict[str, Any], Path]],
                       qualities: list[tuple[dict[str, Any], Path]],
                       p1: dict[str, Any] | None = None,
                       pareto: dict[str, Any] | None = None,
                       recommendations: list[dict[str, Any]] | None = None,
                       validations: list[dict[str, Any]] | None = None) -> None:
    build_csv = [_build_csv(item, path / "report.md") for item, path in builds]
    quality_csv = [_quality_csv(item, path / "report.md") for item, path in qualities]
    store.write_csv(
        "build-summary.csv", build_csv,
        fieldnames=(
            "build_id", "lists", "requested_rows", "actual_rows",
            "eligible_vectors", "status", "database_status",
            "collection_status", "build_seconds", "peak_summed_rss_bytes",
            "peak_rss_bytes",
            "peak_pss_bytes", "index_oid", "relfilenode", "index_bytes",
            "index_pages", "report",
        ),
    )
    store.write_csv(
        "quality-summary.csv", quality_csv,
        fieldnames=(
            "quality_run_id", "build_id", "lists", "probes", "k",
            "status", "recall_at_k", "mean_ms", "p50_ms", "p95_ms",
            "p99_ms", "stddev_ms", "single_client_qps", "used_ivfflat",
            "report",
        ),
    )

    settings = manifest.get("postgresql_settings", {})
    successful_builds = [
        item for item, _ in builds if item.get("database_status") == "success"
    ]
    valid_qualities = [
        item for item, _ in qualities if item.get("status") == "success"
    ]
    actual_rows = sorted({
        item.get("actual_rows") for item, _ in builds
        if item.get("actual_rows") is not None
    })
    configured_lists = sorted({
        item.get("lists") for item, _ in builds if item.get("lists") is not None
    })
    configured_probes = sorted({
        item.get("probes") for item, _ in qualities if item.get("probes") is not None
    }) or sorted(set(manifest.get("probes", [])))
    rows_text = ", ".join(_integer(value) for value in actual_rows)
    lists_text = ", ".join(str(value) for value in configured_lists) or "unavailable"
    probes_text = ", ".join(str(value) for value in configured_probes) or "unavailable"
    memory_text = _setting_mib(settings.get("maintenance_work_mem"))
    result_status = str(summary.get("status", "unavailable")).replace("_", " ").upper()

    representative_build = successful_builds[0] if len(successful_builds) == 1 else None
    representative_quality = valid_qualities[0] if len(valid_qualities) == 1 else None
    if representative_build:
        finding_1 = (
            "IVFFlat index build succeeded in "
            f"{_decimal(representative_build.get('build_seconds'), 2, 's')}."
        )
    else:
        finding_1 = (
            f"{len(successful_builds)} of {len(builds)} IVFFlat index builds succeeded."
        )

    memory_estimates = [
        item.get("memory_estimate") for item, _ in builds
        if (item.get("memory_estimate") or {}).get("availability") == "available"
    ]
    if len(memory_estimates) == 1:
        estimate = memory_estimates[0]
        preflight_state = (
            "passed" if estimate.get("predicted_memory_failure") is False
            else "predicted a hard memory failure"
            if estimate.get("predicted_memory_failure") is True else "was inconclusive"
        )
        finding_2 = (
            f"Build-memory preflight {preflight_state}.  \n"
            f"   Estimated pgvector checked memory: "
            f"{_mib(estimate.get('estimated_pgvector_checked_memory_bytes'))}  \n"
            f"   Available maintenance_work_mem: "
            f"{_mib(estimate.get('memory_limit_bytes'))}."
        )
    else:
        finding_2 = (
            f"Build-memory preflight produced {len(memory_estimates)} "
            "configuration-specific estimates."
        )

    peaks = [
        _summed_rss(item) for item in successful_builds
        if _summed_rss(item) is not None
    ]
    peak_text = _mib(max(peaks)) if peaks else "unavailable"
    finding_3 = (
        f"Peak summed process-group RSS was {peak_text}.  \n"
        "   This sums per-process RSS, may double-count shared pages, and is not  \n"
        "   directly limited by  \n"
        "   maintenance_work_mem."
    )
    if representative_quality:
        finding_4 = (
            f"probes={representative_quality.get('probes')} achieved:  \n"
            f"   Recall@{representative_quality.get('k')} = "
            f"{_decimal(representative_quality.get('recall_at_k'), 3)}  \n"
            f"   P95 = {_decimal(representative_quality.get('p95_ms'), 2, ' ms')}  \n"
            f"   Single-client QPS = "
            f"{_decimal(representative_quality.get('single_client_qps'), 2)}."
        )
    else:
        finding_4 = (
            f"{len(valid_qualities)} of {len(qualities)} Quality Runs were valid."
        )

    tested_pairs = {
        (item.get("lists"), item.get("probes")) for item, _ in qualities
    }
    if len(tested_pairs) == 1:
        finding_5 = (
            "Only one lists/probes configuration was tested in this Diagnosis Run.  \n"
            '   No parameter ranking or "best configuration" conclusion can be made.'
        )
    else:
        finding_5 = (
            f"{len(tested_pairs)} lists/probes configurations were tested.\n"
            "   Conclusions apply only to these tested configurations; no global "
            "optimum is claimed."
        )

    build_sections = "\n\n".join(
        _build_result_block(item, heading=len(builds) > 1)
        for item, _ in builds
    ) or "No Build Runs were recorded."
    quality_sections = "\n\n".join(
        _quality_result_block(item, heading=len(qualities) > 1)
        for item, _ in qualities
    ) or "No Quality Runs were recorded."
    structure_sections = "\n\n".join(
        _structure_result_block(item, heading=len(builds) > 1)
        for item, _ in builds
    ) or "No Structure Runs were recorded because no Build Runs were recorded."
    build_links = (
        f"Build report: [{builds[0][1] / 'report.md'}]({builds[0][1] / 'report.md'})"
        if len(builds) == 1 else "\n".join(
            f"Build report (lists={item.get('lists')}): "
            f"[{path / 'report.md'}]({path / 'report.md'})"
            for item, path in builds
        ) or "Build report: unavailable"
    )
    quality_links = (
        f"Quality report: [{qualities[0][1] / 'report.md'}]({qualities[0][1] / 'report.md'})"
        if len(qualities) == 1 else "\n".join(
            f"Quality report (lists={item.get('lists')}, probes={item.get('probes')}): "
            f"[{path / 'report.md'}]({path / 'report.md'})"
            for item, path in qualities
        ) or "Quality report: unavailable"
    )
    p1_sections = _p1_human_sections(p1, pareto, recommendations, validations)

    readme = f"""# IVFFlat Build Diagnosis

## Result

Status: {result_status}

Dataset: {str(manifest.get('dataset', 'unavailable')).upper()}  
Metric: {str(manifest.get('metric', 'unavailable')).upper()}  
Rows: {rows_text or 'unavailable'}  
Dimension: {manifest.get('dimension', 'unavailable')}

Configuration:  
lists = {lists_text}  
probes = {probes_text}  
maintenance_work_mem = {memory_text}

## Main Findings

1. {finding_1}

2. {finding_2}

3. {finding_3}

4. {finding_4}

5. {finding_5}

## Build Result

{build_sections}

## Quality Result

{quality_sections}

## Structure Summary

{structure_sections}

{p1_sections}

## Unavailable Metrics

{_unavailable_metrics(manifest)}

## Scope

This result represents:  
{str(manifest.get('dataset', 'unavailable')).upper()} / {str(manifest.get('metric', 'unavailable')).upper()} / warm-cache / single-client workload.

It must not be interpreted as production concurrency capacity
or a globally optimal IVFFlat configuration.

## Details

{build_links}

{quality_links}

Raw evidence: [raw/](raw/)
"""
    ledger = f"""# Run status ledger

- Diagnosis ID: `{summary.get('diagnosis_id')}`
- Status: `{summary.get('status')}`
- Build runs: `{summary.get('build_runs')}`; successful: `{summary.get('successful_build_runs')}`; failed: `{summary.get('failed_build_runs')}`; skipped: `{summary.get('skipped_build_runs')}`
- Quality runs: `{summary.get('quality_runs')}`; valid: `{summary.get('valid_quality_runs')}`

The complete machine summary is [`raw/p0-summary.json`](raw/p0-summary.json).
"""
    store.write_text("README.md", readme)
    store.write_text("summary.md", ledger)
    if p1:
        diagnosis_report = f"""# P1 Joint Diagnosis Report

Diagnosis ID: `{summary.get('diagnosis_id')}`  
Dataset / metric: `{manifest.get('dataset')}` / `{manifest.get('metric')}`

{p1_sections}

## Evidence and machine results

- [diagnosis.json](diagnosis.json)
- [candidates.json](candidates.json)
- [evidence.json](evidence.json)
- [findings.json](findings.json)
- [pareto.json](pareto.json)
- [recommendations.json](recommendations.json)
- [validations/](validations/)
"""
        store.write_text("report.md", diagnosis_report)
