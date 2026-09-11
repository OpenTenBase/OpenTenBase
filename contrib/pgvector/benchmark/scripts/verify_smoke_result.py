#!/usr/bin/env python3
"""Fail unless an automated Smoke produced the required P0/P1 artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def read_json(path: Path) -> dict:
    if not path.is_file():
        raise RuntimeError(f"missing artifact: {path}")
    return json.loads(path.read_text())


def verify(root: Path) -> None:
    summary = read_json(root / "raw/p0-summary.json")
    manifest = read_json(root / "raw/manifest.json")
    builds = read_json(root / "raw/builds.json")
    qualities = read_json(root / "raw/quality.json")
    structures = read_json(root / "raw/structures.json")
    if summary.get("status") not in ("complete", "complete_with_partial_data"):
        raise RuntimeError(f"unexpected Diagnosis status: {summary.get('status')}")
    if summary.get("successful_build_runs", 0) < 1:
        raise RuntimeError("Smoke did not complete a Build Run")
    if summary.get("valid_quality_runs", 0) < 1:
        raise RuntimeError("Smoke did not complete a Quality Run")
    if summary.get("valid_structure_runs", 0) < 1:
        raise RuntimeError("Smoke did not complete a Structure Run")
    if not manifest.get("pgvector"):
        raise RuntimeError("manifest did not observe the pgvector extension version")
    if not any(row.get("collection_status") == "success" for row in builds):
        raise RuntimeError("Build collection did not succeed")
    if not any(row.get("used_ivfflat") is True for row in qualities):
        raise RuntimeError("Quality Run did not use IVFFlat")
    if not any(row.get("status") == "success" for row in structures):
        raise RuntimeError("Structure collection did not succeed")
    report = root / "README.md"
    if not report.is_file() or "## Structure Summary" not in report.read_text():
        raise RuntimeError("human-readable report is missing Structure Summary")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    args = parser.parse_args()
    verify(args.result)
    print(f"Smoke verification passed: {args.result}")


if __name__ == "__main__":
    main()
