#!/usr/bin/env python3
"""Offline checks for the Issue #201 Stage 4B design artifacts."""

from __future__ import annotations

import re
import sys
from pathlib import Path


POC = Path(__file__).resolve().parents[1]
FILES = {
    "readme": POC / "README.md",
    "crd": POC / "crd" / "opentenbasecluster-crd.yaml",
    "sample": POC / "samples" / "minimal-cluster.yaml",
    "status": POC / "samples" / "partial-status-example.yaml",
    "plan": POC / "pseudostructure" / "generated-resources.yaml",
    "results": POC / "validation" / "validation-results.md",
    "trace": POC / "traceability" / "field-evidence-matrix.md",
}
PHASES = [
    "Pending",
    "Validating",
    "ProvisioningGTM",
    "WaitingForGTM",
    "ProvisioningMasters",
    "RegisteringTopology",
    "RestartingMasters",
    "ProvisioningStandbys",
    "CreatingGroups",
    "Ready",
    "Degraded",
    "Failed",
]
FORBIDDEN_KINDS = {"Pod", "StatefulSet", "Deployment", "Job", "PersistentVolumeClaim"}
WARNING = "这些材料不会部署 OpenTenBase，也没有实现 Operator。"
PLACEHOLDER = "<OPEN_TENBASE_IMAGE_NOT_SELECTED>"


class Checks:
    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[str] = []
        self.warnings: list[str] = []

    def check(self, condition: bool, label: str) -> None:
        (self.passed if condition else self.failed).append(label)


def nested(value, *keys):
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def find_version(crd):
    versions = nested(crd, "spec", "versions") or []
    return next((item for item in versions if item.get("name") == "v1alpha1"), None)


def parsed_checks(checks: Checks, docs: dict[str, object]) -> None:
    crd = docs["crd"]
    sample = docs["sample"]
    status_example = docs["status"]
    plan = docs["plan"]
    version = find_version(crd)

    checks.check(crd.get("apiVersion") == "apiextensions.k8s.io/v1", "CRD apiVersion")
    checks.check(crd.get("kind") == "CustomResourceDefinition", "CRD kind")
    checks.check(nested(crd, "spec", "group") == "database.opentenbase.org", "CRD group")
    checks.check(nested(crd, "spec", "scope") == "Namespaced", "CRD scope")
    checks.check(nested(crd, "spec", "names", "kind") == "OpenTenBaseCluster", "CRD resource kind")
    checks.check(nested(crd, "spec", "names", "plural") == "opentenbaseclusters", "CRD plural")
    checks.check(nested(crd, "spec", "names", "singular") == "opentenbasecluster", "CRD singular")
    checks.check("otb" in (nested(crd, "spec", "names", "shortNames") or []), "CRD short name")
    checks.check(bool(version), "CRD v1alpha1 version")
    if version:
        checks.check(version.get("served") is True and version.get("storage") is True, "v1alpha1 served and storage")
        checks.check(isinstance(version.get("subresources", {}).get("status"), dict), "status subresource")
        schema = nested(version, "schema", "openAPIV3Schema") or {}
        checks.check(schema.get("type") == "object", "explicitly typed root schema")
        checks.check(nested(schema, "properties", "spec", "type") == "object", "explicitly typed spec object")
        checks.check(nested(schema, "properties", "status", "type") == "object", "explicitly typed status object")
        checks.check(nested(schema, "properties", "spec", "properties", "status") is None, "status is not under spec")
        phase_enum = nested(schema, "properties", "status", "properties", "phase", "enum") or []
        checks.check(phase_enum == PHASES, "complete ordered phase enum")
        schema_text = str(schema)
        checks.check("observedGeneration" in schema_text, "Kubernetes observed generation field")
        checks.check("desiredTopologyGeneration" in schema_text and "observedTopologyGeneration" in schema_text, "distinct topology generation fields")

    checks.check(sample.get("apiVersion") == "database.opentenbase.org/v1alpha1", "sample apiVersion")
    checks.check(sample.get("kind") == "OpenTenBaseCluster", "sample kind")
    spec = sample.get("spec", {})
    shards = nested(spec, "dataNodes", "shards") or []
    shard_ids = [item.get("id") for item in shards if isinstance(item, dict)]
    checks.check(len(shard_ids) == len(set(shard_ids)), "unique sample shard IDs")
    checks.check(len(shards) == 2, "sample has two DataNode shards")
    checks.check(nested(spec, "gtm", "primaryCount") == 1, "sample has one GTM primary")
    checks.check(nested(spec, "gtm", "standbyCount") == 1, "sample has one GTM standby")
    coordinator_primaries = nested(spec, "coordinators", "primaryCount")
    coordinator_standbys = nested(spec, "coordinators", "standbyCountPerPrimary")
    checks.check(coordinator_primaries == 2, "sample has two Coordinator primaries")
    checks.check(coordinator_standbys == 1, "sample has one standby per Coordinator primary")
    expected_coordinators = coordinator_primaries * (1 + coordinator_standbys)
    coordinator_readiness = next(
        (item for item in nested(status_example, "status", "roleReadiness") or [] if item.get("role") == "Coordinator"),
        {},
    )
    checks.check(coordinator_readiness.get("desired") == expected_coordinators, "partial status Coordinator desired cardinality")
    checks.check(all(item.get("standbyCount") == 1 for item in shards), "one standby per DataNode shard")
    checks.check(spec.get("image") == PLACEHOLDER, "sample uses explicit non-runnable image placeholder")

    plan_objects = nested(plan, "spec", "objects") or []
    logical_kinds = {item.get("logicalKind") for item in plan_objects if isinstance(item, dict)}
    checks.check(not (logical_kinds & FORBIDDEN_KINDS), "no forbidden runnable logical kinds")
    checks.check(nested(plan, "spec", "workloadPrimitive") == "Unselected", "workload primitive is unselected")


def fallback_checks(checks: Checks, texts: dict[str, str]) -> None:
    """Text checks are deliberately not described as full YAML validation."""
    crd = texts["crd"]
    sample = texts["sample"]
    status_example = texts["status"]
    plan = texts["plan"]
    checks.check("apiVersion: apiextensions.k8s.io/v1" in crd, "fallback CRD apiVersion")
    checks.check("kind: CustomResourceDefinition" in crd, "fallback CRD kind")
    checks.check("group: database.opentenbase.org" in crd, "fallback CRD group")
    checks.check("kind: OpenTenBaseCluster" in crd, "fallback CRD resource kind")
    checks.check("- name: v1alpha1" in crd, "fallback CRD v1alpha1 version")
    checks.check("subresources:" in crd and "status: {}" in crd, "fallback status subresource")
    checks.check(all(phase in crd for phase in PHASES), "fallback complete phase vocabulary")
    checks.check("apiVersion: database.opentenbase.org/v1alpha1" in sample, "fallback sample apiVersion")
    checks.check("kind: OpenTenBaseCluster" in sample, "fallback sample kind")
    shard_ids = re.findall(r"^\s+- id:\s+(\S+)\s*$", sample, re.MULTILINE)
    checks.check(len(shard_ids) == 2 and len(shard_ids) == len(set(shard_ids)), "fallback two unique shard IDs")
    checks.check("primaryCount: 2" in sample, "fallback Coordinator primary count")
    checks.check("standbyCountPerPrimary: 1" in sample, "fallback Coordinator standby count per primary")
    coordinator_status = re.search(r"- role: Coordinator\s+desired:\s+4", status_example)
    checks.check(bool(coordinator_status), "fallback partial status Coordinator desired cardinality")
    checks.check(sample.count("standbyCount: 1") >= 3, "fallback intended GTM and DataNode standby counts")
    checks.check(PLACEHOLDER in sample, "fallback explicit image placeholder")
    checks.check("workloadPrimitive: Unselected" in plan, "fallback workload primitive unselected")


def main() -> int:
    checks = Checks()
    texts: dict[str, str] = {}
    for key, path in FILES.items():
        exists = path.is_file()
        checks.check(exists, f"required file exists: {path.relative_to(POC)}")
        text = path.read_text(encoding="utf-8") if exists else ""
        texts[key] = text
        checks.check(bool(text.strip()), f"required file non-empty: {path.relative_to(POC)}")

    yaml_module = None
    try:
        import yaml  # type: ignore

        yaml_module = yaml
    except ImportError:
        checks.warnings.append("PyYAML unavailable; fallback text checks used (not full YAML validation).")

    if yaml_module:
        docs = {}
        for key in ("crd", "sample", "status", "plan"):
            try:
                docs[key] = yaml_module.safe_load(texts[key])
                checks.check(isinstance(docs[key], dict), f"YAML parse: {FILES[key].relative_to(POC)}")
            except Exception as exc:  # validation should report all parse failures
                checks.failed.append(f"YAML parse: {FILES[key].relative_to(POC)} ({exc})")
        if all(isinstance(docs.get(key), dict) for key in ("crd", "sample", "status", "plan")):
            parsed_checks(checks, docs)
    else:
        fallback_checks(checks, texts)

    plan_kinds = set(re.findall(r"^\s*(?:kind|logicalKind):\s*(\S+)\s*$", texts["plan"], re.MULTILINE))
    checks.check(not (plan_kinds & FORBIDDEN_KINDS), "pseudostructure excludes runnable resource kinds")
    image_values = re.findall(r"^\s*image:\s*[\"']?([^\"'\s]+)", texts["sample"], re.MULTILINE)
    checks.check(image_values == [PLACEHOLDER], "no real-looking sample image selected")
    checks.check(WARNING in texts["readme"], "required README warning")
    checks.check(WARNING in texts["plan"], "required pseudostructure warning")
    for label in ("Evidence-backed", "Provisional PoC assumption", "Unconfirmed", "Explicitly out of scope"):
        checks.check(label in texts["trace"], f"traceability classification: {label}")

    for label in checks.passed:
        print(f"PASS: {label}")
    for label in checks.failed:
        print(f"FAIL: {label}")
    for warning in checks.warnings:
        print(f"WARN: {warning}")
    warning_word = "warning" if len(checks.warnings) == 1 else "warnings"
    print(f"SUMMARY: {len(checks.passed)} passed, {len(checks.failed)} failed, {len(checks.warnings)} {warning_word}")
    return 1 if checks.failed else 0


if __name__ == "__main__":
    sys.exit(main())
