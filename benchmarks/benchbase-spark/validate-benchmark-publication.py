#!/usr/bin/env python3
"""Validate the committed benchmark Pages bundle and versioned report."""

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def load_module(name, filename):
    """Load one sibling benchmark module."""
    path = Path(__file__).resolve().parent / filename
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RESULT_SCHEMA = load_module(
    "benchmark_result_schema_for_publication",
    "benchmark-result-schema.py",
)
PAGES_BUILDER = load_module(
    "build_pages_site_for_publication",
    "build-pages-site.py",
)


def parse_args():
    """Parse publication validator arguments."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    parser = argparse.ArgumentParser(
        description="Validate benchmark Pages and report publication semantics."
    )
    parser.add_argument("--pages", type=Path, default=repo_root / "pages")
    parser.add_argument(
        "--report",
        type=Path,
        default=repo_root
        / "docs"
        / "benchmarks"
        / "tpch-flight-vs-direct-v2.md",
    )
    return parser.parse_args()


def read_json(path):
    """Read one required JSON document."""
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def require(condition, message):
    """Raise a publication error when a condition is false."""
    if not condition:
        raise ValueError(message)


def require_reference(root, reference, label):
    """Require one safe artifact-relative reference to exist."""
    require(reference, f"{label} reference is missing")
    candidate = (root / reference).resolve()
    require(
        root.resolve() in candidate.parents,
        f"{label} escapes its result directory: {reference}",
    )
    require(candidate.is_file(), f"{label} does not exist: {reference}")


def validate_evidence_files(artifact, aggregate_artifact, result_root):
    """Require every published raw, SQL, plan, and metadata link to resolve."""
    require_reference(
        result_root,
        artifact["dataset"]["manifest_ref"],
        "dataset manifest",
    )
    for query in artifact["queries"]:
        query_id = query["logical_query_id"]
        require_reference(
            result_root, query["sql_ref"], f"{query_id} SQL"
        )
        for plan in query["physical_plan_refs"]:
            require_reference(
                result_root,
                plan["path"],
                f"{query_id} {plan['engine']} physical plan",
            )
    for observation in artifact["observations"]:
        for engine in observation["engines"]:
            engine_root = (
                result_root
                / "observations"
                / f"observation-{observation['observation_index']:03d}"
                / engine["id"]
            )
            for name, reference in engine["artifact_refs"].items():
                if name == "report" and reference is None:
                    continue
                require_reference(
                    result_root,
                    reference,
                    f"observation-{observation['observation_index']} "
                    f"{engine['id']} {name}",
                )
            for outcome in engine["correctness"]["queries"]:
                require_reference(
                    engine_root,
                    outcome["actual_ref"],
                    f"observation-{observation['observation_index']} "
                    f"{engine['id']} {outcome['logical_query_id']} actual",
                )
            for event in engine["execution_paths"]["events"]:
                source_ref = event.get("source_ref")
                if source_ref:
                    require_reference(
                        engine_root,
                        source_ref.split("#", 1)[0],
                        f"observation-{observation['observation_index']} "
                        f"{engine['id']} execution event",
                    )
    for reference in aggregate_artifact["raw_repetition_refs"]:
        require_reference(result_root, reference, "aggregate raw repetition")


def validate_artifact_set(result_path):
    """Validate all four artifact types emitted for one paired comparison."""
    result_root = result_path.parent
    paired = read_json(result_path)
    RESULT_SCHEMA.validate_artifact(paired)
    run_path = result_root / "run.json"
    aggregate_path = result_root / "aggregate-summary.json"
    require(run_path.is_file(), f"{run_path} is missing")
    require(aggregate_path.is_file(), f"{aggregate_path} is missing")
    run_artifact = read_json(run_path)
    aggregate_artifact = read_json(aggregate_path)
    RESULT_SCHEMA.validate_artifact(run_artifact)
    RESULT_SCHEMA.validate_artifact(aggregate_artifact)
    expected_engines = paired["run"]["policy"]["paired_observations"] * 2
    engine_paths = sorted(result_root.rglob("engine-result.json"))
    require(
        len(engine_paths) == expected_engines,
        f"{result_root} must contain {expected_engines} engine artifacts",
    )
    for engine_path in engine_paths:
        engine_artifact = read_json(engine_path)
        RESULT_SCHEMA.validate_artifact(engine_artifact)
        require(
            engine_artifact["run_id"] == paired["run"]["id"],
            f"engine artifact run id mismatch: {engine_path}",
        )
    return paired, aggregate_artifact


def validate_bundle(pages_dir, report_path):
    """Validate curated indices, copied artifacts, links, and report coverage."""
    curated_path = pages_dir / "benchmarks.json"
    exploratory_path = pages_dir / "exploratory-benchmarks.json"
    index_path = pages_dir / "index.html"
    require(curated_path.is_file(), f"{curated_path} is missing")
    require(exploratory_path.is_file(), f"{exploratory_path} is missing")
    require(index_path.is_file(), f"{index_path} is missing")
    require(report_path.is_file(), f"{report_path} is missing")
    published_schema = (
        pages_dir / "schemas" / "benchmark-result-v2.schema.json"
    )
    source_schema = RESULT_SCHEMA.schema_path_for(
        RESULT_SCHEMA.SCHEMA_VERSION
    )
    require(published_schema.is_file(), f"{published_schema} is missing")
    require(
        published_schema.read_bytes() == source_schema.read_bytes(),
        "published v2 schema differs from the harness contract",
    )

    curated = read_json(curated_path)
    exploratory = read_json(exploratory_path)
    require(isinstance(curated, list), "pages/benchmarks.json must be an array")
    require(
        isinstance(exploratory, list),
        "pages/exploratory-benchmarks.json must be an array",
    )
    report = report_path.read_text(encoding="utf-8")
    index = index_path.read_text(encoding="utf-8")
    require("report v2" in report.lower(), "versioned report heading is missing")
    require(
        PAGES_BUILDER.REPORT_URL in index,
        "Pages index does not link the versioned report",
    )
    require(
        "schemas/benchmark-result-v2.schema.json" in index,
        "Pages index does not link the published v2 schema",
    )

    seen_ids = set()
    for run in curated:
        run_id = run.get("id")
        require(run_id and run_id not in seen_ids, f"duplicate run id: {run_id}")
        seen_ids.add(run_id)
        require(
            PAGES_BUILDER.is_curated_matrix_run(run),
            f"non-matrix run reached curated index: {run_id}",
        )
        result_path = pages_dir / "benchmarks" / run_id / "benchmark-result.json"
        require(result_path.is_file(), f"machine result is missing: {run_id}")
        artifact, aggregate_artifact = validate_artifact_set(result_path)
        require(
            artifact["schema_version"] == RESULT_SCHEMA.SCHEMA_VERSION,
            f"curated run does not use current schema: {run_id}",
        )
        require(
            artifact["comparison"]["publication"]["state"] == "publishable",
            f"non-publishable artifact was copied: {run_id}",
        )
        require(
            artifact["run"]["id"] == run_id,
            f"index/artifact run id mismatch: {run_id}",
        )
        require(run_id in report, f"report omits curated run: {run_id}")
        for reference in (
            "benchmark-result.json",
            "aggregate-summary.json",
            "benchmark-metadata.json",
        ):
            require(
                (result_path.parent / reference).is_file(),
                f"{run_id} is missing {reference}",
            )
        validate_evidence_files(
            artifact, aggregate_artifact, result_path.parent
        )

    for run in exploratory:
        run_id = run.get("id")
        require(run_id and run_id not in seen_ids, f"duplicate run id: {run_id}")
        seen_ids.add(run_id)
        require(
            run.get("machineReadable") is True,
            f"unversioned run reached exploratory index: {run_id}",
        )
        require(
            not PAGES_BUILDER.is_curated_matrix_run(run),
            f"curated run was misplaced in exploratory index: {run_id}",
        )
        result_path = pages_dir / "benchmarks" / run_id / "benchmark-result.json"
        require(result_path.is_file(), f"machine result is missing: {run_id}")
        artifact, _ = validate_artifact_set(result_path)
        require(
            artifact["comparison"]["publication"]["state"]
            == run.get("publicationState"),
            f"exploratory publication state mismatch: {run_id}",
        )

    for result_path in (pages_dir / "benchmarks").rglob(
        "benchmark-result.json"
    ):
        artifact, _ = validate_artifact_set(result_path)
        require(
            artifact["run"]["id"] in seen_ids,
            f"copied artifact is absent from both indices: {result_path}",
        )

    if not curated:
        require(
            "makes no performance claim" in report,
            "empty matrix report must explicitly make no performance claim",
        )
    return {
        "curated_runs": len(curated),
        "exploratory_runs": len(exploratory),
        "schema_version": RESULT_SCHEMA.SCHEMA_VERSION,
    }


def main():
    """Validate publication inputs and print a machine-readable result."""
    args = parse_args()
    try:
        result = validate_bundle(args.pages.resolve(), args.report.resolve())
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Benchmark publication validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"valid": True, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
