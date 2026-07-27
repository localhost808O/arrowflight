#!/usr/bin/env python3
"""Build the versioned TPC-H reproducibility and interpretation report."""

import argparse
import importlib.util
import json
from pathlib import Path


SCHEMA_VERSION = "2.0.0"
CURATED_QUERIES = {"q1", "q6", "q14"}
CURATED_SCALE_FACTORS = {0.1, 1.0}
CURATED_FLIGHT_NODES = {1, 3, 8}
PAGES_ROOT = "https://nsu-fit.github.io/ArrowFlight/benchmarks"


def load_schema_module():
    """Load the benchmark artifact validator."""
    path = Path(__file__).resolve().parent / "benchmark-result-schema.py"
    spec = importlib.util.spec_from_file_location(
        "benchmark_result_schema_for_report", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RESULT_SCHEMA = load_schema_module()


def parse_args():
    """Parse report generator arguments."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    parser = argparse.ArgumentParser(
        description="Build the versioned benchmark interpretation report."
    )
    parser.add_argument("--results", type=Path, default=script_dir / "results")
    parser.add_argument(
        "--out",
        type=Path,
        default=repo_root
        / "docs"
        / "benchmarks"
        / "tpch-flight-vs-direct-v2.md",
    )
    return parser.parse_args()


def read_json(path):
    """Read one JSON document."""
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def is_matrix_artifact(artifact):
    """Return whether a validated artifact belongs in the curated report."""
    run = artifact["run"]
    workload = run["workload"]
    topology = run["topology"]
    try:
        scale = float(workload["scale_factor"])
        nodes = int(topology["cluster_nodes"])
    except (TypeError, ValueError):
        return False
    return (
        artifact["schema_version"] == SCHEMA_VERSION
        and run["benchmark"].lower() == "tpch"
        and workload["query_set"].lower() in CURATED_QUERIES
        and scale in CURATED_SCALE_FACTORS
        and nodes in CURATED_FLIGHT_NODES
        and artifact["comparison"]["validity"]["valid"] is True
        and artifact["comparison"]["publication"]["state"] == "publishable"
    )


def collect_artifacts(results_root):
    """Collect only schema-valid, publishable v2 matrix artifacts."""
    artifacts = []
    if not results_root.exists():
        return artifacts
    for path in sorted(results_root.rglob("benchmark-result.json")):
        try:
            artifact = read_json(path)
            RESULT_SCHEMA.validate_artifact(artifact)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if is_matrix_artifact(artifact):
            artifacts.append(artifact)
    return sorted(
        artifacts,
        key=lambda item: (
            item["run"]["workload"]["query_set"],
            float(item["run"]["workload"]["scale_factor"]),
            int(item["run"]["topology"]["cluster_nodes"]),
            item["run"]["finished_at"],
        ),
    )


def result_url(artifact, reference="benchmark-result.json"):
    """Return the public URL for one raw run artifact."""
    return f"{PAGES_ROOT}/{artifact['run']['id']}/{reference}"


def number(value):
    """Return a float or null."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def display_number(value, digits=3):
    """Format a nullable report number."""
    numeric = number(value)
    return "n/a" if numeric is None else f"{numeric:.{digits}f}"


def query_point(artifact):
    """Extract one linked query-specific matrix point."""
    run = artifact["run"]
    query_id = run["workload"]["query_set"].lower()
    aggregate = artifact["aggregate_summary"]
    flight_query = aggregate["engines"]["flight"]["queries"][query_id]
    direct_query = aggregate["engines"]["direct"]["queries"][query_id]
    flight_us = flight_query[
        "observation_median_latency_microseconds"
    ]["median"]
    direct_us = direct_query[
        "observation_median_latency_microseconds"
    ]["median"]
    ratio = aggregate["paired"]["queries"][query_id][
        "flight_to_direct_median_latency_ratio"
    ]["median"]
    raw_refs = [
        engine["artifact_refs"]["raw"]
        for observation in artifact["observations"]
        for engine in observation["engines"]
        if engine["artifact_refs"]["raw"]
    ]
    return {
        "artifact": artifact,
        "query": query_id.upper(),
        "scale": run["workload"]["scale_factor"],
        "nodes": run["topology"]["cluster_nodes"],
        "resources": run["topology"]["host_resources"],
        "flight_ms": number(flight_us) / 1000,
        "direct_ms": number(direct_us) / 1000,
        "speedup": 1 / number(ratio) if number(ratio) not in {None, 0} else None,
        "pairs": aggregate["paired"]["complete_pairs"],
        "raw_refs": raw_refs,
    }


def linked_metric(point, key, suffix=""):
    """Render one metric as a link to its versioned aggregate evidence."""
    value = display_number(point[key])
    url = result_url(point["artifact"], "aggregate-summary.json")
    return f"[{value}{suffix}]({url})"


def matrix_table(points):
    """Render all report headline values with evidence links."""
    if not points:
        return (
            "No schema-v2 publishable matrix points are committed at this "
            "revision. Consequently this report makes no performance claim. "
            "The command below is the acceptance path for the first point."
        )
    rows = [
        "| Query | SF | Nodes | Resources | Flight median | Direct median | "
        "Paired speedup | Valid pairs | Evidence |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for point in points:
        artifact = point["artifact"]
        evidence = (
            f"[result]({result_url(artifact)}) · "
            f"[metadata]({result_url(artifact, 'benchmark-metadata.json')}) · "
            f"[raw repetitions](#{artifact['run']['id'].lower()}-raw-repetitions)"
        )
        rows.append(
            f"| {point['query']} | {point['scale']} | {point['nodes']} | "
            f"{point['resources']} | {linked_metric(point, 'flight_ms', ' ms')} | "
            f"{linked_metric(point, 'direct_ms', ' ms')} | "
            f"{linked_metric(point, 'speedup', '×')} | "
            f"[{point['pairs']}]({result_url(artifact)}) | {evidence} |"
        )
    return "\n".join(rows)


def query_charts(points):
    """Render one latency chart for every query represented by valid points."""
    sections = []
    for query in sorted({point["query"] for point in points}):
        selected = [point for point in points if point["query"] == query]
        labels = [
            f'"SF{point["scale"]}/N{point["nodes"]}"' for point in selected
        ]
        flight = [display_number(point["flight_ms"]) for point in selected]
        direct = [display_number(point["direct_ms"]) for point in selected]
        maximum = max(
            max(point["flight_ms"], point["direct_ms"]) for point in selected
        )
        sections.append(
            f"""### {query}

```mermaid
xychart-beta
    title "{query} observation-median latency; lower is faster"
    x-axis [{", ".join(labels)}]
    y-axis "milliseconds" 0 --> {maximum * 1.1:.3f}
    bar [{", ".join(flight)}]
    line [{", ".join(direct)}]
```

Bars are Flight and the line is Spark Direct. The linked table above is
authoritative; the chart is only a visual comparison."""
        )
    if not sections:
        return (
            "Charts are intentionally absent until a schema-v2 publishable "
            "point exists; rendering diagnostic or legacy values here would "
            "violate the publication contract."
        )
    return "\n\n".join(sections)


def conclusions(points):
    """Build conservative, query-specific conclusions."""
    if not points:
        return (
            "There is no valid published evidence for Q1, Q6, or Q14 at this "
            "revision, so neither engine is declared faster."
        )
    lines = []
    for query in sorted({point["query"] for point in points}):
        selected = [point for point in points if point["query"] == query]
        flight_wins = sum(point["speedup"] > 1 for point in selected)
        direct_wins = sum(point["speedup"] < 1 for point in selected)
        ties = len(selected) - flight_wins - direct_wins
        lines.append(
            f"- {query}: Flight has lower paired median latency at "
            f"{flight_wins} point(s), Spark Direct at {direct_wins}, with "
            f"{ties} tie(s). This applies only to the listed scale/topology "
            "points."
        )
    return "\n".join(lines)


def raw_repetition_sections(points):
    """Link every raw engine repetition used by headline values."""
    if not points:
        return "No raw repetitions are referenced because no point is published."
    sections = []
    for point in points:
        artifact = point["artifact"]
        links = " · ".join(
            f"[{Path(reference).parent.as_posix()}]"
            f"({result_url(artifact, reference)})"
            for reference in point["raw_refs"]
        )
        sections.append(
            f"### {artifact['run']['id']} raw repetitions\n\n{links}"
        )
    return "\n\n".join(sections)


def reproduction_section(points):
    """Render an exact clean-checkout command for one matrix point."""
    if points:
        point = points[0]
        run = point["artifact"]["run"]
        source_sha = run["source"]["git_sha"]
        query = point["query"].lower()
        scale = point["scale"]
        nodes = point["nodes"]
        resources = run["topology"]["host_resources"]
        warmup = run["policy"]["warmup_seconds"]
        measurement = run["policy"]["measurement_seconds"]
        repetitions = run["policy"]["repetitions"]
        pairs = run["policy"]["paired_observations"]
        terminals = run["policy"]["terminals"]
        rate = run["policy"]["rate"]
        first_engine = run["policy"]["starting_engine_order"][0]
        compare_order = f"{first_engine}-first"
        configuration = run["configuration"]
        batch_size = configuration["flight"]["batch_size"]
        duckdb_threads = configuration["flight"]["duckdb_threads"]
        ansi_enabled = configuration["spark"]["sql_ansi_enabled"]
        block_size = configuration["hadoop"]["block_size_bytes"]
        benchbase_image = run["runtime_dependencies"]["benchbase"]["image_ref"]
        generator_image = run["runtime_dependencies"]["generator"]["image_ref"]
    else:
        source_sha = (
            '"$(git log -n1 --format=%H -- '
            'docs/benchmarks/tpch-flight-vs-direct-v2.md)"'
        )
        query = "q1"
        scale = 1
        nodes = 3
        resources = "8 vCPU, 32 GiB RAM, Spark workers=3"
        warmup = 30
        measurement = 120
        repetitions = 1
        pairs = 3
        terminals = 1
        rate = "unlimited"
        compare_order = "flight-first"
        batch_size = 65536
        duckdb_threads = "properties-default"
        ansi_enabled = "true"
        block_size = 1073741824
        benchbase_image = "benchbase.azurecr.io/benchbase:latest"
        generator_image = "arrowflight-duckdb-benchmark-generator:latest"
    measurement_line = (
        f"BENCHBASE_TIME_SECONDS={measurement} \\\n"
        if measurement is not None
        else ""
    )
    duckdb_threads_line = (
        f"FLIGHT_DUCKDB_THREADS={duckdb_threads} \\\n"
        if duckdb_threads != "properties-default"
        else ""
    )
    return f"""From a clean Linux checkout with Java 21, Docker, Compose, and
Python 3:

```bash
git clone https://github.com/nsu-fit/ArrowFlight.git
cd ArrowFlight
git checkout {source_sha}
test -z "$(git status --porcelain)"

BENCHBASE_CLUSTER_NODES={nodes} \\
BENCHBASE_PAIRED_OBSERVATIONS={pairs} \\
BENCHBASE_HOST_RESOURCES="{resources}" \\
BENCHBASE_WARMUP_SECONDS={warmup} \\
{measurement_line}BENCHBASE_QUERY_REPETITIONS={repetitions} \\
BENCHBASE_TERMINALS={terminals} \\
BENCHBASE_RATE={rate} \\
BENCHBASE_COMPARE_ORDER={compare_order} \\
BENCHBASE_CACHE_POLICY=warm-cache \\
BENCHBASE_IMAGE="{benchbase_image}" \\
BENCHMARK_GENERATOR_IMAGE="{generator_image}" \\
BENCHMARK_SCALE_FACTOR={scale} \\
HDFS_BLOCK_SIZE_BYTES={block_size} \\
SPARK_SQL_ANSI_ENABLED={ansi_enabled} \\
FLIGHT_BATCH_SIZE={batch_size} \\
{duckdb_threads_line}\
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare {query}
```

The run is publishable only if `benchmark-result.json` reports
`validation.valid=true` and `comparison.publication.state=publishable`.
Container content IDs, dependency versions, source state, configuration,
dataset digest, plans, execution paths, correctness, and every raw repetition
are recorded in that artifact."""


def build_report(artifacts):
    """Build the complete English versioned benchmark report."""
    points = [query_point(artifact) for artifact in artifacts]
    return f"""# TPC-H Spark Direct vs ArrowFlight report v2

This is the curated reproducibility and interpretation report for the
ArrowFlight benchmark matrix. It is generated only from valid, publishable
`paired-comparison` artifacts using schema `{SCHEMA_VERSION}`. The
[JSON Schema](../../benchmarks/benchbase-spark/schema/benchmark-result-v2.schema.json),
[contract guide](../../benchmarks/benchbase-spark/RESULT_SCHEMA.md), and
[benchmark harness](../../benchmarks/benchbase-spark/run-benchbase-spark.sh)
are part of the report contract. The
[GitHub-hosted runner feasibility decision](github-hosted-runner-feasibility.md)
defines which checks may run in required CI and why performance measurements
require dedicated infrastructure.

## Published matrix

{matrix_table(points)}

Latency is the median of equally weighted per-observation query medians.
Paired speedup is `Direct latency / Flight latency`; values above 1 mean
Flight is faster. Throughput is retained in the raw aggregate but is not used
as a headline for serial query points. Lower latency is better.

## Per-query charts

{query_charts(points)}

## Interpretation

{conclusions(points)}

These are query-specific synthetic TPC-H results, not a statement of
production readiness. Spark Direct can win when Flight transport, planning,
or client-side merge overhead exceeds the saved scan/aggregation work. Flight
can win when recorded execution-path evidence shows effective pushdown and
reduced data transfer. A result without correct answers, both formatted
physical plans, resolved runtime pins, or three complete alternating pairs is
diagnostic rather than evidence.

## Reproduce one matrix point

{reproduction_section(points)}

## Environment and artifacts

Every result embeds:

- the clean Git SHA and dirty-file list;
- dataset manifest plus SHA-256, logical query ID, normalized SQL digest, and
  captured expected/actual answers;
- Spark, Hadoop, JVM, Flight, and DuckDB settings;
- Maven versions and ArrowFlight/BenchBase/generator container references and
  content IDs;
- topology and host resources;
- warmup, measurement duration, serial repetition index, cache policy, and
  alternating engine order;
- formatted Spark physical plans, Flight execution-path events, raw BenchBase
  values, explicit failures, validity, and publication reasons.

## Validity classes

- **Smoke** checks setup and a small query, normally with one pair; it is not a
  publishable performance result.
- **Diagnostic** preserves schema-valid artifacts from exploratory query sets,
  incomplete matrix coordinates, or fewer than three pairs.
- **Invalid** means execution, correctness, required evidence, or schema
  validation failed. It remains inspectable but is excluded from the public
  artifact bundle.
- **Publishable** means schema-valid v2, correct, clean-source, fully pinned,
  reproducible evidence with at least three complete pairs and both engine
  orders. Only Q1/Q6/Q14 at SF 0.1/1 and 1/3/8 nodes enter this report.

## Variance, failed experiments, and limitations

The aggregate gives every paired observation equal weight and reports median,
min/max spread, p25/p75, IQR, and p95. Warm-cache runs deliberately do not
reset JVM, Spark, HDFS, or OS caches between engines; alternating order
distributes, but does not eliminate, order effects. Host contention, Spark
code generation, HDFS locality, and Flight batch sizing remain possible
sources of variance.

Failed or partial pairs retain exit codes and reasons but never contribute to
headline aggregates. Schema-valid all-query, smoke, and diagnostic runs belong
only on the exploratory page. Invalid, zero-request legacy, and unversioned
one-sided artifacts are not deployed. No cross-query average or global speedup
is reported.

## Raw repetitions

{raw_repetition_sections(points)}

## Regeneration

```bash
python benchmarks/benchbase-spark/build-benchmark-report.py
python benchmarks/benchbase-spark/build-pages-site.py
python benchmarks/benchbase-spark/validate-benchmark-publication.py
```
"""


def main():
    """Generate the report from the local result corpus."""
    args = parse_args()
    artifacts = collect_artifacts(args.results.resolve())
    output = args.out.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_report(artifacts), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
