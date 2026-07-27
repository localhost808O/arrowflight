#!/usr/bin/env python3
import argparse
import csv
import html
import importlib.util
import json
import shutil
from datetime import datetime
from pathlib import Path


THROUGHPUT = "Throughput (requests/second)"
CURATED_QUERIES = {"q1", "q6", "q14"}
CURATED_SCALE_FACTORS = {0.1, 1.0}
CURATED_FLIGHT_NODES = {1, 3, 8}
REPORT_URL = (
    "https://github.com/nsu-fit/ArrowFlight/blob/main/"
    "docs/benchmarks/tpch-flight-vs-direct-v2.md"
)
FEASIBILITY_URL = (
    "https://github.com/nsu-fit/ArrowFlight/blob/main/"
    "docs/benchmarks/github-hosted-runner-feasibility.md"
)


def load_schema_module():
    """Load the versioned benchmark validator shared with the harness."""
    path = Path(__file__).resolve().parent / "benchmark-result-schema.py"
    spec = importlib.util.spec_from_file_location(
        "benchmark_result_schema_for_pages", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RESULT_SCHEMA = load_schema_module()


def parse_args():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    parser = argparse.ArgumentParser(description="Build a GitHub Pages dashboard for BenchBase results.")
    parser.add_argument("--results", type=Path, default=script_dir / "results")
    parser.add_argument("--out", type=Path, default=repo_root / "pages")
    return parser.parse_args()


def read_json(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def read_valid_machine_result(path):
    """Read one machine artifact only when its declared schema validates."""
    if not path.exists():
        return None
    try:
        artifact = read_json(path)
        RESULT_SCHEMA.validate_artifact(artifact)
        return artifact
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt(value):
    value = number(value)
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def latency_ms(summary, label):
    return number(summary.get("Latency Distribution", {}).get(f"{label} Latency (microseconds)")) / 1000


def per_query_latency_rows(run_dir, base):
    grouped = {}
    for row in read_csv(run_dir / f"{base}.raw.csv"):
        name = str(row.get("Transaction Name", "")).strip().upper()
        if not name.startswith("Q") or not name[1:].isdigit():
            continue
        try:
            latency = float(row.get("Latency (microseconds)", "")) / 1000
        except (TypeError, ValueError):
            continue
        grouped.setdefault(int(name[1:]), []).append(latency)
    return [
        {
            "query": f"Q{query_id}",
            "avg": sum(values) / len(values),
            "samples": len(values),
        }
        for query_id, values in sorted(grouped.items())
    ]


def first_summary(run_dir):
    summaries = sorted(run_dir.glob("*.summary.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    return summaries[0] if summaries else None


def report_link_for(run_dir, copied_root):
    reports = sorted(run_dir.glob("*.report.html"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not reports:
        return ""
    return str(copied_root / run_dir.name / reports[0].name).replace("\\", "/")


def copied_link(results_root, path):
    rel = path.relative_to(results_root)
    return str(Path("benchmarks") / rel).replace("\\", "/")


def query_label_from_name(name):
    marker = "-q"
    if marker not in name:
        return "all"
    tail = name.split(marker, 1)[1]
    parts = tail.rsplit("-", 2)
    return f"q{parts[0]}" if parts else "all"


def metadata_for(run_dir):
    for path in [run_dir / "benchmark-metadata.json", run_dir.parent / "benchmark-metadata.json"]:
        if path.exists():
            return read_json(path)
    return {}


def run_timestamp(path):
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except OSError:
        return ""


def load_single_run(results_root, run_dir):
    summary_path = first_summary(run_dir)
    if not summary_path:
        return None
    summary = read_json(summary_path)
    metadata = metadata_for(run_dir)
    base = summary_path.name.removesuffix(".summary.json")
    report = run_dir / f"{base}.report.html"
    rows = read_csv(run_dir / f"{base}.results.csv")
    last = rows[-1] if rows else {}
    return {
        "kind": "run",
        "id": str(run_dir.relative_to(results_root)).replace("\\", "/"),
        "title": run_dir.name,
        "benchmark": summary.get("Benchmark Type", metadata.get("dataset", "n/a")),
        "path": run_dir.name,
        "query": query_label_from_name(run_dir.name),
        "scale": metadata.get("scale_factor", summary.get("scalefactor", "n/a")),
        "timestamp": run_timestamp(run_dir),
        "throughput": number(summary.get(THROUGHPUT)),
        "lastThroughput": number(last.get(THROUGHPUT)),
        "avgMs": latency_ms(summary, "Average"),
        "p95Ms": latency_ms(summary, "95th Percentile"),
        "measuredRequests": summary.get("Measured Requests", 0),
        "queryLatencies": per_query_latency_rows(run_dir, base),
        "report": copied_link(results_root, report) if report.exists() else "",
        "files": copied_link(results_root, run_dir),
    }


def load_machine_engine(results_root, run_dir, machine_result, engine_id):
    aggregate = (
        machine_result.get("aggregate_summary", {})
        .get("engines", {})
        .get(engine_id, {})
    )
    latency = aggregate.get("latency_microseconds", {})
    throughput = aggregate.get("throughput_requests_per_second", {})
    query_latencies = []
    for query_id, query in sorted(
        aggregate.get("queries", {}).items(),
        key=lambda item: int(item[0].removeprefix("q")),
    ):
        query_summary = query.get(
            "observation_median_latency_microseconds", {}
        )
        query_latencies.append(
            {
                "query": query_id.upper(),
                "avg": number(query_summary.get("median")) / 1000,
                "samples": query_summary.get("count", 0),
                "throughput": number(
                    query.get(
                        "throughput_requests_per_second", {}
                    ).get("median")
                ),
            }
        )
    return {
        "kind": "run",
        "id": f"{run_dir.relative_to(results_root).as_posix()}/{engine_id}",
        "title": engine_id,
        "benchmark": machine_result.get("run", {}).get("benchmark", "n/a"),
        "path": engine_id,
        "query": machine_result.get("run", {})
        .get("workload", {})
        .get("query_set", "all"),
        "scale": machine_result.get("run", {})
        .get("workload", {})
        .get("scale_factor", "n/a"),
        "timestamp": machine_result.get("run", {}).get("finished_at", ""),
        "throughput": number(throughput.get("median")),
        "lastThroughput": number(throughput.get("median")),
        "avgMs": number(latency.get("median")) / 1000,
        "p95Ms": number(latency.get("p95")) / 1000,
        "measuredRequests": aggregate.get("total_samples", 0),
        "queryLatencies": query_latencies,
        "report": "",
        "files": copied_link(results_root, run_dir),
    }


def paired_speedup(machine_result):
    """Return Direct latency divided by Flight latency for the paired aggregate."""
    ratio = (
        machine_result.get("aggregate_summary", {})
        .get("paired", {})
        .get("flight_to_direct_median_latency_ratio", {})
        .get("median")
    )
    ratio = number(ratio)
    return 1 / ratio if ratio > 0 else None


def load_compare_run(results_root, run_dir):
    report = run_dir / "compare.report.html"
    if not report.exists():
        return None
    machine_path = run_dir / "benchmark-result.json"
    machine_result = read_valid_machine_result(machine_path)
    if not machine_path.exists() or not machine_result:
        return None
    validation = machine_result.get("validation", {})
    publication = machine_result.get("comparison", {}).get("publication", {})
    if validation.get("valid") is not True:
        return None

    if machine_result.get("observations"):
        flight = load_machine_engine(
            results_root, run_dir, machine_result, "flight"
        )
        direct = load_machine_engine(
            results_root, run_dir, machine_result, "direct"
        )
    else:
        flight = load_single_run(results_root, run_dir / "flight")
        direct = load_single_run(results_root, run_dir / "direct")
    metadata = metadata_for(run_dir)
    run = machine_result.get("run", {})
    policy = run.get("policy", {})
    topology = run.get("topology", {})
    benchmark = run.get("benchmark", metadata.get("dataset", "tpch"))
    return {
        "kind": "compare",
        "id": str(run_dir.relative_to(results_root)).replace("\\", "/"),
        "title": run_dir.name,
        "benchmark": benchmark,
        "path": "flight vs direct",
        "query": run.get("workload", {}).get(
            "query_set", query_label_from_name(run_dir.name)
        ),
        "scale": run.get("workload", {}).get(
            "scale_factor", metadata.get("scale_factor", "n/a")
        ),
        "timestamp": run.get("finished_at", run_timestamp(run_dir)),
        "report": copied_link(results_root, report),
        "files": copied_link(results_root, run_dir),
        "flight": flight,
        "direct": direct,
        "flightNodes": run.get("topology", {}).get(
            "cluster_nodes", metadata.get("cluster_nodes", "n/a")
        ),
        "flightHosts": run.get("topology", {}).get(
            "flight_hosts", metadata.get("flight_hosts", "n/a")
        ),
        "hostResources": topology.get(
            "host_resources", metadata.get("host_resources", "not-recorded")
        ),
        "cachePolicy": policy.get(
            "cache_policy", metadata.get("cache_policy", "not-recorded")
        ),
        "warmupSeconds": policy.get(
            "warmup_seconds", metadata.get("warmup_seconds", "not-recorded")
        ),
        "orderPolicy": policy.get(
            "engine_order_schedule",
            metadata.get("engine_order_schedule", "not-recorded"),
        ),
        "sampleCount": min(
            number((flight or {}).get("measuredRequests")),
            number((direct or {}).get("measuredRequests")),
        ),
        "pairedObservations": policy.get(
            "paired_observations", metadata.get("paired_observations", 0)
        ),
        "pairedSpeedup": paired_speedup(machine_result),
        "machineReadable": bool(machine_result),
        "publicationState": publication.get("state", "not-publishable"),
        "publicationReasons": publication.get("reasons", []),
    }


def is_curated_matrix_run(run):
    """Return whether a run belongs to the agreed publishable matrix."""
    try:
        scale = float(run.get("scale"))
        nodes = int(run.get("flightNodes"))
    except (TypeError, ValueError):
        return False
    return (
        run.get("kind") == "compare"
        and run.get("machineReadable") is True
        and run.get("publicationState") == "publishable"
        and str(run.get("benchmark", "")).lower() == "tpch"
        and str(run.get("query", "")).lower() in CURATED_QUERIES
        and scale in CURATED_SCALE_FACTORS
        and nodes in CURATED_FLIGHT_NODES
        and run.get("hostResources") not in {None, "", "not-recorded"}
    )


def collect_runs(results_root):
    runs = []
    if not results_root.exists():
        return runs

    compare_dirs = set()
    for report in results_root.rglob("compare.report.html"):
        compare_dirs.add(report.parent)
        run = load_compare_run(results_root, report.parent)
        if run:
            runs.append(run)

    runs.sort(key=lambda item: item.get("timestamp", ""), reverse=True)
    return runs


def copy_results(results_root, out_dir):
    target = out_dir / "benchmarks"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / ".gitkeep").write_text("", encoding="utf-8")

    if not results_root.exists():
        return

    for path in results_root.iterdir():
        if not path.is_dir():
            continue
        machine_path = path / "benchmark-result.json"
        machine_result = read_valid_machine_result(machine_path)
        if (
            not machine_path.exists()
            or not machine_result
            or not (path / "compare.report.html").is_file()
        ):
            continue
        validation = machine_result.get("validation", {})
        if validation.get("valid") is not True:
            continue
        destination = target / path.name
        shutil.copytree(path, destination)


def copy_schemas(out_dir):
    """Publish immutable machine-readable result schemas with the dashboard."""
    source = Path(__file__).resolve().parent / "schema"
    target = out_dir / "schemas"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)


def metric_cell(run, side, field, suffix=""):
    nested = run.get(side) or {}
    value = nested.get(field)
    if value in (None, ""):
        return "-"
    return f"{fmt(value)}{suffix}"


def order_label(value):
    """Render the deterministic engine-order schedule compactly."""
    if not isinstance(value, list):
        return str(value)
    return " / ".join(
        "→".join(item.get("engine_order", []))
        for item in value
        if isinstance(item, dict)
    )


def grouped_latency_chart(run):
    flight_rows = (run.get("flight") or {}).get("queryLatencies", [])
    direct_rows = (run.get("direct") or {}).get("queryLatencies", [])
    flight = {int(row["query"][1:]): row for row in flight_rows}
    direct = {int(row["query"][1:]): row for row in direct_rows}
    query_ids = (
        list(range(1, 23))
        if str(run.get("query", "")).lower() == "all"
        else sorted(set(flight) | set(direct))
    )
    if not query_ids:
        return ""

    width = 1120
    height = 430
    pad_left = 72
    pad_right = 24
    pad_top = 46
    pad_bottom = 62
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    group_w = plot_w / len(query_ids)
    bar_w = min(17, group_w * 0.34)
    max_y = max(
        [number(row.get("avg")) for row in list(flight.values()) + list(direct.values())]
        or [1]
    ) or 1

    def sy(value):
        return pad_top + plot_h - (value / max_y) * plot_h

    y_labels = []
    for step in range(5):
        value = max_y * step / 4
        y = sy(value)
        y_labels.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" '
            f'y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="{pad_left-10}" y="{y+4:.1f}" '
            f'text-anchor="end">{fmt(value)}</text>'
        )

    bars = []
    labels = []
    for index, query_id in enumerate(query_ids):
        center = pad_left + group_w * (index + 0.5)
        labels.append(
            f'<text x="{center:.1f}" y="{height-pad_bottom+22}" '
            f'text-anchor="middle">q{query_id:02d}</text>'
        )
        for offset, rows, color, label in [
            (-bar_w, flight, "#2563eb", "Flight"),
            (0, direct, "#f97316", "Direct"),
        ]:
            row = rows.get(query_id)
            if row is None:
                continue
            value = number(row.get("avg"))
            y = sy(value)
            tooltip = (
                f"{label} Q{query_id}: {fmt(value)} ms, "
                f"{int(number(row.get('samples')))} samples"
            )
            bars.append(
                f'<rect x="{center+offset:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
                f'height="{pad_top+plot_h-y:.1f}" fill="{color}">'
                f'<title>{html.escape(tooltip)}</title></rect>'
            )
        flight_value = number((flight.get(query_id) or {}).get("avg"))
        direct_value = number((direct.get(query_id) or {}).get("avg"))
        if flight_value > 0 and direct_value > 0:
            winner = "Flight" if flight_value < direct_value else "Direct"
            speedup = max(flight_value, direct_value) / min(
                flight_value, direct_value
            )
            labels.append(
                f'<text x="{center:.1f}" y="{height-pad_bottom+39}" '
                f'text-anchor="middle" font-size="9">{winner} {speedup:.2f}x</text>'
            )

    return f"""
  <section class="query-chart">
    <h2>Latest TPC-H Q1-Q22 Average Query Execution Time</h2>
    <p>{html.escape(run["title"])} · average execution time of measured
    BenchBase samples. <strong>Lower is faster.</strong></p>
    <div class="legend">
      <span><i style="background:#2563eb"></i>Flight (ms)</span>
      <span><i style="background:#f97316"></i>Direct (ms)</span>
    </div>
    <svg viewBox="0 0 {width} {height}" role="img"
         aria-label="TPC-H per-query average execution time comparison">
      <rect x="0" y="0" width="{width}" height="{height}" fill="#fff"/>
      {''.join(y_labels)}
      <line x1="{pad_left}" y1="{height-pad_bottom}" x2="{width-pad_right}"
            y2="{height-pad_bottom}" stroke="#9ca3af"/>
      <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}"
            y2="{height-pad_bottom}" stroke="#9ca3af"/>
      {''.join(bars)}
      {''.join(labels)}
      <text x="18" y="{height/2:.1f}" text-anchor="middle"
            transform="rotate(-90 18 {height/2:.1f})">average query execution time, ms</text>
    </svg>
  </section>
"""


def build_index(runs, curated=True):
    payload = json.dumps(runs, ensure_ascii=False)
    latest_all_compare = next(
        (
            run for run in runs
            if run.get("kind") == "compare"
            and str(run.get("query", "")).lower() == "all"
            and (run.get("flight") or {}).get("queryLatencies")
            and (run.get("direct") or {}).get("queryLatencies")
        ),
        None,
    )
    all_query_chart = (
        grouped_latency_chart(latest_all_compare)
        if latest_all_compare and not curated
        else ""
    )
    rows = []
    for run in runs:
        if run["kind"] == "compare":
            flight = run.get("flight") or {}
            direct = run.get("direct") or {}
            links = [
                f'<a href="{html.escape(run["report"])}">compare</a>',
            ]
            if flight.get("report"):
                links.append(f'<a href="{html.escape(flight["report"])}">flight</a>')
            if direct.get("report"):
                links.append(f'<a href="{html.escape(direct["report"])}">direct</a>')
            links.append(f'<a href="{html.escape(run["files"])}">files</a>')
            rows.append(
                f"""
      <tr data-benchmark="{html.escape(str(run["benchmark"]))}" data-kind="compare" data-query="{html.escape(str(run["query"]))}">
        <td><strong>{html.escape(run["title"])}</strong><span>compare</span></td>
        <td>{html.escape(str(run["benchmark"]))}</td>
        <td>{html.escape(str(run["query"]))}</td>
        <td>{html.escape(str(run["scale"]))}</td>
        <td>{metric_cell(run, "flight", "throughput", " req/s")}</td>
        <td>{metric_cell(run, "direct", "throughput", " req/s")}</td>
        <td>{metric_cell(run, "flight", "avgMs", " ms")}</td>
        <td>{metric_cell(run, "direct", "avgMs", " ms")}</td>
        <td>{html.escape(str(run.get("flightNodes", "-")))}</td>
        <td>{html.escape(str(run.get("hostResources", "-")))}</td>
        <td>{html.escape(str(run.get("cachePolicy", "-")))}</td>
        <td>{html.escape(str(run.get("warmupSeconds", "-")))}</td>
        <td>{html.escape(order_label(run.get("orderPolicy", "-")))}</td>
        <td>{html.escape(str(run.get("pairedObservations", "-")))}</td>
        <td>{html.escape(str(run.get("sampleCount", "-")))}</td>
        <td>{fmt(run.get("pairedSpeedup")) + "x" if run.get("pairedSpeedup") else "-"}</td>
        <td title="{html.escape('; '.join(str(reason) for reason in run.get('publicationReasons', [])))}">{html.escape(str(run.get("publicationState", "-")))}</td>
        <td>{' '.join(links)}</td>
      </tr>
"""
            )
        else:
            links = [f'<a href="{html.escape(run["report"])}">report</a>' if run.get("report") else ""]
            links.append(f'<a href="{html.escape(run["files"])}">files</a>')
            rows.append(
                f"""
      <tr data-benchmark="{html.escape(str(run["benchmark"]))}" data-kind="run" data-query="{html.escape(str(run["query"]))}">
        <td><strong>{html.escape(run["title"])}</strong><span>{html.escape(run["path"])}</span></td>
        <td>{html.escape(str(run["benchmark"]))}</td>
        <td>{html.escape(str(run["query"]))}</td>
        <td>{html.escape(str(run["scale"]))}</td>
        <td>{fmt(run["throughput"])} req/s</td>
        <td>-</td>
        <td>{fmt(run["avgMs"])} ms</td>
        <td>-</td>
        <td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>
        <td>-</td><td>legacy</td>
        <td>{' '.join(link for link in links if link)}</td>
      </tr>
"""
            )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{"Curated Arrow Flight Benchmark Matrix" if curated else "Exploratory Arrow Flight Benchmarks"}</title>
  <style>
    body {{ margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: #f7f8fb; color: #111827; }}
    main {{ max-width: 1220px; margin: 0 auto; padding: 28px; }}
    h1 {{ margin: 0 0 6px; font-size: 30px; }}
    p {{ color: #5b6472; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 22px 0; }}
    select, input {{ border: 1px solid #d1d5db; border-radius: 7px; padding: 9px 10px; background: #fff; min-width: 150px; }}
    input {{ flex: 1; min-width: 230px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
    .value {{ font-size: 25px; font-weight: 750; margin-top: 3px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #edf0f3; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: #5b6472; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; background: #fbfcfe; }}
    td strong {{ display: block; }}
    td span {{ display: block; color: #6b7280; font-size: 12px; margin-top: 2px; }}
    a {{ color: #2563eb; margin-right: 10px; white-space: nowrap; }}
    .empty {{ display: none; background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; }}
    .query-chart {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; margin: 18px 0; }}
    .query-chart h2 {{ margin: 0 0 4px; }}
    .query-chart svg {{ width: 100%; height: auto; }}
    .query-chart svg text {{ fill: #4b5563; font-size: 12px; }}
    .legend {{ display: flex; gap: 18px; margin: 12px 0; color: #4b5563; }}
    .legend i {{ display: inline-block; width: 11px; height: 11px; border-radius: 2px; margin-right: 6px; vertical-align: -1px; }}
  </style>
</head>
<body>
<main>
  <h1>{"Curated Arrow Flight Benchmark Matrix" if curated else "Exploratory Benchmarks"}</h1>
  <p>{"Only schema-valid, publishable paired TPC-H Q1/Q6/Q14 measurements at SF 0.1/1 and 1/3/8 Flight nodes are shown. Conclusions are per query; no global speedup is claimed." if curated else "Schema-valid all-query, smoke, diagnostic, and other non-matrix results are kept separate here. Invalid and unversioned legacy artifacts are not deployed."}</p>
  <p><a href="{REPORT_URL}">Versioned reproducibility and interpretation report (v2)</a> · <a href="{FEASIBILITY_URL}">GitHub-hosted runner decision</a> · <a href="schemas/benchmark-result-v2.schema.json">JSON Schema v2</a></p>
  <p><a href="{"exploratory.html" if curated else "index.html"}">{"Open exploratory/history" if curated else "Back to curated matrix"}</a></p>

  <div class="cards">
    <div class="card"><div class="label">Reports</div><div class="value" id="visible-count">0</div></div>
    <div class="card"><div class="label">Compare Runs</div><div class="value" id="compare-count">0</div></div>
    <div class="card"><div class="label">Latest Run</div><div class="value" id="latest-run">-</div></div>
  </div>

  {all_query_chart}

  <div class="toolbar">
    <select id="benchmark-filter">
      <option value="">All benchmarks</option>
    </select>
    <select id="kind-filter">
      <option value="">All paths</option>
      <option value="compare">Flight vs Direct</option>
      <option value="run">Single run</option>
    </select>
    <select id="query-filter">
      <option value="">All queries</option>
    </select>
    <input id="search" type="search" placeholder="Search run name">
  </div>

  <div class="empty" id="empty">No reports match the selected filters.</div>
  <table>
    <thead>
      <tr>
        <th>Run</th>
        <th>Benchmark</th>
        <th>Query</th>
        <th>SF</th>
        <th>Flight thr.</th>
        <th>Direct thr.</th>
        <th>Flight avg</th>
        <th>Direct avg</th>
        <th>Nodes</th>
        <th>Resources</th>
        <th>Cache</th>
        <th>Warmup, s</th>
        <th>Order</th>
        <th>Pairs</th>
        <th>Samples</th>
        <th>Paired speedup</th>
        <th>State</th>
        <th>Open</th>
      </tr>
    </thead>
    <tbody id="runs">
      {''.join(rows)}
    </tbody>
  </table>
</main>
<script type="application/json" id="runs-json">{html.escape(payload)}</script>
<script>
  const runs = JSON.parse(document.getElementById('runs-json').textContent);
  const rows = [...document.querySelectorAll('#runs tr')];
  const benchmarkFilter = document.getElementById('benchmark-filter');
  const kindFilter = document.getElementById('kind-filter');
  const queryFilter = document.getElementById('query-filter');
  const search = document.getElementById('search');

  function addOptions(select, values) {{
    values.filter(Boolean).sort().forEach((value) => {{
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    }});
  }}

  addOptions(benchmarkFilter, [...new Set(runs.map((run) => String(run.benchmark)))]);
  addOptions(queryFilter, [...new Set(runs.map((run) => String(run.query)))]);
  document.getElementById('compare-count').textContent = runs.filter((run) => run.kind === 'compare').length;
  document.getElementById('latest-run').textContent = runs[0]?.title || '-';

  function applyFilters() {{
    const benchmark = benchmarkFilter.value;
    const kind = kindFilter.value;
    const query = queryFilter.value;
    const term = search.value.trim().toLowerCase();
    let visible = 0;
    rows.forEach((row) => {{
      const text = row.textContent.toLowerCase();
      const show = (!benchmark || row.dataset.benchmark === benchmark)
        && (!kind || row.dataset.kind === kind)
        && (!query || row.dataset.query === query)
        && (!term || text.includes(term));
      row.style.display = show ? '' : 'none';
      if (show) visible += 1;
    }});
    document.getElementById('visible-count').textContent = visible;
    document.getElementById('empty').style.display = visible ? 'none' : 'block';
  }}

  [benchmarkFilter, kindFilter, queryFilter, search].forEach((element) => {{
    element.addEventListener('input', applyFilters);
  }});
  applyFilters();
</script>
</body>
</html>
"""


def main():
    args = parse_args()
    results_root = args.results.resolve()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    copy_results(results_root, out_dir)
    copy_schemas(out_dir)

    runs = collect_runs(results_root)
    curated_runs = [run for run in runs if is_curated_matrix_run(run)]
    exploratory_runs = [
        run for run in runs if not is_curated_matrix_run(run)
    ]
    (out_dir / "benchmarks.json").write_text(
        json.dumps(curated_runs, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "exploratory-benchmarks.json").write_text(
        json.dumps(exploratory_runs, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "index.html").write_text(
        build_index(curated_runs, curated=True), encoding="utf-8"
    )
    (out_dir / "exploratory.html").write_text(
        build_index(exploratory_runs, curated=False), encoding="utf-8"
    )
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")
    print(out_dir / "index.html")


if __name__ == "__main__":
    main()
