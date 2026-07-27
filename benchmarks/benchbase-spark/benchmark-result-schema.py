#!/usr/bin/env python3
"""Build and validate versioned machine-readable benchmark artifacts."""

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = "2.0.0"
SCHEMA_FILE = "schema/benchmark-result-v2.schema.json"
SCHEMA_FILES = {
    "1.0.0": "schema/benchmark-result-v1.schema.json",
    SCHEMA_VERSION: SCHEMA_FILE,
}
RUNTIME_PATHS = {
    "footer-count",
    "footer-stats",
    "duckdb-scan",
    "duckdb-aggregation",
    "duckdb-join",
    "distributed",
    "mixed",
    "fallback",
    "unknown",
}
CONCRETE_PATHS = RUNTIME_PATHS - {
    "distributed",
    "mixed",
    "fallback",
    "unknown",
}
ENGINE_IDS = ("flight", "direct")
MIN_PUBLISHABLE_PAIRS = 3


class SchemaValidationError(ValueError):
    """Reports a machine-readable artifact contract violation."""


def schema_path_for(version):
    """Return the immutable schema file for one supported contract version."""
    relative_path = SCHEMA_FILES.get(version)
    if relative_path is None:
        supported = ", ".join(sorted(SCHEMA_FILES))
        raise SchemaValidationError(
            f"$.schema_version {version!r} is unsupported; supported: {supported}"
        )
    return Path(__file__).resolve().parent / relative_path


def json_type_matches(value, expected):
    """Return whether a value has one JSON Schema primitive type."""
    checks = {
        "null": lambda candidate: candidate is None,
        "object": lambda candidate: isinstance(candidate, dict),
        "array": lambda candidate: isinstance(candidate, list),
        "string": lambda candidate: isinstance(candidate, str),
        "boolean": lambda candidate: isinstance(candidate, bool),
        "integer": lambda candidate: (
            isinstance(candidate, int) and not isinstance(candidate, bool)
        ),
        "number": lambda candidate: (
            isinstance(candidate, (int, float))
            and not isinstance(candidate, bool)
        ),
    }
    return expected in checks and checks[expected](value)


def resolve_schema_ref(root_schema, reference):
    """Resolve one local JSON Pointer reference within a schema."""
    if not reference.startswith("#/"):
        raise SchemaValidationError(
            f"unsupported non-local schema reference: {reference}"
        )
    value = root_schema
    for token in reference[2:].split("/"):
        key = token.replace("~1", "/").replace("~0", "~")
        value = value[key]
    return value


def validate_schema_value(value, schema, root_schema, location="$"):
    """Validate a JSON value with the contract's Draft 2020-12 keywords."""
    if "$ref" in schema:
        validate_schema_value(
            value,
            resolve_schema_ref(root_schema, schema["$ref"]),
            root_schema,
            location,
        )
    for child in schema.get("allOf", []):
        validate_schema_value(value, child, root_schema, location)
    if "oneOf" in schema:
        matches = 0
        errors = []
        for child in schema["oneOf"]:
            try:
                validate_schema_value(value, child, root_schema, location)
                matches += 1
            except SchemaValidationError as error:
                errors.append(str(error))
        if matches != 1:
            detail = errors[0] if errors else f"{location} matched multiple schemas"
            raise SchemaValidationError(
                f"{location} must match exactly one schema: {detail}"
            )

    if "const" in schema and value != schema["const"]:
        raise SchemaValidationError(
            f"{location} must equal {schema['const']!r}, got {value!r}"
        )
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(
            f"{location} must be one of {schema['enum']!r}, got {value!r}"
        )

    expected_types = schema.get("type")
    if expected_types is not None:
        if isinstance(expected_types, str):
            expected_types = [expected_types]
        if not any(json_type_matches(value, expected) for expected in expected_types):
            raise SchemaValidationError(
                f"{location} must have type {' or '.join(expected_types)}"
            )

    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                raise SchemaValidationError(f"{location}.{key} is required")
        properties = schema.get("properties", {})
        for key, child_value in value.items():
            child_location = f"{location}.{key}"
            if key in properties:
                validate_schema_value(
                    child_value,
                    properties[key],
                    root_schema,
                    child_location,
                )
                continue
            additional = schema.get("additionalProperties", True)
            if additional is False:
                raise SchemaValidationError(
                    f"{child_location} is not allowed by the schema"
                )
            if isinstance(additional, dict):
                validate_schema_value(
                    child_value, additional, root_schema, child_location
                )

    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0):
            raise SchemaValidationError(
                f"{location} must contain at least {schema['minItems']} items"
            )
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            raise SchemaValidationError(
                f"{location} must contain at most {schema['maxItems']} items"
            )
        if schema.get("uniqueItems") and len(
            {json.dumps(item, sort_keys=True) for item in value}
        ) != len(value):
            raise SchemaValidationError(f"{location} must contain unique items")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, child_value in enumerate(value):
                validate_schema_value(
                    child_value,
                    item_schema,
                    root_schema,
                    f"{location}[{index}]",
                )

    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            raise SchemaValidationError(
                f"{location} must contain at least {schema['minLength']} characters"
            )
        pattern = schema.get("pattern")
        if pattern and re.search(pattern, value) is None:
            raise SchemaValidationError(
                f"{location} must match pattern {pattern!r}"
            )
        if schema.get("format") == "date-time":
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise SchemaValidationError(
                    f"{location} must be an ISO-8601 date-time"
                ) from error

    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and "minimum" in schema
        and value < schema["minimum"]
    ):
        raise SchemaValidationError(
            f"{location} must be at least {schema['minimum']}"
        )


def validate_against_versioned_schema(artifact):
    """Validate an artifact against its declared immutable JSON Schema."""
    if not isinstance(artifact, dict) or "schema_version" not in artifact:
        raise SchemaValidationError("$.schema_version is required")
    path = schema_path_for(artifact["schema_version"])
    schema = read_json(path)
    artifact_type = artifact.get("artifact_type")
    definition_names = {
        "run": "runArtifact",
        "engine-result": "engineArtifact",
        "paired-comparison": "pairedArtifact",
        "aggregate-summary": "aggregateArtifact",
    }
    definition_name = definition_names.get(artifact_type)
    if definition_name is None:
        raise SchemaValidationError(
            f"$.artifact_type is unsupported: {artifact_type!r}"
        )
    validate_schema_value(
        artifact,
        schema["$defs"][definition_name],
        schema,
    )


def utc_now():
    """Return the current UTC timestamp in ISO-8601 form."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_engine_order(order):
    """Return one supported engine order as a two-item list."""
    normalized = str(order).strip().lower().replace("_", "-")
    aliases = {
        "flight-first": ["flight", "direct"],
        "flight-direct": ["flight", "direct"],
        "direct-first": ["direct", "flight"],
        "direct-flight": ["direct", "flight"],
    }
    if normalized not in aliases:
        raise ValueError(f"unsupported engine order: {order}")
    return aliases[normalized]


def paired_schedule(observation_count, starting_order):
    """Build a deterministic schedule that alternates both engine orders."""
    if observation_count < 1:
        raise ValueError("paired observation count must be a positive integer")
    first_order = normalize_engine_order(starting_order)
    second_order = list(reversed(first_order))
    return [
        {
            "observation_index": index,
            "engine_order": first_order if index % 2 else second_order,
        }
        for index in range(1, observation_count + 1)
    ]


def observation_name(index):
    """Return the stable directory name for one paired observation."""
    return f"observation-{index:03d}"


def read_json(path, default=None):
    """Read one JSON file or return a default when it is absent."""
    if not path.exists():
        return {} if default is None else default
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def write_json(path, value):
    """Write deterministic indented JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def sha256_bytes(content):
    """Return a SHA-256 digest for bytes."""
    return hashlib.sha256(content).hexdigest()


def sha256_file(path):
    """Return a SHA-256 digest for one file."""
    return sha256_bytes(path.read_bytes())


def query_digest(query):
    """Return the runtime-compatible digest of normalized SQL."""
    normalized = " ".join((query or "").strip().lower().split())
    return sha256_bytes(normalized.encode("utf-8"))


def command_output(command, cwd):
    """Run a read-only command and return its stripped combined output."""
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            text=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    output = (completed.stdout + completed.stderr).strip()
    return output or "unknown"


def pom_versions(repo_root):
    """Read pinned server and Spark dependency versions from Maven."""
    pom = ET.parse(repo_root / "pom.xml").getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    properties = pom.find("m:properties", namespace)
    wanted = {
        "arrow.version": "arrow_flight",
        "duckdb.version": "duckdb_jdbc",
        "grpc.version": "grpc",
        "hadoop.version": "hadoop",
        "hazelcast.version": "hazelcast",
        "spark.version": "spark",
    }
    versions = {}
    if properties is not None:
        for child in properties:
            key = child.tag.rsplit("}", 1)[-1]
            if key in wanted:
                versions[wanted[key]] = (child.text or "").strip()
    return versions


def docker_image_id(image_ref, repo_root):
    """Return the local content-addressed ID for one runtime image."""
    if not image_ref:
        return "unknown"
    return command_output(
        ["docker", "image", "inspect", "--format={{.Id}}", image_ref],
        repo_root,
    ).splitlines()[0]


def runtime_dependencies(repo_root):
    """Capture declared versions and locally resolved runtime image IDs."""
    benchbase_ref = os.environ.get(
        "BENCHBASE_IMAGE", "benchbase.azurecr.io/benchbase:latest"
    )
    generator_ref = os.environ.get(
        "BENCHMARK_GENERATOR_IMAGE",
        "arrowflight-duckdb-benchmark-generator:latest",
    )
    return {
        "maven": pom_versions(repo_root),
        "arrowflight": {
            "image_ref": "arrowflight-test:latest",
            "image_id": docker_image_id("arrowflight-test:latest", repo_root),
        },
        "benchbase": {
            "image_ref": benchbase_ref,
            "image_id": docker_image_id(benchbase_ref, repo_root),
        },
        "generator": {
            "image_ref": generator_ref,
            "image_id": docker_image_id(generator_ref, repo_root),
            "duckdb_python": "1.4.1",
        },
        "hive_jdbc": "2.3.9",
        "jvm": command_output(
            ["java", "-version"], repo_root
        ).splitlines()[0],
        "python": platform.python_version(),
        "docker": command_output(["docker", "--version"], repo_root),
        "docker_compose": command_output(
            ["docker", "compose", "version"], repo_root
        ),
    }


def refreshed_runtime_dependencies(captured, repo_root):
    """Resolve the image IDs that were actually present after container builds."""
    dependencies = json.loads(json.dumps(captured))
    for key in ("arrowflight", "benchbase", "generator"):
        image = dependencies.get(key, {})
        resolved = docker_image_id(image.get("image_ref"), repo_root)
        if resolved != "unknown":
            image["image_id"] = resolved
    return dependencies


def runtime_configuration(args):
    """Build explicit Spark, Hadoop, JVM, Flight, and DuckDB settings."""
    return {
        "spark": {
            "master_url": args.spark_master_url,
            "sql_ansi_enabled": args.spark_sql_ansi_enabled,
            "direct_parquet_partitions": args.direct_parquet_partitions,
            "thrift_server": args.spark_thrift_server,
        },
        "hadoop": {
            "data_dir": args.hdfs_data_dir,
            "benchmark_path": args.hdfs_benchmark_path,
            "block_size_bytes": args.hdfs_block_size_bytes,
            "replication": args.hdfs_replication,
        },
        "jvm": {
            "java_opts": args.java_opts,
        },
        "flight": {
            "source_host": args.flight_source_host,
            "source_port": args.flight_source_port,
            "batch_size": args.flight_batch_size,
            "duckdb_threads": args.flight_duckdb_threads,
            "timing_log_level": args.flight_timing_log_level,
            "log_level": args.flight_log_level,
        },
        "duckdb": {
            "threads": args.flight_duckdb_threads,
            "generator_image": os.environ.get(
                "BENCHMARK_GENERATOR_IMAGE",
                "arrowflight-duckdb-benchmark-generator:latest",
            ),
        },
    }


def source_state(repo_root):
    """Capture the exact source revision and dirty state."""
    sha = command_output(["git", "rev-parse", "HEAD"], repo_root)
    status = command_output(
        ["git", "status", "--porcelain", "--untracked-files=normal"], repo_root
    )
    return {
        "git_sha": sha,
        "dirty": status != "unknown" and bool(status),
        "dirty_files": [] if status in {"", "unknown"} else status.splitlines(),
    }


def initialize_context(args):
    """Create the immutable run context captured before benchmark execution."""
    results = args.results.resolve()
    context_path = results / "run-context.json"
    if context_path.exists() and not args.force:
        return read_json(context_path)

    repo_root = args.repo_root.resolve()
    compose = repo_root / "docker-compose.yml"
    benchmark_dir = repo_root / "benchmarks" / "benchbase-spark"
    input_files = {
        "docker_compose": compose,
        "runtime_dockerfile": repo_root / "docker" / "Dockerfile",
        "benchbase_dockerfile": benchmark_dir / "Dockerfile",
        "generator_dockerfile": benchmark_dir / "duckdb-generator.Dockerfile",
        "benchmark_config": benchmark_dir / "config" / f"{args.benchmark}.xml",
    }
    schedule = paired_schedule(args.paired_observations, args.engine_order)
    context = {
        "schema_version": SCHEMA_VERSION,
        "run_id": results.name,
        "started_at": utc_now(),
        "benchmark": args.benchmark,
        "mode": args.mode,
        "source": source_state(repo_root),
        "workload": {
            "query_set": args.query_set or "all",
            "scale_factor": args.scale_factor,
        },
        "policy": {
            "warmup_seconds": args.warmup_seconds,
            "measurement_seconds": args.measurement_seconds,
            "cache_policy": args.cache_policy,
            "repetitions": args.repetitions,
            "paired_observations": args.paired_observations,
            "minimum_publishable_pairs": MIN_PUBLISHABLE_PAIRS,
            "terminals": args.terminals,
            "rate": args.rate,
            "starting_engine_order": normalize_engine_order(args.engine_order),
            "engine_order_schedule": schedule,
        },
        "runtime_dependencies": runtime_dependencies(repo_root),
        "configuration": runtime_configuration(args),
        "topology": {
            "cluster_nodes": args.cluster_nodes,
            "flight_hosts": args.flight_hosts.split(",") if args.flight_hosts else [],
            "flight_servers": (
                args.flight_servers.split(",") if args.flight_servers else []
            ),
            "host_resources": args.host_resources,
        },
        "contract": {
            "schema": SCHEMA_FILE,
            "schema_version": SCHEMA_VERSION,
        },
        "inputs": {
            name: {
                "path": path.relative_to(repo_root).as_posix(),
                "sha256": sha256_file(path) if path.exists() else None,
            }
            for name, path in input_files.items()
        },
    }
    write_json(context_path, context)
    return context


def start_observation(args):
    """Record immutable policy and start time for one scheduled observation."""
    results = args.results.resolve()
    context = read_json(results / "run-context.json")
    schedule = {
        item["observation_index"]: item
        for item in context.get("policy", {}).get("engine_order_schedule", [])
    }
    if args.observation_index not in schedule:
        raise SchemaValidationError(
            f"observation {args.observation_index} is not present in the schedule"
        )
    observation_dir = (
        results / "observations" / observation_name(args.observation_index)
    )
    path = observation_dir / "observation-context.json"
    if path.exists():
        raise SchemaValidationError(f"{path} already exists")
    policy = context["policy"]
    observation = {
        "observation_index": args.observation_index,
        "engine_order": schedule[args.observation_index]["engine_order"],
        "cache_policy": policy["cache_policy"],
        "warmup_seconds": policy["warmup_seconds"],
        "repetitions": policy["repetitions"],
        "started_at": utc_now(),
        "finished_at": None,
        "status": "running",
        "engine_exit_codes": {},
        "failures": [],
    }
    write_json(path, observation)
    return observation


def finish_observation(args):
    """Record completion and failures after both scheduled engines were attempted."""
    results = args.results.resolve()
    observation_dir = (
        results / "observations" / observation_name(args.observation_index)
    )
    path = observation_dir / "observation-context.json"
    observation = read_json(path)
    if not observation:
        raise SchemaValidationError(f"{path} is missing")
    exit_codes = {
        "flight": args.flight_exit_code,
        "direct": args.direct_exit_code,
    }
    failures = []
    for engine_id, exit_code in exit_codes.items():
        if exit_code == 0:
            continue
        marker = read_json(
            observation_dir / engine_id / "benchmark-failure.json", {}
        )
        failures.append(
            {
                "engine": engine_id,
                "exit_code": exit_code,
                "reason": marker.get("reason", f"engine-exit-{exit_code}"),
            }
        )
    observation.update(
        {
            "finished_at": utc_now(),
            "status": (
                "completed"
                if not failures
                else "failed"
                if len(failures) == len(ENGINE_IDS)
                else "partial-failure"
            ),
            "engine_exit_codes": exit_codes,
            "failures": failures,
        }
    )
    write_json(path, observation)
    return observation


def required(value, keys, location):
    """Require object fields and report their JSON location."""
    if not isinstance(value, dict):
        raise SchemaValidationError(f"{location} must be an object")
    for key in keys:
        if key not in value:
            raise SchemaValidationError(f"{location}.{key} is required")


def validate_artifact(artifact):
    """Validate the supported schema version and required artifact fields."""
    validate_against_versioned_schema(artifact)
    artifact_type = artifact["artifact_type"]

    if artifact_type == "run":
        validate_run(artifact["run"], "$.run")
    elif artifact_type == "engine-result":
        validate_engine(artifact["engine"], "$.engine")
    elif artifact_type == "aggregate-summary":
        pass
    else:
        validate_run(artifact["run"], "$.run")
        if artifact["schema_version"] == SCHEMA_VERSION:
            schema = read_json(schema_path_for(SCHEMA_VERSION))
            manifest = artifact["dataset"]["manifest"]
            if artifact["dataset"]["manifest_ref"]:
                validate_schema_value(
                    manifest,
                    schema["$defs"]["manifest"],
                    schema,
                    "$.dataset.manifest",
                )
        expected_observations = artifact["run"]["policy"][
            "paired_observations"
        ]
        if len(artifact["observations"]) != expected_observations:
            raise SchemaValidationError(
                "$.observations must match $.run.policy.paired_observations"
            )
        for index, observation in enumerate(artifact["observations"]):
            validate_observation(observation, f"$.observations[{index}]")
        if artifact["validation"]["valid"] is not True:
            raise SchemaValidationError("$.validation.valid must be true")
        expected_schema = SCHEMA_FILES[artifact["schema_version"]]
        if artifact["validation"]["schema"] != expected_schema:
            raise SchemaValidationError(
                f"$.validation.schema must be {expected_schema!r}"
            )
        validate_schedule_consistency(artifact)
    return artifact


def validate_schedule_consistency(artifact):
    """Require observations to follow the run's deterministic order schedule."""
    schedule = artifact["run"]["policy"]["engine_order_schedule"]
    observations = artifact["observations"]
    for offset, (scheduled, observation) in enumerate(
        zip(schedule, observations), start=1
    ):
        if scheduled["observation_index"] != offset:
            raise SchemaValidationError(
                "$.run.policy.engine_order_schedule must use consecutive indices"
            )
        if observation["observation_index"] != scheduled["observation_index"]:
            raise SchemaValidationError(
                f"$.observations[{offset - 1}].observation_index "
                "must match the schedule"
            )
        if observation["engine_order"] != scheduled["engine_order"]:
            raise SchemaValidationError(
                f"$.observations[{offset - 1}].engine_order "
                "must match the schedule"
            )


def validate_run(run, location):
    """Validate run identity, source state, workload, and policy."""
    required(
        run,
        [
            "id",
            "benchmark",
            "mode",
            "started_at",
            "finished_at",
            "source",
            "workload",
            "policy",
            "runtime_dependencies",
            "topology",
            "status",
            "failure_reason",
        ],
        location,
    )
    required(run["source"], ["git_sha", "dirty", "dirty_files"], f"{location}.source")
    required(
        run["policy"],
        [
            "warmup_seconds",
            "measurement_seconds",
            "cache_policy",
            "repetitions",
            "paired_observations",
            "minimum_publishable_pairs",
            "terminals",
            "rate",
            "starting_engine_order",
            "engine_order_schedule",
        ],
        f"{location}.policy",
    )


def validate_observation(observation, location):
    """Validate one paired observation and both ordered engine outcomes."""
    required(
        observation,
        [
            "observation_index",
            "engine_order",
            "cache_policy",
            "warmup_seconds",
            "repetitions",
            "started_at",
            "finished_at",
            "status",
            "failures",
            "engines",
            "metrics",
            "validity",
        ],
        location,
    )
    if (
        len(observation["engine_order"]) != len(ENGINE_IDS)
        or set(observation["engine_order"]) != set(ENGINE_IDS)
    ):
        raise SchemaValidationError(
            f"{location}.engine_order must contain flight and direct"
        )
    if len(observation["engines"]) != 2:
        raise SchemaValidationError(
            f"{location}.engines must contain both paired outcomes"
        )
    if {engine.get("id") for engine in observation["engines"]} != set(
        ENGINE_IDS
    ):
        raise SchemaValidationError(
            f"{location}.engines must identify flight and direct"
        )
    for index, engine in enumerate(observation["engines"]):
        validate_engine(engine, f"{location}.engines[{index}]")


def validate_engine(engine, location):
    """Validate one engine outcome and its evidence links."""
    required(
        engine,
        [
            "id",
            "order_index",
            "status",
            "failure_reason",
            "summary",
            "raw_measurements",
            "artifact_refs",
            "correctness",
            "physical_plan_refs",
            "execution_paths",
            "validity",
            "publication",
        ],
        location,
    )
    required(
        engine["execution_paths"],
        [
            "classification",
            "uniform_path",
            "pushdown_evidence",
            "reason",
            "nodes",
            "events",
        ],
        f"{location}.execution_paths",
    )
    classification = engine["execution_paths"]["classification"]
    if classification not in RUNTIME_PATHS:
        raise SchemaValidationError(
            f"{location}.execution_paths.classification is unsupported"
        )
    required(engine["validity"], ["valid", "reasons"], f"{location}.validity")
    required(
        engine["publication"],
        ["state", "reasons", "pushdown_evidence"],
        f"{location}.publication",
    )


def read_csv(path):
    """Read a BenchBase or Beeline CSV while removing NUL padding."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(line.replace("\0", "") for line in source))


def latest_file(directory, suffix):
    """Return the newest file with a suffix or null."""
    matches = sorted(
        directory.glob(f"*{suffix}"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def raw_measurements(engine_dir, observation_index):
    """Preserve every measured BenchBase transaction as a repetition."""
    raw_path = latest_file(engine_dir, ".raw.csv")
    measurements = []
    counters = defaultdict(int)
    if raw_path is None:
        return measurements, None
    for row in read_csv(raw_path):
        name = str(row.get("Transaction Name", "")).strip().upper()
        if not name.startswith("Q") or not name[1:].isdigit():
            continue
        counters[name] += 1
        measurements.append(
            {
                "observation_index": observation_index,
                "logical_query_id": name.lower(),
                "repetition_index": counters[name],
                "start_time_microseconds": float(
                    row.get("Start Time (microseconds)", 0) or 0
                ),
                "latency_microseconds": int(
                    float(row.get("Latency (microseconds)", 0) or 0)
                ),
                "worker_id": int(row.get("Worker Id (start number)", 0) or 0),
                "phase_id": int(
                    row.get("Phase Id (index in config file)", 0) or 0
                ),
            }
        )
    return measurements, raw_path


def query_contract(metadata, results, observations):
    """Build logical query identities, digests, plan links, and expected rows."""
    queries = []
    for reference in metadata.get("reference_queries", []):
        query_id = int(reference.get("query_id", 0))
        logical_id = f"q{query_id}"
        observation_dirs = [
            results / "observations" / observation_name(
                observation["observation_index"]
            )
            for observation in observations
        ]
        sql_path = first_existing(
            [
                observation_dir / engine_id / f"query-{logical_id}.sql"
                for observation_dir in observation_dirs
                for engine_id in ENGINE_IDS
            ]
        )
        sql = (
            sql_path.read_text(encoding="utf-8")
            if sql_path is not None
            else reference.get("sql", "")
        )
        plans = []
        for observation in observations:
            observation_dir = (
                results
                / "observations"
                / observation_name(observation["observation_index"])
            )
            for engine_id in ENGINE_IDS:
                plan = observation_dir / engine_id / f"query-{logical_id}.plan.txt"
                if plan.exists():
                    plans.append(
                        {
                            "observation_index": observation["observation_index"],
                            "engine": engine_id,
                            "path": relative(plan, results),
                        }
                    )
        path_refs = []
        for observation in observations:
            for engine in observation["engines"]:
                indexed_events = [
                    (index, event)
                    for index, event in enumerate(
                        engine["execution_paths"].get("events", [])
                    )
                    if event.get("logical_query_id") == logical_id
                ]
                query_paths = summarize_paths(
                    [event for _, event in indexed_events], engine["id"]
                )
                path_refs.append(
                    {
                        "observation_index": observation["observation_index"],
                        "engine": engine["id"],
                        "classification": query_paths["classification"],
                        "uniform_path": query_paths["uniform_path"],
                        "pushdown_evidence": query_paths["pushdown_evidence"],
                        "event_indexes": [index for index, _ in indexed_events],
                    }
                )
        queries.append(
            {
                "logical_query_id": logical_id,
                "name": reference.get("name", logical_id.upper()),
                "digest": query_digest(sql),
                "sql_ref": relative(sql_path, results) if sql_path else None,
                "physical_plan_refs": plans,
                "execution_path_refs": path_refs,
            }
        )
    return queries


def first_existing(paths):
    """Return the first existing path."""
    return next((path for path in paths if path.exists()), None)


def relative(path, root):
    """Return a portable artifact-relative path."""
    return path.resolve().relative_to(root.resolve()).as_posix()


def correctness(metadata, engine_dir):
    """Compare captured query answers with generated reference answers."""
    outcomes = []
    for reference in metadata.get("reference_queries", []):
        query_id = int(reference.get("query_id", 0))
        actual_path = engine_dir / f"query-q{query_id}.actual.csv"
        actual = read_csv(actual_path)
        expected = reference.get("expected_rows", [])
        if not actual_path.exists():
            status = "not-captured"
            reason = "actual-result-missing"
        elif normalize_rows(expected) == normalize_rows(actual):
            status = "pass"
            reason = None
        else:
            status = "fail"
            reason = "result-mismatch"
        outcomes.append(
            {
                "logical_query_id": f"q{query_id}",
                "status": status,
                "failure_reason": reason,
                "actual_ref": (
                    actual_path.name if actual_path.exists() else None
                ),
            }
        )
    statuses = {outcome["status"] for outcome in outcomes}
    overall = "pass" if outcomes and statuses == {"pass"} else (
        "fail" if "fail" in statuses else "not-captured"
    )
    return {"status": overall, "queries": outcomes}


def normalize_rows(rows):
    """Normalize result keys and scalar values for exact comparison."""
    return [
        {str(key).strip().lower(): str(value).strip() for key, value in row.items()}
        for row in rows
    ]


def read_path_events(engine_dir, measurements):
    """Read per-node JSONL evidence and map events to measured query windows."""
    events = []
    evidence_dir = engine_dir / "execution-paths"
    for path in sorted(evidence_dir.glob("*.jsonl")):
        with path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    events.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "node": path.stem,
                            "execution_path": "unknown",
                            "pushdown_evidence": False,
                            "success": False,
                            "reason": f"invalid-jsonl-line-{line_number}: {error.msg}",
                            "logical_query_id": None,
                        }
                    )
                    continue
                event["source_ref"] = (
                    f"execution-paths/{path.name}#L{line_number}"
                )
                measurement = match_measurement(event, measurements)
                event["logical_query_id"] = (
                    measurement["logical_query_id"] if measurement else None
                )
                event["repetition_index"] = (
                    measurement["repetition_index"] if measurement else None
                )
                event["observation_index"] = (
                    measurement["observation_index"] if measurement else None
                )
                events.append(event)
    return events


def match_measurement(event, measurements):
    """Associate a runtime event with the measured transaction containing it."""
    raw_timestamp = event.get("timestamp")
    if not raw_timestamp:
        return None
    try:
        event_us = (
            datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00")).timestamp()
            * 1_000_000
        )
    except (TypeError, ValueError):
        return None
    candidates = [
        measurement
        for measurement in measurements
        if measurement["start_time_microseconds"] <= event_us
        <= measurement["start_time_microseconds"]
        + measurement["latency_microseconds"]
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: abs(event_us - item["start_time_microseconds"]),
    )


def summarize_paths(events, engine_id):
    """Classify uniform distributed, mixed, fallback, and unknown evidence."""
    if engine_id != "flight":
        return {
            "classification": "unknown",
            "uniform_path": None,
            "pushdown_evidence": False,
            "reason": "not-a-flight-runtime",
            "nodes": [],
            "events": [],
        }
    if not events:
        return {
            "classification": "unknown",
            "uniform_path": None,
            "pushdown_evidence": False,
            "reason": "execution-path-evidence-missing",
            "nodes": [],
            "events": [],
        }

    paths = [event.get("execution_path", "unknown") for event in events]
    nodes = defaultdict(Counter)
    for event in events:
        nodes[str(event.get("node", "unknown"))][
            event.get("execution_path", "unknown")
        ] += 1
    node_evidence = [
        {"node": node, "paths": dict(sorted(counts.items()))}
        for node, counts in sorted(nodes.items())
    ]
    unique = set(paths)
    unsafe = unique & {"fallback", "unknown"}
    concrete = unique & CONCRETE_PATHS
    if "fallback" in unsafe:
        classification = "fallback"
        uniform_path = None
    elif "unknown" in unsafe:
        classification = "unknown"
        uniform_path = None
    elif len(concrete) > 1:
        classification = "mixed"
        uniform_path = None
    elif len(nodes) > 1:
        classification = "distributed"
        uniform_path = next(iter(concrete), None)
    else:
        classification = next(iter(concrete), "unknown")
        uniform_path = classification if classification in CONCRETE_PATHS else None
    pushdown = not unsafe and bool(concrete) and all(
        event.get("pushdown_evidence") is True and event.get("success") is True
        for event in events
    )
    return {
        "classification": classification,
        "uniform_path": uniform_path,
        "pushdown_evidence": pushdown,
        "reason": None if pushdown else "fallback-unknown-or-failed-event",
        "nodes": node_evidence,
        "events": events,
    }


def build_engine(
    engine_id,
    engine_dir,
    results,
    metadata,
    order_index,
    observation_index,
):
    """Build one complete engine outcome without discarding raw repetitions."""
    summary_path = latest_file(engine_dir, ".summary.json")
    failure_marker = read_json(engine_dir / "benchmark-failure.json", {})
    measurements, raw_path = raw_measurements(engine_dir, observation_index)
    query_correctness = correctness(metadata, engine_dir)
    events = read_path_events(engine_dir, measurements)
    paths = summarize_paths(events, engine_id)
    status = "failed" if failure_marker else (
        "completed" if summary_path is not None else "failed"
    )
    failure_reason = failure_marker.get("reason")
    if status == "failed" and failure_reason is None:
        failure_reason = "benchbase-summary-missing"

    validity_reasons = []
    if status != "completed":
        validity_reasons.append(failure_reason)
    if not measurements:
        validity_reasons.append("raw-measurements-missing")
    if query_correctness["status"] != "pass":
        validity_reasons.append(f"correctness-{query_correctness['status']}")
    valid = not validity_reasons
    publication_reasons = list(validity_reasons)
    publication_state = "publishable" if valid else "not-publishable"
    refs = {
        "summary": relative(summary_path, results) if summary_path else None,
        "raw": relative(raw_path, results) if raw_path else None,
        "config": relative_file(engine_dir, results, ".config.xml"),
        "metrics": relative_file(engine_dir, results, ".metrics.json"),
        "params": relative_file(engine_dir, results, ".params.json"),
        "report": relative_file(engine_dir, results, ".report.html"),
    }
    plans = [
        relative(path, results)
        for path in sorted(engine_dir.glob("query-q*.plan.txt"))
    ]
    return {
        "id": engine_id,
        "order_index": order_index,
        "status": status,
        "failure_reason": failure_reason,
        "summary": read_json(summary_path, {}) if summary_path else {},
        "raw_measurements": measurements,
        "artifact_refs": refs,
        "correctness": query_correctness,
        "physical_plan_refs": plans,
        "execution_paths": paths,
        "validity": {"valid": valid, "reasons": validity_reasons},
        "publication": {
            "state": publication_state,
            "reasons": publication_reasons,
            "pushdown_evidence": paths["pushdown_evidence"],
        },
    }


def relative_file(directory, root, suffix):
    """Return the newest matching artifact as a relative reference."""
    path = latest_file(directory, suffix)
    return relative(path, root) if path else None


def percentile(values, percentage):
    """Return a linearly interpolated percentile or null for no values."""
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentage / 100
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def distribution(values):
    """Summarize a set with median, spread, quartiles, and p95."""
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "minimum": None,
            "maximum": None,
            "spread": None,
            "p25": None,
            "p75": None,
            "iqr": None,
            "p95": None,
        }
    p25 = percentile(numeric, 25)
    p75 = percentile(numeric, 75)
    minimum = min(numeric)
    maximum = max(numeric)
    return {
        "count": len(numeric),
        "mean": statistics.fmean(numeric),
        "median": statistics.median(numeric),
        "minimum": minimum,
        "maximum": maximum,
        "spread": maximum - minimum,
        "p25": p25,
        "p75": p75,
        "iqr": p75 - p25,
        "p95": percentile(numeric, 95),
    }


def observation_engine_metrics(engine, measurement_seconds, repetitions):
    """Summarize one engine without losing its individual raw measurements."""
    latencies = [
        item["latency_microseconds"] for item in engine["raw_measurements"]
    ]
    queries = defaultdict(list)
    for measurement in engine["raw_measurements"]:
        queries[measurement["logical_query_id"]].append(
            measurement["latency_microseconds"]
        )
    return {
        "samples": len(latencies),
        "latency_microseconds": distribution(latencies),
        "throughput_requests_per_second": engine["summary"].get(
            "Throughput (requests/second)"
        ),
        "queries": {
            query_id: {
                **distribution(values),
                "throughput_requests_per_second": (
                    len(values) / (measurement_seconds * repetitions)
                    if measurement_seconds > 0 and repetitions > 0
                    else None
                ),
            }
            for query_id, values in sorted(queries.items())
        },
    }


def build_observation(results, metadata, scheduled):
    """Build one ordered pair, including partial failures and timestamps."""
    index = scheduled["observation_index"]
    observation_dir = results / "observations" / observation_name(index)
    captured = read_json(observation_dir / "observation-context.json", {})
    order = captured.get("engine_order", scheduled["engine_order"])
    engines = [
        build_engine(
            engine_id,
            observation_dir / engine_id,
            results,
            metadata,
            order.index(engine_id) + 1,
            index,
        )
        for engine_id in ENGINE_IDS
    ]
    validity_reasons = [
        f"{engine['id']}: {reason}"
        for engine in engines
        for reason in engine["validity"]["reasons"]
    ]
    failures = list(captured.get("failures", []))
    for engine in engines:
        if engine["status"] == "failed" and not any(
            failure.get("engine") == engine["id"] for failure in failures
        ):
            failures.append(
                {
                    "engine": engine["id"],
                    "exit_code": captured.get("engine_exit_codes", {}).get(
                        engine["id"]
                    ),
                    "reason": engine["failure_reason"],
                }
            )
    for failure in failures:
        reason = f"{failure.get('engine')}: {failure.get('reason')}"
        if reason not in validity_reasons:
            validity_reasons.append(reason)
    failed_engines = {failure.get("engine") for failure in failures}
    return {
        "observation_index": index,
        "engine_order": order,
        "cache_policy": captured.get(
            "cache_policy", scheduled["cache_policy"]
        ),
        "warmup_seconds": captured.get(
            "warmup_seconds", scheduled["warmup_seconds"]
        ),
        "repetitions": captured.get(
            "repetitions", scheduled["repetitions"]
        ),
        "started_at": captured.get("started_at"),
        "finished_at": captured.get("finished_at"),
        "status": (
            "completed"
            if not failures
            else "failed"
            if failed_engines == set(ENGINE_IDS)
            else "partial-failure"
        ),
        "failures": failures,
        "engines": engines,
        "metrics": {
            engine["id"]: observation_engine_metrics(
                engine,
                scheduled["measurement_seconds"],
                scheduled["repetitions"],
            )
            for engine in engines
        },
        "validity": {
            "valid": not validity_reasons,
            "reasons": validity_reasons,
        },
    }


def aggregate_summary(observations):
    """Aggregate equally weighted observation values and complete paired ratios."""
    engine_summaries = {}
    for engine_id in ENGINE_IDS:
        values = []
        query_observations = defaultdict(list)
        for observation in observations:
            engine = next(
                item for item in observation["engines"] if item["id"] == engine_id
            )
            metrics = observation["metrics"][engine_id]
            latency = metrics["latency_microseconds"]
            value = {
                "observation_index": observation["observation_index"],
                "order_index": engine["order_index"],
                "status": engine["status"],
                "valid": engine["validity"]["valid"],
                "failure_reason": engine["failure_reason"],
                "samples": metrics["samples"],
                "median_latency_microseconds": latency["median"],
                "p95_latency_microseconds": latency["p95"],
                "minimum_latency_microseconds": latency["minimum"],
                "maximum_latency_microseconds": latency["maximum"],
                "throughput_requests_per_second": metrics[
                    "throughput_requests_per_second"
                ],
            }
            values.append(value)
            if engine["validity"]["valid"]:
                for query_id, query_summary in metrics["queries"].items():
                    query_observations[query_id].append(
                        {
                            "latency": query_summary["median"],
                            "throughput": query_summary[
                                "throughput_requests_per_second"
                            ],
                        }
                    )
        valid_values = [value for value in values if value["valid"]]
        engine_summaries[engine_id] = {
            "scheduled_observations": len(values),
            "successful_observations": len(valid_values),
            "total_samples": sum(value["samples"] for value in valid_values),
            "observation_values": values,
            "latency_microseconds": distribution(
                [
                    value["median_latency_microseconds"]
                    for value in valid_values
                ]
            ),
            "p95_latency_microseconds": distribution(
                [value["p95_latency_microseconds"] for value in valid_values]
            ),
            "throughput_requests_per_second": distribution(
                [
                    value["throughput_requests_per_second"]
                    for value in valid_values
                ]
            ),
            "queries": {
                query_id: {
                    "observation_median_latency_microseconds": distribution(
                        [value["latency"] for value in query_values]
                    ),
                    "throughput_requests_per_second": distribution(
                        [value["throughput"] for value in query_values]
                    ),
                }
                for query_id, query_values in sorted(query_observations.items())
            },
        }

    pair_values = []
    paired_query_values = defaultdict(list)
    engine_values = {
        engine_id: {
            value["observation_index"]: value
            for value in engine_summaries[engine_id]["observation_values"]
        }
        for engine_id in ENGINE_IDS
    }
    for observation in observations:
        flight = engine_values["flight"][observation["observation_index"]]
        direct = engine_values["direct"][observation["observation_index"]]
        flight_latency = flight["median_latency_microseconds"]
        direct_latency = direct["median_latency_microseconds"]
        complete = (
            observation["validity"]["valid"]
            and flight["valid"]
            and direct["valid"]
            and flight_latency is not None
            and direct_latency not in {None, 0}
        )
        pair_values.append(
            {
                "observation_index": observation["observation_index"],
                "engine_order": observation["engine_order"],
                "complete": complete,
                "flight_to_direct_median_latency_ratio": (
                    flight_latency / direct_latency if complete else None
                ),
                "flight_minus_direct_median_latency_microseconds": (
                    flight_latency - direct_latency if complete else None
                ),
            }
        )
        flight_metrics = observation["metrics"]["flight"]["queries"]
        direct_metrics = observation["metrics"]["direct"]["queries"]
        for query_id in sorted(set(flight_metrics) | set(direct_metrics)):
            flight_query = flight_metrics.get(query_id, {}).get("median")
            direct_query = direct_metrics.get(query_id, {}).get("median")
            query_complete = (
                observation["validity"]["valid"]
                and flight["valid"]
                and direct["valid"]
                and flight_query is not None
                and direct_query not in {None, 0}
            )
            paired_query_values[query_id].append(
                {
                    "observation_index": observation["observation_index"],
                    "complete": query_complete,
                    "flight_to_direct_median_latency_ratio": (
                        flight_query / direct_query
                        if query_complete
                        else None
                    ),
                    "flight_minus_direct_median_latency_microseconds": (
                        flight_query - direct_query
                        if query_complete
                        else None
                    ),
                }
            )
    complete_pairs = [value for value in pair_values if value["complete"]]
    return {
        "engines": engine_summaries,
        "paired": {
            "scheduled_pairs": len(pair_values),
            "complete_pairs": len(complete_pairs),
            "observation_values": pair_values,
            "flight_to_direct_median_latency_ratio": distribution(
                [
                    value["flight_to_direct_median_latency_ratio"]
                    for value in complete_pairs
                ]
            ),
            "flight_minus_direct_median_latency_microseconds": distribution(
                [
                    value[
                        "flight_minus_direct_median_latency_microseconds"
                    ]
                    for value in complete_pairs
                ]
            ),
            "queries": {
                query_id: {
                    "observation_values": values,
                    "flight_to_direct_median_latency_ratio": distribution(
                        [
                            value[
                                "flight_to_direct_median_latency_ratio"
                            ]
                            for value in values
                            if value["complete"]
                        ]
                    ),
                    "flight_minus_direct_median_latency_microseconds": (
                        distribution(
                            [
                                value[
                                    "flight_minus_direct_median_latency_microseconds"
                                ]
                                for value in values
                                if value["complete"]
                            ]
                        )
                    ),
                }
                for query_id, values in sorted(paired_query_values.items())
            },
        },
    }


def dataset_contract(results):
    """Embed the generated dataset manifest and its content digest."""
    manifest_path = results / "benchmark-metadata.json"
    if not manifest_path.exists():
        return {
            "manifest_ref": None,
            "manifest_sha256": None,
            "manifest": {},
        }
    return {
        "manifest_ref": manifest_path.name,
        "manifest_sha256": sha256_file(manifest_path),
        "manifest": read_json(manifest_path),
    }


def dependency_pin_reasons(dependencies):
    """Return publication failures for unresolved runtime dependency pins."""
    reasons = []
    maven = dependencies.get("maven", {})
    for name in (
        "arrow_flight",
        "duckdb_jdbc",
        "grpc",
        "hadoop",
        "hazelcast",
        "spark",
    ):
        if not maven.get(name) or maven.get(name) == "unknown":
            reasons.append(f"runtime-dependency-unpinned: maven.{name}")
    for name in ("arrowflight", "benchbase", "generator"):
        image = dependencies.get(name, {})
        image_id = str(image.get("image_id", ""))
        if not image_id.startswith("sha256:"):
            reasons.append(f"runtime-image-unresolved: {name}")
    for name in ("hive_jdbc", "jvm", "python", "docker", "docker_compose"):
        if not dependencies.get(name) or dependencies.get(name) == "unknown":
            reasons.append(f"runtime-dependency-unpinned: {name}")
    return reasons


def reproducibility_reasons(run, dataset, queries, observations):
    """Return reasons why a valid comparison is not independently reproducible."""
    reasons = []
    source = run["source"]
    if not re.fullmatch(r"[0-9a-f]{40}", str(source.get("git_sha", ""))):
        reasons.append("source-git-sha-unavailable")
    if source.get("dirty"):
        reasons.append("source-worktree-dirty")
    if not dataset.get("manifest_ref") or not dataset.get("manifest_sha256"):
        reasons.append("dataset-manifest-missing")
    manifest = dataset.get("manifest", {})
    if manifest:
        if manifest.get("dataset") != run["benchmark"]:
            reasons.append("dataset-benchmark-mismatch")
        if number_or_none(manifest.get("scale_factor")) != number_or_none(
            run["workload"]["scale_factor"]
        ):
            reasons.append("dataset-scale-factor-mismatch")
        if manifest.get("cluster_nodes") != run["topology"]["cluster_nodes"]:
            reasons.append("dataset-topology-mismatch")
        if manifest.get("shared_parquet_dataset") is not True:
            reasons.append("dataset-is-not-shared")
    if not queries:
        reasons.append("logical-query-contract-missing")
    expected_evidence = {
        (observation_index, engine_id)
        for observation_index in range(
            1, run["policy"]["paired_observations"] + 1
        )
        for engine_id in ENGINE_IDS
    }
    for query in queries:
        query_id = query["logical_query_id"]
        if not query.get("sql_ref"):
            reasons.append(f"query-sql-missing: {query_id}")
        plan_evidence = {
            (item.get("observation_index"), item.get("engine"))
            for item in query.get("physical_plan_refs", [])
        }
        if plan_evidence != expected_evidence:
            reasons.append(f"formatted-physical-plans-incomplete: {query_id}")
        path_evidence = {
            (item.get("observation_index"), item.get("engine"))
            for item in query.get("execution_path_refs", [])
        }
        if path_evidence != expected_evidence:
            reasons.append(f"execution-path-references-incomplete: {query_id}")
    for observation in observations:
        observation_index = observation["observation_index"]
        for engine in observation["engines"]:
            engine_id = engine["id"]
            refs = engine["artifact_refs"]
            for name in ("summary", "raw", "config", "metrics", "params"):
                if not refs.get(name):
                    reasons.append(
                        f"raw-artifact-missing: observation-{observation_index} "
                        f"{engine_id} {name}"
                    )
            if (
                engine_id == "flight"
                and not engine["execution_paths"].get("events")
            ):
                reasons.append(
                    f"execution-path-evidence-missing: "
                    f"observation-{observation_index}"
                )
    if run["topology"].get("host_resources") in {
        None,
        "",
        "not-recorded",
    }:
        reasons.append("host-resources-not-recorded")
    for name, input_digest in run.get("inputs", {}).items():
        if not re.fullmatch(
            r"[0-9a-f]{64}", str(input_digest.get("sha256", ""))
        ):
            reasons.append(f"benchmark-input-missing: {name}")
    reasons.extend(dependency_pin_reasons(run["runtime_dependencies"]))
    return reasons


def number_or_none(value):
    """Return a float for equality checks or null for non-numeric values."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_artifacts(args):
    """Build run, engine, aggregate, and paired artifacts and validate each."""
    results = args.results.resolve()
    context = read_json(results / "run-context.json")
    if not context:
        raise SchemaValidationError(
            f"{results / 'run-context.json'} is missing; run init before execution"
        )
    metadata = read_json(results / "benchmark-metadata.json")
    policy = context["policy"]
    schedule = policy["engine_order_schedule"]
    observations = [
        build_observation(
            results,
            metadata,
            {
                **scheduled,
                "cache_policy": policy["cache_policy"],
                "warmup_seconds": policy["warmup_seconds"],
                "repetitions": policy["repetitions"],
                "measurement_seconds": policy["measurement_seconds"],
            },
        )
        for scheduled in schedule
    ]
    failed = [
        observation
        for observation in observations
        if observation["status"] != "completed"
        or not observation["validity"]["valid"]
    ]
    repo_root = Path(__file__).resolve().parent.parent.parent
    dependencies = refreshed_runtime_dependencies(
        context["runtime_dependencies"], repo_root
    )
    run = {
        "id": context["run_id"],
        "benchmark": context["benchmark"],
        "mode": context["mode"],
        "started_at": context["started_at"],
        "finished_at": utc_now(),
        "source": context["source"],
        "workload": context["workload"],
        "policy": context["policy"],
        "runtime_dependencies": dependencies,
        "configuration": context["configuration"],
        "topology": context["topology"],
        "inputs": context["inputs"],
        "status": "failed" if failed else "completed",
        "failure_reason": (
            "; ".join(
                f"observation-{observation['observation_index']}: "
                + ", ".join(observation["validity"]["reasons"])
                for observation in failed
            )
            if failed else None
        ),
    }
    run_artifact = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "run",
        "run": run,
    }
    engine_artifacts = [
        (
            observation,
            engine,
            {
                "schema_version": SCHEMA_VERSION,
                "artifact_type": "engine-result",
                "run_id": run["id"],
                "observation_index": observation["observation_index"],
                "engine": engine,
            },
        )
        for observation in observations
        for engine in observation["engines"]
    ]
    aggregate = aggregate_summary(observations)
    aggregate_artifact = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "aggregate-summary",
        "run_id": run["id"],
        "summary": aggregate,
        "raw_repetition_refs": [
            engine["artifact_refs"]["raw"]
            for observation in observations
            for engine in observation["engines"]
            if engine["artifact_refs"]["raw"]
        ],
    }
    comparison_valid = all(
        observation["validity"]["valid"] for observation in observations
    )
    correctness_statuses = {
        engine["correctness"]["status"]
        for observation in observations
        for engine in observation["engines"]
    }
    observed_orders = {
        tuple(observation["engine_order"]) for observation in observations
    }
    complete_pairs = aggregate["paired"]["complete_pairs"]
    publication_reasons = []
    if len(observations) < MIN_PUBLISHABLE_PAIRS:
        publication_reasons.append(
            f"paired-observations-below-minimum: "
            f"{len(observations)} < {MIN_PUBLISHABLE_PAIRS}"
        )
    if complete_pairs < MIN_PUBLISHABLE_PAIRS:
        publication_reasons.append(
            f"complete-pairs-below-minimum: "
            f"{complete_pairs} < {MIN_PUBLISHABLE_PAIRS}"
        )
    if observed_orders != {
        ("flight", "direct"),
        ("direct", "flight"),
    }:
        publication_reasons.append("both-engine-orders-not-observed")
    publication_reasons.extend(
        f"observation-{observation['observation_index']}: {reason}"
        for observation in observations
        for reason in observation["validity"]["reasons"]
    )
    dataset = dataset_contract(results)
    queries = query_contract(metadata, results, observations)
    publication_reasons.extend(
        reproducibility_reasons(run, dataset, queries, observations)
    )
    publication_reasons = list(dict.fromkeys(publication_reasons))
    comparison = {
        "engine_order_schedule": [
            {
                "observation_index": observation["observation_index"],
                "engine_order": observation["engine_order"],
            }
            for observation in observations
        ],
        "correctness": {
            "status": (
                "pass" if correctness_statuses == {"pass"} else
                "fail" if "fail" in correctness_statuses else "not-captured"
            ),
            "observation_outcomes": [
                {
                    "observation_index": observation["observation_index"],
                    "engines": {
                        engine["id"]: engine["correctness"]["status"]
                        for engine in observation["engines"]
                    },
                }
                for observation in observations
            ],
        },
        "validity": {
            "valid": comparison_valid,
            "reasons": [
                f"observation-{observation['observation_index']}: {reason}"
                for observation in observations
                for reason in observation["validity"]["reasons"]
            ],
        },
        "publication": {
            "state": (
                "publishable" if not publication_reasons else "not-publishable"
            ),
            "reasons": publication_reasons,
        },
    }
    paired = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": "paired-comparison",
        "run": run,
        "dataset": dataset,
        "queries": queries,
        "observations": observations,
        "comparison": comparison,
        "aggregate_summary": aggregate,
        "validation": {
            "valid": True,
            "schema": SCHEMA_FILE,
        },
    }

    artifacts = [
        run_artifact,
        *(artifact for _, _, artifact in engine_artifacts),
        aggregate_artifact,
        paired,
    ]
    for artifact in artifacts:
        validate_artifact(artifact)
    write_json(results / "run.json", run_artifact)
    for observation, engine, artifact in engine_artifacts:
        engine_dir = (
            results
            / "observations"
            / observation_name(observation["observation_index"])
            / engine["id"]
        )
        write_json(engine_dir / "engine-result.json", artifact)
    write_json(results / "aggregate-summary.json", aggregate_artifact)
    write_json(results / "benchmark-result.json", paired)
    return paired


def parse_args():
    """Parse command-line arguments."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    parser = argparse.ArgumentParser(
        description="Build and validate benchmark result schema v1 artifacts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Capture immutable pre-run context.")
    init.add_argument("--results", type=Path, required=True)
    init.add_argument("--repo-root", type=Path, default=repo_root)
    init.add_argument("--benchmark", required=True)
    init.add_argument("--mode", default="compare")
    init.add_argument("--query-set", default="all")
    init.add_argument("--scale-factor", type=float, required=True)
    init.add_argument("--warmup-seconds", type=int, default=0)
    init.add_argument("--measurement-seconds", type=int)
    init.add_argument("--cache-policy", default="warm-cache")
    init.add_argument("--repetitions", type=int, default=1)
    init.add_argument("--paired-observations", type=int, default=1)
    init.add_argument("--terminals", type=int, default=1)
    init.add_argument("--rate", default="unlimited")
    init.add_argument("--engine-order", default="flight-first")
    init.add_argument("--cluster-nodes", type=int, required=True)
    init.add_argument("--flight-hosts", default="")
    init.add_argument("--flight-servers", default="")
    init.add_argument("--host-resources", default="not-recorded")
    init.add_argument("--spark-master-url", default="spark://spark-master:7077")
    init.add_argument("--spark-sql-ansi-enabled", default="true")
    init.add_argument("--spark-thrift-server", default="spark-thrift-server:10000")
    init.add_argument("--direct-parquet-partitions", type=int, required=True)
    init.add_argument("--hdfs-data-dir", required=True)
    init.add_argument("--hdfs-benchmark-path", required=True)
    init.add_argument("--hdfs-block-size-bytes", type=int, required=True)
    init.add_argument("--hdfs-replication", type=int, default=1)
    init.add_argument("--java-opts", default="-Xmx2g")
    init.add_argument("--flight-source-host", required=True)
    init.add_argument("--flight-source-port", type=int, default=32010)
    init.add_argument("--flight-batch-size", type=int, required=True)
    init.add_argument("--flight-duckdb-threads", default="properties-default")
    init.add_argument("--flight-timing-log-level", default="inherited")
    init.add_argument("--flight-log-level", default="INFO")
    init.add_argument("--force", action="store_true")

    build = subparsers.add_parser("build", help="Build and validate final artifacts.")
    build.add_argument("--results", type=Path, required=True)

    schedule = subparsers.add_parser(
        "schedule", help="Print the deterministic paired schedule as TSV."
    )
    schedule.add_argument("--paired-observations", type=int, required=True)
    schedule.add_argument("--engine-order", default="flight-first")

    observation_start = subparsers.add_parser(
        "observation-start", help="Record one observation start."
    )
    observation_start.add_argument("--results", type=Path, required=True)
    observation_start.add_argument(
        "--observation-index", type=int, required=True
    )

    observation_finish = subparsers.add_parser(
        "observation-finish", help="Record one observation finish."
    )
    observation_finish.add_argument("--results", type=Path, required=True)
    observation_finish.add_argument(
        "--observation-index", type=int, required=True
    )
    observation_finish.add_argument(
        "--flight-exit-code", type=int, required=True
    )
    observation_finish.add_argument(
        "--direct-exit-code", type=int, required=True
    )

    validate = subparsers.add_parser("validate", help="Validate one artifact.")
    validate.add_argument("artifact", type=Path)
    return parser.parse_args()


def main():
    """Run the selected schema lifecycle command."""
    args = parse_args()
    try:
        if args.command == "init":
            result = initialize_context(args)
            output = args.results.resolve() / "run-context.json"
        elif args.command == "build":
            result = build_artifacts(args)
            output = args.results.resolve() / "benchmark-result.json"
        elif args.command == "schedule":
            result = paired_schedule(
                args.paired_observations, args.engine_order
            )
            for observation in result:
                print(
                    observation["observation_index"],
                    *observation["engine_order"],
                    sep="\t",
                )
            return 0
        elif args.command == "observation-start":
            result = start_observation(args)
            output = (
                args.results.resolve()
                / "observations"
                / observation_name(args.observation_index)
                / "observation-context.json"
            )
        elif args.command == "observation-finish":
            result = finish_observation(args)
            output = (
                args.results.resolve()
                / "observations"
                / observation_name(args.observation_index)
                / "observation-context.json"
            )
        else:
            output = args.artifact.resolve()
            result = validate_artifact(read_json(output))
    except (OSError, KeyError, SchemaValidationError, ValueError) as error:
        print(f"Benchmark result validation failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(
        {
            "artifact": str(output),
            "schema_version": result.get("schema_version", SCHEMA_VERSION),
            "valid": True,
        },
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
