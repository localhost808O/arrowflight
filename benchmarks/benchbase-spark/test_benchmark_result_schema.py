#!/usr/bin/env python3
"""Tests for versioned machine-readable benchmark artifacts."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPT_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "benchmark_result_schema",
    SCRIPT_DIR / "benchmark-result-schema.py",
)
SCHEMA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCHEMA)
PUBLICATION_SPEC = importlib.util.spec_from_file_location(
    "validate_benchmark_publication_for_schema_test",
    SCRIPT_DIR / "validate-benchmark-publication.py",
)
PUBLICATION = importlib.util.module_from_spec(PUBLICATION_SPEC)
PUBLICATION_SPEC.loader.exec_module(PUBLICATION)


class BenchmarkResultSchemaTest(unittest.TestCase):
    """Validates artifact construction, versioning, and failure behavior."""

    def setUp(self):
        """Create one complete paired benchmark fixture."""
        self.temp = tempfile.TemporaryDirectory()
        self.results = Path(self.temp.name) / "tpch-compare-q6-test"
        schedule = SCHEMA.paired_schedule(3, "flight-first")
        context = {
            "schema_version": SCHEMA.SCHEMA_VERSION,
            "run_id": self.results.name,
            "started_at": "2026-07-26T00:00:00Z",
            "benchmark": "tpch",
            "mode": "compare",
            "source": {
                "git_sha": "a" * 40,
                "dirty": False,
                "dirty_files": [],
            },
            "workload": {"query_set": "q6", "scale_factor": 1.0},
            "policy": {
                "warmup_seconds": 20,
                "measurement_seconds": 180,
                "cache_policy": "warm-cache",
                "repetitions": 2,
                "paired_observations": 3,
                "minimum_publishable_pairs": 3,
                "terminals": 1,
                "rate": "unlimited",
                "starting_engine_order": ["flight", "direct"],
                "engine_order_schedule": schedule,
            },
            "runtime_dependencies": {
                "maven": {
                    "spark": "3.5.9",
                    "hadoop": "3.3.6",
                    "arrow_flight": "18.0.0",
                    "duckdb_jdbc": "1.4.1.0",
                    "grpc": "1.65.0",
                    "hazelcast": "5.6.0",
                },
                "arrowflight": {
                    "image_ref": "arrowflight-test:test",
                    "image_id": "sha256:" + "a" * 64,
                },
                "benchbase": {
                    "image_ref": "benchbase:test",
                    "image_id": "sha256:" + "b" * 64,
                },
                "generator": {
                    "image_ref": "generator:test",
                    "image_id": "sha256:" + "c" * 64,
                    "duckdb_python": "1.4.1",
                },
                "hive_jdbc": "2.3.9",
                "jvm": "openjdk version 21",
                "python": "3.11.9",
                "docker": "Docker version 28.0.0",
                "docker_compose": "Docker Compose version v2.35.0",
            },
            "configuration": {
                "spark": {
                    "master_url": "spark://spark-master:7077",
                    "sql_ansi_enabled": "true",
                    "direct_parquet_partitions": 2,
                    "thrift_server": "spark-thrift-server:10000",
                },
                "hadoop": {
                    "data_dir": "hdfs://hdfs-namenode:8020/bench",
                    "benchmark_path": "/bench",
                    "block_size_bytes": 1073741824,
                    "replication": 1,
                },
                "jvm": {"java_opts": "-Xmx2g"},
                "flight": {
                    "source_host": "flight-server-1",
                    "source_port": 32010,
                    "batch_size": 65536,
                    "duckdb_threads": "2",
                    "timing_log_level": "DEBUG",
                    "log_level": "INFO",
                },
                "duckdb": {
                    "threads": "2",
                    "generator_image": "generator:test",
                },
            },
            "topology": {
                "cluster_nodes": 2,
                "flight_hosts": ["flight-server-1", "flight-server-2"],
                "flight_servers": [
                    "flight-server-1:32010",
                    "flight-server-2:32010",
                ],
                "host_resources": "8 vCPU, 32 GiB RAM, Spark workers=2",
            },
            "inputs": {
                name: {
                    "path": path,
                    "sha256": "d" * 64,
                }
                for name, path in {
                    "docker_compose": "docker-compose.yml",
                    "runtime_dockerfile": "docker/Dockerfile",
                    "benchbase_dockerfile": (
                        "benchmarks/benchbase-spark/Dockerfile"
                    ),
                    "generator_dockerfile": (
                        "benchmarks/benchbase-spark/"
                        "duckdb-generator.Dockerfile"
                    ),
                    "benchmark_config": (
                        "benchmarks/benchbase-spark/config/tpch.xml"
                    ),
                }.items()
            },
        }
        self.write_json(self.results / "run-context.json", context)
        metadata = {
            "dataset": "tpch",
            "schema": "tpch",
            "scale_factor": 1.0,
            "storage": "hdfs",
            "hdfs_data_dir": "hdfs://hdfs-namenode:8020/bench",
            "hdfs_replication": 1,
            "hdfs_block_size_bytes": 1073741824,
            "shared_parquet_dataset": True,
            "cluster_nodes": 2,
            "reference_queries": [
                {
                    "query_id": 6,
                    "name": "Q6",
                    "sql": "select sum(value) as total from lineitem;",
                    "expected_rows": [{"total": "42"}],
                }
            ],
        }
        self.write_json(self.results / "benchmark-metadata.json", metadata)
        latencies = {
            "flight": [400000, 600000, 800000],
            "direct": [800000, 700000, 1000000],
        }
        event = {
            "schema_version": "1.0.0",
            "timestamp": "1970-01-01T00:00:01.250000Z",
            "qid": "one",
            "node": "flight-server-1",
            "query_digest": "b" * 64,
            "execution_path": "duckdb-aggregation",
            "pushdown_evidence": True,
            "success": True,
            "fallback_target": None,
            "reason": None,
            "failure_reason": None,
        }
        event_two = dict(event, qid="two", node="flight-server-2")
        for scheduled in schedule:
            observation_index = scheduled["observation_index"]
            observation = (
                self.results
                / "observations"
                / SCHEMA.observation_name(observation_index)
            )
            self.write_json(
                observation / "observation-context.json",
                {
                    "observation_index": observation_index,
                    "engine_order": scheduled["engine_order"],
                    "cache_policy": "warm-cache",
                    "warmup_seconds": 20,
                    "repetitions": 2,
                    "started_at": f"2026-07-26T00:0{observation_index}:00Z",
                    "finished_at": f"2026-07-26T00:0{observation_index}:30Z",
                    "status": "completed",
                    "engine_exit_codes": {"flight": 0, "direct": 0},
                    "failures": [],
                },
            )
            for engine in ("flight", "direct"):
                directory = observation / engine
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "query-q6.sql").write_text(
                    "select sum(value) as total from lineitem;\n",
                    encoding="utf-8",
                )
                (directory / "query-q6.actual.csv").write_text(
                    "total\n42\n", encoding="utf-8"
                )
                (directory / "query-q6.plan.txt").write_text(
                    "== Physical Plan ==\nScan\n", encoding="utf-8"
                )
                latency = latencies[engine][observation_index - 1]
                self.write_json(
                    directory / "tpch.summary.json",
                    {
                        "Measured Requests": 2,
                        "Throughput (requests/second)": 0.5,
                    },
                )
                (directory / "tpch.raw.csv").write_text(
                    "Transaction Type Index,Transaction Name,"
                    "Start Time (microseconds),Latency (microseconds),"
                    "Worker Id (start number),Phase Id (index in config file)\n"
                    f"6,Q6,1000000,{latency},0,1\n"
                    f"6,Q6,2000000,{latency + 100000},0,2\n",
                    encoding="utf-8",
                )
                (directory / "tpch.config.xml").write_text(
                    "<parameters/>\n", encoding="utf-8"
                )
                self.write_json(directory / "tpch.metrics.json", {})
                self.write_json(directory / "tpch.params.json", {})
                if engine == "flight":
                    evidence = directory / "execution-paths" / "nodes.jsonl"
                    evidence.parent.mkdir(parents=True)
                    evidence.write_text(
                        json.dumps(event)
                        + "\n"
                        + json.dumps(event_two)
                        + "\n",
                        encoding="utf-8",
                    )

    def tearDown(self):
        """Remove the temporary benchmark fixture."""
        self.temp.cleanup()

    def write_json(self, path, value):
        """Write one JSON fixture."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def build(self):
        """Build artifacts for the current fixture."""
        return SCHEMA.build_artifacts(SimpleNamespace(results=self.results))

    def test_builds_and_validates_complete_paired_artifacts(self):
        """A complete paired run emits all four validated artifact types."""
        artifact = self.build()

        SCHEMA.validate_artifact(artifact)
        PUBLICATION.validate_artifact_set(
            self.results / "benchmark-result.json"
        )
        self.assertTrue((self.results / "run.json").exists())
        self.assertTrue(
            (
                self.results
                / "observations"
                / "observation-001"
                / "flight"
                / "engine-result.json"
            ).exists()
        )
        self.assertTrue((self.results / "aggregate-summary.json").exists())
        self.assertTrue((self.results / "benchmark-result.json").exists())
        first = artifact["observations"][0]
        flight = next(
            engine for engine in first["engines"] if engine["id"] == "flight"
        )
        self.assertEqual(2, len(flight["raw_measurements"]))
        self.assertEqual(
            [1, 2],
            [
                measurement["repetition_index"]
                for measurement in flight["raw_measurements"]
            ],
        )
        self.assertTrue(
            all(
                measurement["observation_index"] == 1
                for measurement in flight["raw_measurements"]
            )
        )
        self.assertEqual("distributed", flight["execution_paths"]["classification"])
        self.assertEqual(
            "duckdb-aggregation", flight["execution_paths"]["uniform_path"]
        )
        self.assertEqual(
            "q6",
            flight["execution_paths"]["events"][0]["logical_query_id"],
        )
        self.assertTrue(flight["publication"]["pushdown_evidence"])
        self.assertEqual(
            "publishable", artifact["comparison"]["publication"]["state"]
        )
        self.assertEqual(
            3, artifact["aggregate_summary"]["paired"]["complete_pairs"]
        )

    def test_rejects_omitted_required_field(self):
        """Validation rejects an artifact with a required field omitted."""
        artifact = self.build()
        del artifact["run"]["source"]

        with self.assertRaisesRegex(
            SCHEMA.SchemaValidationError, r"\$\.run\.source is required"
        ):
            SCHEMA.validate_artifact(artifact)

    def test_rejects_unknown_schema_version(self):
        """Validation requires an explicit new contract for a version change."""
        artifact = self.build()
        artifact["schema_version"] = "9.0.0"

        with self.assertRaisesRegex(
            SCHEMA.SchemaValidationError, "unsupported"
        ):
            SCHEMA.validate_artifact(artifact)

    def test_keeps_v1_validation_available(self):
        """The immutable v1 contract remains validatable after v2 adoption."""
        artifact = self.build()
        legacy_run = {
            "schema_version": "1.0.0",
            "artifact_type": "run",
            "run": artifact["run"],
        }

        self.assertIs(
            legacy_run, SCHEMA.validate_artifact(legacy_run)
        )

    def test_rejects_wrong_nested_type_from_json_schema(self):
        """Validation executes nested JSON Schema constraints."""
        artifact = self.build()
        artifact["run"]["policy"]["warmup_seconds"] = "twenty"

        with self.assertRaisesRegex(
            SCHEMA.SchemaValidationError,
            r"\$\.run\.policy\.warmup_seconds must have type integer",
        ):
            SCHEMA.validate_artifact(artifact)

    def test_dirty_source_is_valid_but_not_publishable(self):
        """A dirty source remains machine-valid while publication is denied."""
        context_path = self.results / "run-context.json"
        context = json.loads(context_path.read_text(encoding="utf-8"))
        context["source"]["dirty"] = True
        context["source"]["dirty_files"] = [" M benchmark-result-schema.py"]
        self.write_json(context_path, context)

        artifact = self.build()

        self.assertTrue(artifact["comparison"]["validity"]["valid"])
        self.assertEqual(
            "not-publishable",
            artifact["comparison"]["publication"]["state"],
        )
        self.assertIn(
            "source-worktree-dirty",
            artifact["comparison"]["publication"]["reasons"],
        )

    def test_serializes_engine_failure_reason(self):
        """A failed engine remains present with explicit validity state."""
        self.write_json(
            self.results
            / "observations"
            / "observation-002"
            / "direct"
            / "benchmark-failure.json",
            {"reason": "query-timeout", "exit_code": 124},
        )

        artifact = self.build()
        second = artifact["observations"][1]
        direct = next(
            engine for engine in second["engines"] if engine["id"] == "direct"
        )

        self.assertEqual("failed", direct["status"])
        self.assertEqual("query-timeout", direct["failure_reason"])
        self.assertFalse(direct["validity"]["valid"])
        self.assertEqual("not-publishable", direct["publication"]["state"])
        self.assertEqual("failed", artifact["run"]["status"])
        self.assertFalse(artifact["comparison"]["validity"]["valid"])
        self.assertEqual(
            2, artifact["aggregate_summary"]["paired"]["complete_pairs"]
        )
        self.assertEqual(
            "not-publishable",
            artifact["comparison"]["publication"]["state"],
        )
        self.assertEqual(3, len(artifact["observations"]))

    def test_schedule_alternates_both_engine_orders(self):
        """The deterministic schedule alternates without manual invocations."""
        schedule = SCHEMA.paired_schedule(5, "direct-first")

        self.assertEqual(
            [
                ["direct", "flight"],
                ["flight", "direct"],
                ["direct", "flight"],
                ["flight", "direct"],
                ["direct", "flight"],
            ],
            [item["engine_order"] for item in schedule],
        )

    def test_single_pair_is_valid_but_not_publishable(self):
        """A quick one-pair run builds a report without claiming publication."""
        context_path = self.results / "run-context.json"
        context = json.loads(context_path.read_text(encoding="utf-8"))
        context["policy"]["paired_observations"] = 1
        context["policy"]["engine_order_schedule"] = SCHEMA.paired_schedule(
            1, "flight-first"
        )
        self.write_json(context_path, context)

        artifact = self.build()

        self.assertEqual(1, len(artifact["observations"]))
        self.assertTrue(artifact["comparison"]["validity"]["valid"])
        self.assertEqual(
            "not-publishable",
            artifact["comparison"]["publication"]["state"],
        )
        self.assertIn(
            "paired-observations-below-minimum: 1 < 3",
            artifact["comparison"]["publication"]["reasons"],
        )

    def test_aggregate_uses_equal_observation_weight_and_reports_spread(self):
        """Aggregate latency summarizes observation medians and their spread."""
        aggregate = self.build()["aggregate_summary"]
        flight = aggregate["engines"]["flight"]["latency_microseconds"]
        ratios = aggregate["paired"][
            "flight_to_direct_median_latency_ratio"
        ]

        self.assertEqual(3, flight["count"])
        self.assertEqual(650000, flight["median"])
        self.assertEqual(400000, flight["spread"])
        self.assertIsNotNone(flight["iqr"])
        self.assertIsNotNone(flight["p95"])
        self.assertEqual(3, ratios["count"])
        self.assertEqual(
            3,
            aggregate["paired"]["queries"]["q6"][
                "flight_to_direct_median_latency_ratio"
            ]["count"],
        )
        self.assertAlmostEqual(
            2 / (180 * 2),
            aggregate["engines"]["flight"]["queries"]["q6"][
                "throughput_requests_per_second"
            ]["median"],
        )

    def test_observation_lifecycle_records_partial_failure_metadata(self):
        """Observation timestamps and one-sided failures remain explicit."""
        lifecycle = Path(self.temp.name) / "lifecycle"
        self.write_json(
            lifecycle / "run-context.json",
            {
                "policy": {
                    "cache_policy": "warm-cache",
                    "warmup_seconds": 30,
                    "repetitions": 4,
                    "engine_order_schedule": SCHEMA.paired_schedule(
                        3, "flight-first"
                    ),
                }
            },
        )
        start_args = SimpleNamespace(
            results=lifecycle, observation_index=2
        )
        started = SCHEMA.start_observation(start_args)
        self.write_json(
            lifecycle
            / "observations"
            / "observation-002"
            / "direct"
            / "benchmark-failure.json",
            {"reason": "query-timeout", "exit_code": 124},
        )
        finish_args = SimpleNamespace(
            results=lifecycle,
            observation_index=2,
            flight_exit_code=0,
            direct_exit_code=124,
        )
        finished = SCHEMA.finish_observation(finish_args)

        self.assertEqual(["direct", "flight"], started["engine_order"])
        self.assertEqual("warm-cache", started["cache_policy"])
        self.assertEqual(30, started["warmup_seconds"])
        self.assertEqual(4, started["repetitions"])
        self.assertIsNotNone(started["started_at"])
        self.assertIsNotNone(finished["finished_at"])
        self.assertEqual("partial-failure", finished["status"])
        self.assertEqual("query-timeout", finished["failures"][0]["reason"])

    def test_mixed_paths_are_preserved_and_unsafe_paths_are_not_evidence(self):
        """Mixed concrete paths remain evidence while unsafe outcomes do not."""
        mixed = SCHEMA.summarize_paths(
            [
                self.path_event("one", "duckdb-scan", True),
                self.path_event("two", "duckdb-aggregation", True),
            ],
            "flight",
        )
        fallback = SCHEMA.summarize_paths(
            [self.path_event("one", "fallback", False)], "flight"
        )
        unknown = SCHEMA.summarize_paths([], "flight")

        self.assertEqual("mixed", mixed["classification"])
        self.assertTrue(mixed["pushdown_evidence"])
        self.assertEqual("fallback", fallback["classification"])
        self.assertFalse(fallback["pushdown_evidence"])
        self.assertEqual("unknown", unknown["classification"])
        self.assertFalse(unknown["pushdown_evidence"])

    @staticmethod
    def path_event(node, path, evidence):
        """Create a minimal runtime path event."""
        return {
            "node": node,
            "execution_path": path,
            "pushdown_evidence": evidence,
            "success": True,
        }


if __name__ == "__main__":
    unittest.main()
