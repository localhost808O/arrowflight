#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("build-pages-site.py")
SPEC = importlib.util.spec_from_file_location("build_pages_site", MODULE_PATH)
BUILD_PAGES_SITE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD_PAGES_SITE)


class AllQueryChartTest(unittest.TestCase):
    def test_latest_all_compare_is_rendered_on_index(self):
        query_rows = [{"query": "Q1", "avg": 12.0, "samples": 4}]
        run = {
            "kind": "compare",
            "id": "tpch-compare-all-test",
            "title": "tpch-compare-all-test",
            "benchmark": "tpch",
            "path": "flight vs direct",
            "query": "all",
            "scale": 1,
            "timestamp": "2026-07-23T10:00:00",
            "report": "benchmarks/test/compare.report.html",
            "files": "benchmarks/test",
            "flight": {
                "throughput": 1,
                "avgMs": 12,
                "report": "",
                "queryLatencies": query_rows,
            },
            "direct": {
                "throughput": 1,
                "avgMs": 10,
                "report": "",
                "queryLatencies": [
                    {"query": "Q1", "avg": 24.0, "samples": 4}
                ],
            },
            "flightNodes": 4,
        }

        page = BUILD_PAGES_SITE.build_index([run], curated=False)

        self.assertIn("Latest TPC-H Q1-Q22 Average Query Execution Time", page)
        self.assertIn("average query execution time, ms", page)
        self.assertIn("q01", page)
        self.assertIn("q22", page)
        self.assertIn("Flight (ms)", page)
        self.assertIn("Direct (ms)", page)
        self.assertIn("Lower is faster", page)
        self.assertIn("Flight 2.00x", page)

    def test_chart_identifies_lower_latency_as_faster(self):
        """A lower Flight latency is labelled as a Flight speedup."""
        run = {
            "title": "q1-check",
            "query": "all",
            "flight": {
                "queryLatencies": [
                    {"query": "Q1", "avg": 365.8, "samples": 164}
                ]
            },
            "direct": {
                "queryLatencies": [
                    {"query": "Q1", "avg": 1224.5, "samples": 49}
                ]
            },
        }

        chart = BUILD_PAGES_SITE.grouped_latency_chart(run)

        self.assertIn("Lower is faster", chart)
        self.assertIn("Flight 3.35x", chart)
        self.assertIn("Flight Q1: 365.8 ms, 164 samples", chart)
        self.assertIn("Direct Q1: 1224.5 ms, 49 samples", chart)

    def test_curated_matrix_excludes_all_query_and_legacy_runs(self):
        """Only agreed machine-readable matrix points reach the main page."""
        base = {
            "kind": "compare",
            "benchmark": "tpch",
            "scale": 1,
            "flightNodes": 3,
            "machineReadable": True,
            "publicationState": "publishable",
            "hostResources": "8 vCPU, 32 GiB RAM",
        }

        self.assertTrue(
            BUILD_PAGES_SITE.is_curated_matrix_run({**base, "query": "q1"})
        )
        self.assertFalse(
            BUILD_PAGES_SITE.is_curated_matrix_run({**base, "query": "all"})
        )
        self.assertFalse(
            BUILD_PAGES_SITE.is_curated_matrix_run(
                {**base, "query": "q1", "machineReadable": False}
            )
        )
        self.assertFalse(
            BUILD_PAGES_SITE.is_curated_matrix_run(
                {
                    **base,
                    "query": "q1",
                    "publicationState": "not-publishable",
                }
            )
        )


class MachineResultTest(unittest.TestCase):
    def test_loads_paired_aggregate_instead_of_one_engine_run(self):
        machine_result = {
            "run": {
                "benchmark": "tpch",
                "finished_at": "2026-07-26T10:00:00Z",
                "workload": {"query_set": "q6", "scale_factor": 1.0},
                "topology": {
                    "cluster_nodes": 3,
                    "flight_hosts": ["flight-server-1"],
                },
            },
            "validation": {"valid": True},
            "comparison": {
                "publication": {"state": "publishable", "reasons": []}
            },
            "observations": [{"observation_index": 1}],
            "aggregate_summary": {
                "paired": {
                    "flight_to_direct_median_latency_ratio": {
                        "median": 0.5
                    }
                },
                "engines": {
                    "flight": {
                        "total_samples": 6,
                        "latency_microseconds": {
                            "median": 4000,
                            "p95": 6000,
                        },
                        "throughput_requests_per_second": {"median": 2.5},
                        "queries": {
                            "q6": {
                                "observation_median_latency_microseconds": {
                                    "median": 4000,
                                    "count": 3,
                                }
                            }
                        },
                    },
                    "direct": {
                        "total_samples": 6,
                        "latency_microseconds": {
                            "median": 8000,
                            "p95": 10000,
                        },
                        "throughput_requests_per_second": {"median": 1.5},
                        "queries": {},
                    },
                }
            },
        }

        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            run_dir = results / "tpch-compare-q6-test"
            run_dir.mkdir()
            (run_dir / "compare.report.html").write_text(
                "report", encoding="utf-8"
            )
            (run_dir / "benchmark-result.json").write_text(
                json.dumps(machine_result), encoding="utf-8"
            )

            with mock.patch.object(
                BUILD_PAGES_SITE,
                "read_valid_machine_result",
                return_value=machine_result,
            ):
                run = BUILD_PAGES_SITE.load_compare_run(results, run_dir)

        self.assertEqual(4.0, run["flight"]["avgMs"])
        self.assertEqual(2.5, run["flight"]["throughput"])
        self.assertEqual("Q6", run["flight"]["queryLatencies"][0]["query"])
        self.assertEqual(3, run["flightNodes"])
        self.assertEqual(2.0, run["pairedSpeedup"])

    def test_rejects_claimed_valid_result_that_fails_schema(self):
        """A self-declared valid artifact cannot enter the Pages index."""
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory)
            run_dir = results / "tpch-compare-q6-invalid"
            run_dir.mkdir()
            (run_dir / "compare.report.html").write_text(
                "report", encoding="utf-8"
            )
            (run_dir / "benchmark-result.json").write_text(
                json.dumps(
                    {
                        "schema_version": "2.0.0",
                        "artifact_type": "paired-comparison",
                        "validation": {"valid": True},
                        "comparison": {
                            "publication": {"state": "publishable"}
                        },
                    }
                ),
                encoding="utf-8",
            )

            run = BUILD_PAGES_SITE.load_compare_run(results, run_dir)

        self.assertIsNone(run)

    def test_copy_excludes_legacy_but_keeps_valid_diagnostics(self):
        """Only schema-valid machine artifacts enter the public file bundle."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "results"
            output = root / "pages"
            legacy = results / "legacy"
            diagnostic = results / "diagnostic"
            legacy.mkdir(parents=True)
            diagnostic.mkdir()
            (legacy / "old.summary.json").write_text(
                "{}", encoding="utf-8"
            )
            (diagnostic / "benchmark-result.json").write_text(
                "{}", encoding="utf-8"
            )
            (diagnostic / "compare.report.html").write_text(
                "report", encoding="utf-8"
            )
            machine_result = {
                "validation": {"valid": True},
                "comparison": {
                    "publication": {"state": "not-publishable"}
                },
            }

            with mock.patch.object(
                BUILD_PAGES_SITE,
                "read_valid_machine_result",
                side_effect=lambda path: (
                    machine_result if path.exists() else None
                ),
            ):
                BUILD_PAGES_SITE.copy_results(results, output)

            self.assertFalse((output / "benchmarks" / "legacy").exists())
            self.assertTrue(
                (output / "benchmarks" / "diagnostic").exists()
            )


if __name__ == "__main__":
    unittest.main()
