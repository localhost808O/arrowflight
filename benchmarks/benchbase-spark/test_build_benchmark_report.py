#!/usr/bin/env python3
"""Tests for the versioned benchmark interpretation report."""

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("build-benchmark-report.py")
SPEC = importlib.util.spec_from_file_location(
    "build_benchmark_report", MODULE_PATH
)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class BenchmarkReportTest(unittest.TestCase):
    """Validates conservative reporting and evidence links."""

    def test_empty_matrix_makes_no_performance_claim(self):
        """An empty valid matrix is explicit instead of using legacy data."""
        report = REPORT.build_report([])

        self.assertIn("makes no performance claim", report)
        self.assertIn("neither engine is declared faster", report)
        self.assertIn("BENCHBASE_CLUSTER_NODES=3", report)
        self.assertIn("comparison.publication.state=publishable", report)
        self.assertIn("github-hosted-runner-feasibility.md", report)

    def test_headline_values_link_to_aggregate_and_raw_repetitions(self):
        """Every displayed metric and raw repetition has an evidence URL."""
        artifact = {
            "run": {
                "id": "tpch-compare-q6-test",
                "source": {"git_sha": "a" * 40},
                "workload": {"query_set": "q6", "scale_factor": 1.0},
                "topology": {
                    "cluster_nodes": 3,
                    "host_resources": "8 vCPU, 32 GiB RAM",
                },
                "policy": {
                    "warmup_seconds": 30,
                    "measurement_seconds": 120,
                    "repetitions": 1,
                    "paired_observations": 3,
                    "terminals": 1,
                    "rate": "unlimited",
                    "starting_engine_order": ["flight", "direct"],
                },
                "configuration": {
                    "flight": {
                        "batch_size": 65536,
                        "duckdb_threads": "2",
                    },
                    "spark": {"sql_ansi_enabled": "true"},
                    "hadoop": {"block_size_bytes": 1073741824},
                },
                "runtime_dependencies": {
                    "benchbase": {"image_ref": "benchbase:test"},
                    "generator": {"image_ref": "generator:test"},
                },
            },
            "aggregate_summary": {
                "engines": {
                    "flight": {
                        "queries": {
                            "q6": {
                                "observation_median_latency_microseconds": {
                                    "median": 4000
                                }
                            }
                        }
                    },
                    "direct": {
                        "queries": {
                            "q6": {
                                "observation_median_latency_microseconds": {
                                    "median": 8000
                                }
                            }
                        }
                    },
                },
                "paired": {
                    "complete_pairs": 3,
                    "queries": {
                        "q6": {
                            "flight_to_direct_median_latency_ratio": {
                                "median": 0.5
                            }
                        }
                    },
                },
            },
            "observations": [
                {
                    "engines": [
                        {
                            "artifact_refs": {
                                "raw": "observations/observation-001/flight/raw.csv"
                            }
                        },
                        {
                            "artifact_refs": {
                                "raw": "observations/observation-001/direct/raw.csv"
                            }
                        },
                    ]
                }
            ],
        }

        report = REPORT.build_report([artifact])

        self.assertIn("[4.000 ms]", report)
        self.assertIn("[8.000 ms]", report)
        self.assertIn("[2.000×]", report)
        self.assertIn("aggregate-summary.json", report)
        self.assertIn(
            "observations/observation-001/flight/raw.csv", report
        )
        self.assertIn("Spark Direct at 0", report)


if __name__ == "__main__":
    unittest.main()
