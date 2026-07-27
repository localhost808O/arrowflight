#!/usr/bin/env python3
"""Tests for the GitHub-hosted benchmark feasibility measurement tool."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("measure-benchmark-feasibility.py")
SPEC = importlib.util.spec_from_file_location(
    "measure_benchmark_feasibility", MODULE_PATH
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MeasureBenchmarkFeasibilityTest(unittest.TestCase):
    """Verify byte parsing and multi-trial reliability aggregation."""

    def test_parse_size_supports_docker_units(self):
        """Convert decimal and binary Docker quantities to bytes."""
        self.assertEqual(MODULE.parse_size("1.5GiB"), 1610612736)
        self.assertEqual(MODULE.parse_size("12.5MB"), 12500000)
        self.assertEqual(MODULE.parse_pair("10MiB / 2GiB"), (10485760, 2147483648))
        self.assertEqual(MODULE.parse_size("not-a-size"), 0)

    def test_summarize_marks_three_stable_successes_reliable(self):
        """Accept three successful trials with low wall-time variance."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, wall_time in enumerate([100.0, 104.0, 98.0], start=1):
                trial = root / f"trial-{index}"
                trial.mkdir()
                payload = {
                    "label": f"trial-{index}",
                    "exit_code": 0,
                    "timed_out": False,
                    "wall_time_seconds": wall_time,
                    "usage": {
                        "peak_host_memory_used_bytes": 10,
                        "peak_docker_memory_bytes": 8,
                        "peak_disk_growth_bytes": 6,
                        "artifact_size_bytes": 4,
                    },
                }
                (trial / "feasibility-metrics.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )
            args = type(
                "Args",
                (),
                {"root": root, "output": root / "summary"},
            )()
            self.assertEqual(MODULE.summarize(args), 0)
            summary = json.loads(
                (root / "summary" / "feasibility-summary.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(summary["reliable_smoke"])
            self.assertEqual(summary["successful_trials"], 3)

    def test_summarize_exposes_failure_as_flakiness(self):
        """Reject a trial set containing a failed command."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, exit_code in enumerate([0, 1, 0], start=1):
                trial = root / f"trial-{index}"
                trial.mkdir()
                payload = {
                    "label": f"trial-{index}",
                    "exit_code": exit_code,
                    "timed_out": False,
                    "wall_time_seconds": 100.0,
                    "usage": {
                        "peak_host_memory_used_bytes": 10,
                        "peak_docker_memory_bytes": 8,
                        "peak_disk_growth_bytes": 6,
                        "artifact_size_bytes": 4,
                    },
                }
                (trial / "feasibility-metrics.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )
            args = type(
                "Args",
                (),
                {"root": root, "output": root / "summary"},
            )()
            MODULE.summarize(args)
            summary = json.loads(
                (root / "summary" / "feasibility-summary.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertFalse(summary["reliable_smoke"])
            self.assertAlmostEqual(summary["failure_rate"], 1 / 3)


if __name__ == "__main__":
    unittest.main()
