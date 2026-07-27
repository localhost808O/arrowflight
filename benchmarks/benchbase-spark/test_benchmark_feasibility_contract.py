#!/usr/bin/env python3
"""Tests for the GitHub-hosted benchmark feasibility policy."""

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DOCUMENT = REPO_ROOT / "docs" / "benchmarks" / "github-hosted-runner-feasibility.md"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-feasibility.yml"
PAGES_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-pages.yml"


class BenchmarkFeasibilityContractTest(unittest.TestCase):
    """Verify the documented runner decision remains enforceable."""

    def test_document_records_decision_metrics_and_handoff(self):
        """Require resource, retention, and publication policy sections."""
        text = DOCUMENT.read_text(encoding="utf-8")
        for required in [
            "NO-GO for performance measurements",
            "wall time",
            "peak Docker memory",
            "disk growth",
            "artifact",
            "failure rate",
            "Required pull-request checks",
            "External or self-hosted execution",
            "Heavy-run storage and retention",
            "Controlled handoff to Pages",
        ]:
            self.assertIn(required, text)

    def test_probe_is_manual_diagnostic_only(self):
        """Keep the Docker feasibility probe out of required and scheduled CI."""
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", text)
        self.assertNotIn("pull_request:", text)
        self.assertNotIn("schedule:", text)
        self.assertIn("matrix:\n        trial: [1, 2, 3]", text)
        self.assertIn('BENCHMARK_SCALE_FACTOR: "0.01"', text)
        self.assertIn("tpch compare q6", text)
        self.assertIn("retention-days: 14", text)

    def test_required_pages_check_does_not_execute_benchmark(self):
        """Prevent required Pages checks from making performance measurements."""
        text = PAGES_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotIn("run-benchbase-spark.sh", text)
        self.assertIn("validate-benchmark-publication.py", text)


if __name__ == "__main__":
    unittest.main()
