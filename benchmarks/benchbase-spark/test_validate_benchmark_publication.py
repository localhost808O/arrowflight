#!/usr/bin/env python3
"""Tests for benchmark publication semantic validation."""

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name(
    "validate-benchmark-publication.py"
)
SPEC = importlib.util.spec_from_file_location(
    "validate_benchmark_publication", MODULE_PATH
)
PUBLICATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLICATION)


class BenchmarkPublicationTest(unittest.TestCase):
    """Validates the empty-matrix publication contract."""

    def setUp(self):
        """Create a minimal committed Pages fixture."""
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.pages = root / "pages"
        self.pages.mkdir()
        self.report = root / "report.md"
        (self.pages / "benchmarks.json").write_text(
            "[]", encoding="utf-8"
        )
        (self.pages / "exploratory-benchmarks.json").write_text(
            "[]", encoding="utf-8"
        )
        (self.pages / "index.html").write_text(
            PUBLICATION.PAGES_BUILDER.REPORT_URL
            + " schemas/benchmark-result-v2.schema.json",
            encoding="utf-8",
        )
        schemas = self.pages / "schemas"
        schemas.mkdir()
        source_schema = PUBLICATION.RESULT_SCHEMA.schema_path_for("2.0.0")
        (schemas / source_schema.name).write_bytes(
            source_schema.read_bytes()
        )
        self.report.write_text(
            "# Benchmark report v2\n\n"
            "The empty matrix makes no performance claim.\n",
            encoding="utf-8",
        )

    def tearDown(self):
        """Remove the temporary publication fixture."""
        self.temp.cleanup()

    def test_accepts_explicit_empty_matrix(self):
        """An empty curated index is valid when the report is explicit."""
        result = PUBLICATION.validate_bundle(self.pages, self.report)

        self.assertEqual(0, result["curated_runs"])
        self.assertEqual("2.0.0", result["schema_version"])

    def test_rejects_empty_matrix_with_performance_implication(self):
        """An empty matrix cannot omit the no-claim disclosure."""
        self.report.write_text(
            "# Benchmark report v2\n", encoding="utf-8"
        )

        with self.assertRaisesRegex(
            ValueError, "must explicitly make no performance claim"
        ):
            PUBLICATION.validate_bundle(self.pages, self.report)


if __name__ == "__main__":
    unittest.main()
