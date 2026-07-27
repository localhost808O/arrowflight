# Benchmark result schema

New paired benchmark runs use schema version `2.0.0`, defined by
[`schema/benchmark-result-v2.schema.json`](schema/benchmark-result-v2.schema.json).
Execution infrastructure and CI eligibility are governed separately by the
[GitHub-hosted runner feasibility decision](../../docs/benchmarks/github-hosted-runner-feasibility.md).
A schema-valid diagnostic smoke result is not performance evidence.

The contract covers four artifact types:

- `run.json` records source SHA and dirty state, workload policy, resolved
  container IDs and library pins, Spark/Hadoop/JVM/Flight/DuckDB settings,
  topology, the deterministic engine-order schedule, timestamps, and terminal
  status.
- `observations/observation-NNN/<engine>/engine-result.json` retains every
  ordered engine outcome, raw measured transaction, serial repetition index,
  correctness result, plan, failure reason, validity state, and raw artifact
  link.
- `aggregate-summary.json` stores per-observation values, equal-weight
  observation aggregates, and links back to every raw repetition CSV.
- `benchmark-result.json` is the complete paired comparison, with both engine
  outcomes for every observation, dataset manifest, logical query digests,
  per-observation engine order, aggregate summary, and schema validation state.

## Paired measurement policy

A comparison defaults to one paired observation. Set
`BENCHBASE_PAIRED_OBSERVATIONS=3` or higher for a publishable comparison, which
requires at least three complete pairs. The first pair follows
`BENCHBASE_COMPARE_ORDER`, and later pairs alternate automatically, so both
`flight → direct` and `direct → flight` occur in one publishable invocation.

The supported cache policy is `warm-cache`, recorded through
`BENCHBASE_CACHE_POLICY`. The stack is prepared once and is not reset or
evicted between engine runs or observations. A timed workload still applies
`BENCHBASE_WARMUP_SECONDS` independently to every engine run. This combination
uses explicit warm-cache measurements while alternating order to distribute
JVM, Spark code generation, HDFS, and OS cache effects.

Each observation records its start and finish timestamps, warmup, cache policy,
engine order, engine exit codes, and failures in `observation-context.json`.
A failed engine does not stop the remaining engine or later pairs. Failed and
partial observations remain in the final artifact, while publication requires
three valid complete pairs and both engine orders.

Latency aggregates use each successful observation's median as one equally
weighted input. Reports include every observation and the median, minimum,
maximum, spread, p25, p75, IQR, and p95 across observation values. Paired
Flight/Direct ratios and differences are calculated only for complete pairs.
Per-query aggregates follow the same observation-level method.

Runtime execution paths use only these stable labels: `footer-count`,
`footer-stats`, `duckdb-scan`, `duckdb-aggregation`, `duckdb-join`,
`distributed`, `mixed`, `fallback`, and `unknown`. A uniform path spanning
multiple nodes is classified as `distributed` and keeps its concrete
`uniform_path`; differing node paths are `mixed`. `fallback` and `unknown`
never set `pushdown_evidence` to true.

The harness captures `run-context.json` before execution, resolves runtime
container IDs after the images have been built, preserves every raw
observation, and validates all four artifact types against the actual Draft
2020-12 JSON Schema before updating the publishable dashboard. Validate an
artifact manually with:

```bash
python benchmarks/benchbase-spark/benchmark-result-schema.py validate \
  benchmarks/benchbase-spark/results/<run>/benchmark-result.json
```

Breaking contract changes require a new schema file and a new
`schema_version`. Version `1.0.0` remains available for validation, while all
new artifacts use `2.0.0`.

Schema validity and publication eligibility are deliberately separate. A
failed, dirty, or partially captured run can still be a valid v2 document so
automation can diagnose it. Publication additionally requires a clean
40-character source SHA, resolved content-addressed BenchBase and generator
image IDs, a dataset manifest and digest, SQL and formatted physical plans for
both engines, recorded host resources, three complete correct pairs, and both
engine orders. Each rejection is retained in
`comparison.publication.reasons`.

## Curated publication matrix

The primary Pages index contains only schema-valid, publishable paired runs for
TPC-H Q1, Q6, or Q14 at scale factor 0.1 or 1 and with 1, 3, or 8 Flight
nodes. Set `BENCHBASE_HOST_RESOURCES` to a concise host description, for
example `8 vCPU, 32 GiB RAM, Spark workers=2`; a matrix point without recorded
resources remains exploratory. Schema-valid all-query, smoke, diagnostic, and
other non-matrix runs are written to `exploratory.html` and
`exploratory-benchmarks.json`. Invalid or unversioned legacy artifacts are not
copied into the deployed Pages bundle.

The curated table identifies query, scale, node/resources topology, cache
policy, warmup, deterministic order schedule, paired-observation count, sample
count, per-engine throughput and latency, and paired latency speedup. Speedup
is `Direct latency / Flight latency`, so values above 1 mean Flight is faster.
Latency charts state explicitly that lower values are faster and report a
separate winner for every query.
