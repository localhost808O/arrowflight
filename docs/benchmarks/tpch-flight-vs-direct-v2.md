# TPC-H Spark Direct vs ArrowFlight report v2

This is the curated reproducibility and interpretation report for the
ArrowFlight benchmark matrix. It is generated only from valid, publishable
`paired-comparison` artifacts using schema `2.0.0`. The
[JSON Schema](../../benchmarks/benchbase-spark/schema/benchmark-result-v2.schema.json),
[contract guide](../../benchmarks/benchbase-spark/RESULT_SCHEMA.md), and
[benchmark harness](../../benchmarks/benchbase-spark/run-benchbase-spark.sh)
are part of the report contract. The
[GitHub-hosted runner feasibility decision](github-hosted-runner-feasibility.md)
defines which checks may run in required CI and why performance measurements
require dedicated infrastructure.

## Published matrix

No schema-v2 publishable matrix points are committed at this revision. Consequently this report makes no performance claim. The command below is the acceptance path for the first point.

Latency is the median of equally weighted per-observation query medians.
Paired speedup is `Direct latency / Flight latency`; values above 1 mean
Flight is faster. Throughput is retained in the raw aggregate but is not used
as a headline for serial query points. Lower latency is better.

## Per-query charts

Charts are intentionally absent until a schema-v2 publishable point exists; rendering diagnostic or legacy values here would violate the publication contract.

## Interpretation

There is no valid published evidence for Q1, Q6, or Q14 at this revision, so neither engine is declared faster.

These are query-specific synthetic TPC-H results, not a statement of
production readiness. Spark Direct can win when Flight transport, planning,
or client-side merge overhead exceeds the saved scan/aggregation work. Flight
can win when recorded execution-path evidence shows effective pushdown and
reduced data transfer. A result without correct answers, both formatted
physical plans, resolved runtime pins, or three complete alternating pairs is
diagnostic rather than evidence.

## Reproduce one matrix point

From a clean Linux checkout with Java 21, Docker, Compose, and
Python 3:

```bash
git clone https://github.com/nsu-fit/ArrowFlight.git
cd ArrowFlight
git checkout "$(git log -n1 --format=%H -- docs/benchmarks/tpch-flight-vs-direct-v2.md)"
test -z "$(git status --porcelain)"

BENCHBASE_CLUSTER_NODES=3 \
BENCHBASE_PAIRED_OBSERVATIONS=3 \
BENCHBASE_HOST_RESOURCES="8 vCPU, 32 GiB RAM, Spark workers=3" \
BENCHBASE_WARMUP_SECONDS=30 \
BENCHBASE_TIME_SECONDS=120 \
BENCHBASE_QUERY_REPETITIONS=1 \
BENCHBASE_TERMINALS=1 \
BENCHBASE_RATE=unlimited \
BENCHBASE_COMPARE_ORDER=flight-first \
BENCHBASE_CACHE_POLICY=warm-cache \
BENCHBASE_IMAGE="benchbase.azurecr.io/benchbase:latest" \
BENCHMARK_GENERATOR_IMAGE="arrowflight-duckdb-benchmark-generator:latest" \
BENCHMARK_SCALE_FACTOR=1 \
HDFS_BLOCK_SIZE_BYTES=1073741824 \
SPARK_SQL_ANSI_ENABLED=true \
FLIGHT_BATCH_SIZE=65536 \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q1
```

The run is publishable only if `benchmark-result.json` reports
`validation.valid=true` and `comparison.publication.state=publishable`.
Container content IDs, dependency versions, source state, configuration,
dataset digest, plans, execution paths, correctness, and every raw repetition
are recorded in that artifact.

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

No raw repetitions are referenced because no point is published.

## Regeneration

```bash
python benchmarks/benchbase-spark/build-benchmark-report.py
python benchmarks/benchbase-spark/build-pages-site.py
python benchmarks/benchbase-spark/validate-benchmark-publication.py
```
