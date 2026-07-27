# GitHub-hosted benchmark feasibility

Status: **NO-GO for performance measurements on free GitHub-hosted runners**.
Status: **GO for required schema, report, and publication-contract checks**.

This decision covers the Spark/HDFS/ArrowFlight BenchBase topology in
`docker-compose.yml`. It prevents a short-lived shared runner from being
treated as a stable performance laboratory.

## Evidence and current decision

The successful
[Pages deployment run 30002799772](https://github.com/nsu-fit/ArrowFlight/actions/runs/30002799772)
is the only measured GitHub-hosted benchmark-related activity currently
published. Its log records Ubuntu 24.04 and a wall interval from
`2026-07-23T11:21:13.0909693Z` to `2026-07-23T11:21:20.5617480Z`, or
approximately **7.47 seconds**. It checked two files and deployed a committed
bundle; it did not start Docker, HDFS, Spark, Flight, or BenchBase. The log did
not sample memory or disk, so none is inferred.

As of 2026-07-27, GitHub documents the public-repository `ubuntu-latest`
standard runner as 4 vCPU, 16 GiB RAM, and 14 GiB SSD, with a six-hour
GitHub-hosted job limit:

- [GitHub-hosted runner hardware](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
- [GitHub Actions limits](https://docs.github.com/en/actions/reference/limits)

The full Compose topology can start HDFS, Spark, one to ten combined
Flight/DataNode/Spark workers, a Spark publisher, Spark Thrift, BenchBase, a
data generator, and optional observability. Every selected worker alone is
configured with a 2 GiB Spark worker allocation and a 2 GiB JVM maximum.
Image layers, build caches, HDFS data, Spark/DuckDB working data, and the
remaining JVMs must also fit on the same 14 GiB disk and 16 GiB host.

No end-to-end Docker smoke measurement has yet been produced on
`ubuntu-latest`. Absence of a measurement is a failed feasibility gate, not
evidence that the stack fits. Therefore:

| Activity | Current evidence | Decision |
|---|---|---|
| Python schema/report/Pages tests | Required workflow executes them without Docker | GO as a required PR check |
| Publish a prevalidated committed Pages bundle | 7.47-second successful historical job | GO after semantic validation |
| One-node SF 0.01 Q6 Flight-vs-Direct probe | Exact manual workflow exists; no completed evidence artifact yet | NO-GO for required CI; manual diagnostic only |
| Latency or throughput regression gate | No runner-noise or repeatability evidence | NO-GO |
| Publishable Q1/Q6/Q14 matrix, SF 0.1/1, 1/3/8 nodes | Exceeds the tested topology and has no hosted-runner measurements | NO-GO; dedicated external/self-hosted execution required |

## Exact diagnostic smoke probe

The manually dispatched
[`Benchmark Runner Feasibility`](../../.github/workflows/benchmark-feasibility.yml)
workflow runs three independent `ubuntu-latest` trials, sequentially. Each
fresh runner executes exactly:

```bash
BENCHBASE_RESULTS_ID="github-hosted-smoke-<run>-<attempt>-<trial>" \
BENCHMARK_SCALE_FACTOR=0.01 \
BENCHBASE_CLUSTER_NODES=1 \
BENCHBASE_PAIRED_OBSERVATIONS=1 \
BENCHBASE_COMPARE_ORDER=flight-first \
BENCHBASE_CACHE_POLICY=warm-cache \
BENCHBASE_TIME_SECONDS=10 \
BENCHBASE_WARMUP_SECONDS=0 \
BENCHBASE_TERMINALS=1 \
BENCHBASE_UPDATE_PAGES=false \
BENCHBASE_CAPTURE_TIMEOUT_SECONDS=120 \
BENCHMARK_OBSERVABILITY=false \
DIRECT_PARQUET_PARTITIONS=1 \
UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu \
bash benchmarks/benchbase-spark/run-benchbase-spark.sh tpch compare q6
```

Run it from Actions → Benchmark Runner Feasibility → Run workflow. It is
`workflow_dispatch` only: it is not a required PR check and has no schedule.
Every trial has a 60-minute command timeout and a 75-minute job timeout.

[`measure-benchmark-feasibility.py`](../../benchmarks/benchbase-spark/measure-benchmark-feasibility.py)
samples and preserves:

- command wall time, exit code, timeout, and sample count;
- runner and Docker CPU/memory limits;
- baseline, peak, and incremental host memory;
- aggregate peak Docker memory across live containers and container count;
- initial, peak, and final disk use plus peak disk growth;
- Docker block read/write counters and `docker system df`;
- result-artifact byte size and full benchmark/Docker logs.

The summary reports failure rate and wall-time coefficient of variation across
the three fresh runners. Its reliability gate requires all three trials to
succeed, no timeout, and wall-time CV at or below 15%. Passing this gate means
only that the smoke path may be considered technically repeatable. It does
not authorize latency/throughput comparisons or a performance regression
threshold.

To update this document after a dispatch, copy the workflow URL and the
values from `benchmark-feasibility-summary/feasibility-summary.json` into the
evidence table. Do not promote a single successful trial.

## CI and execution split

### Required pull-request checks

`benchmark-pages.yml` runs the Python contract tests, regenerates the report
and Pages bundle, validates every published artifact, and compares generated
files byte-for-byte. It never invokes `run-benchbase-spark.sh`. The normal
Java build/integration checks remain in `ci.yml`.

These checks may reject malformed, invalid, stale, or non-reproducible
benchmark artifacts. They make no claim about performance regression,
throughput, latency, Docker capacity, or production readiness.

### Manual GitHub-hosted execution

Only the diagnostic three-trial SF 0.01 Q6 probe above is selected. Its
purpose is to measure runner feasibility and flakiness. Results remain
`diagnostic`/`not-publishable`, including when all trials succeed.

Do not increase scale factor, node count, query count, warmup, measurement
duration, or paired observations in this workflow. Such a change requires a
new feasibility decision based on three fresh trials.

### Scheduled execution

No Spark/HDFS/Flight benchmark is scheduled on GitHub-hosted runners.
Scheduled measurements are allowed only on a dedicated, labelled self-hosted
runner or an externally managed benchmark host. The operator must prevent
concurrent workloads and preserve the same pinned environment across the
matrix.

### External or self-hosted execution

The publishable matrix for Q1/Q6/Q14 at SF 0.1 and 1 with 1, 3, and 8 nodes
requires a dedicated Linux Docker host. Minimum provisioning for the current
single-host Compose design is:

- 16 dedicated x86-64 vCPU, 64 GiB RAM, and 200 GiB free SSD/NVMe;
- recommended 32 vCPU, 128 GiB RAM, and 500 GiB free NVMe for the eight-node
  points and retained Docker/HDFS data;
- Ubuntu 22.04 or newer, Java 21, Docker Engine with Compose v2, Python 3,
  stable local storage, and no concurrent CI jobs;
- fixed/pinned image IDs and dependency versions, recorded host resources,
  and a clean Git worktree.

These are admission requirements, not a claim that the matrix will fit or be
stable. The first complete run must still record resource use and variance.
If a self-hosted Actions runner is used, assign a dedicated label such as
`benchmark-publishable`; never fall back from that label to `ubuntu-latest`.

## Heavy-run storage and retention

The GitHub-hosted diagnostic workflow uploads each trial and its aggregate
summary with `retention-days: 14`. GitHub permits repository artifact
retention to be configured within its documented limits, but Actions
artifacts are temporary evidence, not the source of record:

- [Artifact and log retention](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository#configuring-the-retention-period-for-github-actions-artifacts-and-logs-in-your-repository)

Heavy external runs use this storage path:

1. Write each immutable run directory and `benchmark-result.json` to local
   NVMe during execution.
2. Validate the v2 artifact before transfer.
3. Create a compressed archive and SHA-256 checksum.
4. Upload the archive, checksum, runner logs, and environment inventory to
   versioned object storage or an immutable release with at least 365 days of
   retention. Keep failed/diagnostic runs for at least 30 days.
5. Record the object URI and checksum in the curation PR. The external archive
   remains the raw source of record.

Example validation and packaging:

```bash
run_dir="benchmarks/benchbase-spark/results/<run-id>"
python benchmarks/benchbase-spark/benchmark-result-schema.py \
  validate "${run_dir}/benchmark-result.json"
tar -C "$(dirname "${run_dir}")" -czf "<run-id>.tar.gz" "$(basename "${run_dir}")"
sha256sum "<run-id>.tar.gz" > "<run-id>.tar.gz.sha256"
```

## Controlled handoff to Pages

Benchmark execution never writes directly to `gh-pages`.

1. A benchmark operator provides the immutable archive, SHA-256, source SHA,
   and external storage URI.
2. A curator verifies the checksum in a clean checkout, extracts to staging,
   validates the schema, and confirms
   `comparison.publication.state=publishable`.
3. The curator checks correctness, both engine orders, all raw repetitions,
   plans, execution-path evidence, dependency pins, and clean source state.
4. Only selected evidence directories are copied into the Pages input and the
   report/Pages generators are run.
5. A normal pull request carries the versioned artifacts, regenerated report,
   and Pages indices. Required PR checks validate semantics and deterministic
   generation without rerunning the benchmark.
6. After review and merge, `benchmark-pages.yml` replaces the benchmark
   artifact subtree and publishes the validated dashboard.

The publication validator excludes invalid and unversioned legacy data.
Workflow artifacts, external archives, and exploratory results cannot bypass
the curation pull request.

## Revisit conditions

The GitHub-hosted decision may change only when a linked three-trial summary
records all of the following:

- zero failures and timeouts;
- wall-time CV at or below 15%;
- peak memory and disk use with at least 25% runner headroom;
- complete schema-valid diagnostic artifacts within the 60-minute command
  limit;
- artifact size low enough for the 14-day upload path.

Even then, enabling a performance-regression PR check requires a separate
noise study across multiple days and runner allocations, a justified
threshold, and a false-positive budget. The smoke feasibility result alone is
insufficient.
