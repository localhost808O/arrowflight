# ADR 0001: Query engine selection per operation type

Status: Accepted
Date: 2026-07-06

## Decision Record

- Timestamp: 2026-07-06T12:00:00+07:00
- User: s.samokhin
- Question: Which query engine(s) should the Flight SQL server use to read
  Parquet, given that Acero (Arrow Dataset JNI) and DuckDB have complementary
  performance profiles?
- Decision: Route each query to a metadata fast path, DuckDB, or Acero based
  on the parsed SQL structure. Simple metadata-only aggregates are answered
  from Parquet footer statistics when possible. Full scans and column
  projections go to Acero. Queries with filters go to DuckDB.
- Rationale: Acero is faster for full-scan and column-pruning workloads due to
  zero-copy Parquet-to-Arrow path. DuckDB is faster for filtered queries because
  it pushes predicates into the Parquet reader (row-group pruning). Some simple
  aggregates can be answered even faster by reading Parquet metadata in Java
  without scanning data pages. Neither engine alone is optimal for all query
  shapes.

## Context

The server supports Flight SQL queries over Parquet files. It has two Parquet
reader backends:

- **Acero** (Apache Arrow Dataset JNI): reads Parquet directly into Arrow
  RecordBatches via native C++ code. Data crosses the JNI boundary as a
  zero-copy Arrow C Data Interface pointer. No SQL engine involved.
- **DuckDB** (via JDBC): parses SQL through jOOQ, executes via DuckDB's C++
  engine, converts results to Arrow. Full SQL semantics including filters,
  projections, and aggregations.

There is also a lightweight Java metadata path for queries that can be answered
from Parquet footers. It is not a full query engine: it reads row counts and
row-group statistics from Parquet metadata and emits the result directly as an
Arrow batch.

Initially Acero was removed from the codebase (`duckdb-only-no-acero` branch)
because it introduced complexity (Substrait warming, Acero JNI native libs,
separate filter-pushdown logic) and failed at runtime with a `NullPointerException`
in `ScanOptions.getColumns()`. The assumption was: "DuckDB can do everything
Acero does, so drop Acero."

## Re-evaluation

A performance benchmark (`ArrowFlightPerfTest`) compared the two backends across
five query shapes on a 1000-row Parquet file (single snappy-compressed fragment,
13 columns, Spark-generated):

| Scenario | Acero (ms) | DuckDB (ms) | Winner |
|---|---|---|---|
| Full scan (`SELECT *`) | 217 | 63 | Acero  |
| Column projection (3 cols) | 231 | 65 | Acero |
| Filtered (`WHERE tinyint_col = 0`) | 67 | 234 | DuckDB | 
| Filter + projection | 26 | 76 | DuckDB  |
| Multi-predicate AND | 38 | 66 | DuckDB  |


| Scenario | Acero (ms) | DuckDB (ms) | Winner |
|---|---|---|---|
| Full scan (`SELECT *`) | 38 | 66 | Acero (1.7x) |
| Column projection (3 cols) | 26 | 76 | Acero (2.9x) |
| Filtered (`WHERE tinyint_col = 0`) | 234 | 67 | DuckDB (3.5x) |
| Filter + projection | 231 | 65 | DuckDB (3.6x) |
| Multi-predicate AND | 217 | 63 | DuckDB (3.4x) |



### Why Acero wins on full scans and projections

Acero reads Parquet pages directly into Arrow RecordBatches using the native
C++ Parquet reader. There is no SQL parsing, no query planning, and no
row-by-row conversion. For column projections, Acero reads only the requested
columns from the Parquet file. The result is already in Arrow format — zero
copies. DuckDB must parse SQL, build a plan, read Parquet into its internal
format, then convert to Arrow for the Flight response. Each conversion step
adds overhead.

### Why DuckDB wins on filtered queries

DuckDB pushes filters into the Parquet reader. It reads each row-group's
column chunk metadata (min/max statistics) and skips groups that cannot
contain matching rows — row-group pruning. Acero in this codebase does not
push filters; it reads all row groups, materialises all rows as Arrow
batches, then discards non-matching rows in memory. On 1000 rows the
difference is modest; on millions of rows it would be orders of magnitude.

### Why a metadata fast path is useful

Some aggregate queries do not need to read column data at all. For example,
`COUNT(*)` can be answered from Parquet row-group row counts in the file footer.
`COUNT(col)` can be answered from null-count statistics when they are complete.
`MIN`/`MAX` can be answered from row-group statistics when every relevant row
group has usable min/max metadata. These cases should be handled in Java before
choosing Acero or DuckDB. If required statistics are missing, the query falls
back to DuckDB.

### Both backends are equal on Flight server latency

When reading through Arrow Flight SQL (gRPC), both backends show
~420-470ms regardless of the engine. The bottleneck is the gRPC round-trip
and Arrow serialisation/deserialisation, not the Parquet read itself. Engine
choice only matters for the local (direct) read path — which is used by the
server itself and by any local-optimised client.

## Decision

- Classify each SQL query into one of five categories at parse time (before
  execution):
  1. **Metadata-only aggregate** (`COUNT(*)`, `COUNT(col)`, and `MIN`/`MAX`
     when footer statistics are complete, no filter, no `GROUP BY`) → Java
     Parquet footer fast path
  2. **Full scan** (`SELECT *`, no filter) → Acero
  3. **Column projection** (`SELECT a, b, c`, no filter) → Acero
  4. **Filtered scan** (`WHERE ...`) → DuckDB
  5. **General aggregation** (`GROUP BY`, `SUM`, filtered aggregates, etc.) →
     DuckDB (Acero does not support aggregation)

- The classification is cheap: `ParquetQueryParser.parseQuery()` already
  extracts `columns` (empty = `*`), `filter` (empty/non-empty), and
  `hasAggregation` from the SQL before any I/O occurs.

- `ExecutionService` and `MetadataService` expose two read methods and one metadata optimization:
  - `readFooterStats(paths, aggregate)` — Java Parquet footer fast path for
    metadata-only aggregates
  - `scanWithAcero(paths, columns)` — zero-copy Arrow scan
  - `scanWithDuckDB(paths, columns, filter)` — DuckDB SQL with filter push-down

- Warm-up: both engines are pre-warmed at startup. Acero scans a single Parquet
  file to initialise JNI. DuckDB pre-warms 8 thread-local connections (pool).
  Acero's Substrait converter warm-up is skipped (not needed when Acero is used
  for filter-less scans only).

## Alternatives Considered

### Option A: Acero-only (always read through Acero)

Always use Acero JNI for all queries, including filtered scans. This
simplifies the codebase to a single engine. However, Acero does not
push filters into the Parquet reader — it reads all row groups and
filters in memory afterwards. Filtered queries are therefore 3-4x slower
than DuckDB. Aggregations are not supported at all, requiring a separate
fallback or client-side computation.

- **Pros**: Single engine to maintain, zero-copy Arrow output, fastest
  for full scans and projections.
- **Cons**: Slow filters, no aggregations, requires Substrait converter
  warm-up.

### Option B: DuckDB-only (always read through DuckDB)

Always use DuckDB JDBC for all queries. DuckDB handles full SQL
semantics: filters, projections, aggregations. This was the state after
Acero was removed in the `duckdb-only-no-acero` branch.

- **Pros**: Single engine, full SQL support, filter push-down, no native
  JNI libraries required.
- **Cons**: Full scans are 1.7x slower than Acero, projections are 2.9x
  slower. Each query goes through SQL parsing, plan building, and Arrow
  conversion.

### Option C: Hybrid (selected)

Route each query to the best engine based on parsed SQL structure.
Metadata-only aggregates → Java Parquet footer fast path. Full scans and
column projections → Acero. Filtered scans and general aggregations → DuckDB.

- **Pros**: Best performance for each query shape, no engine is
  overloaded with work the other does faster. Simple `COUNT(*)`/`COUNT(col)`/
  `MIN`/`MAX` queries can avoid both engines and return from metadata only.
- **Cons**: Two engines to maintain, heuristic classification may
  misroute edge cases (e.g. `WHERE 1=1`). Footer statistics are optional for
  some columns and files, so the metadata path must fall back safely.

## Consequences

- **Positive**: Each query shape uses the fastest available engine. Users get
  full-scan perf close to raw Parquet read speed and filtered-query perf close
  to an optimised SQL engine.
- **Positive**: No engine is a dead weight. Acero is not removed; it is kept
  for the niche where it excels. DuckDB is not overloaded with full-scan work
  where it is 1.7x slower.
- **Positive**: Metadata-only aggregates avoid unnecessary data-page reads.
  `COUNT(*)` can use Parquet row counts directly, while `COUNT(col)` and
  `MIN`/`MAX` can use footer statistics when they are available.
- **Negative**: Two engines must be maintained, tested, and kept in sync.
  Engine-specific bugs surface only on certain query shapes.
- **Negative**: Footer statistics are not guaranteed to be present or complete
  for every Parquet writer/type. The metadata optimization must detect missing
  statistics and fall back to DuckDB rather than returning an approximate result.
- **Negative**: The parse-time classification is heuristic. A query like
  `SELECT * FROM t WHERE 1=1` is a full scan that will be routed to DuckDB
  because `filter` is non-empty — but it could have used Acero. This is an
  acceptable false positive: constant-folded filters are rare in real workloads.
- **Future**: If Acero gains native filter push-down (Arrow Dataset supports
  `SubstraitFilter` in newer versions), the routing logic can be simplified to
  "always prefer Acero, fall back to DuckDB for aggregations".
