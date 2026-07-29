package net.surpin.data.arrowflight.server;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.adapter.arrow.ArrowFieldTypeFactory;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Управление Parquet файлами с учётом локальности данных.
 */
public final class ParquetManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetManager.class);

    // Daemon thread pool for parallel per-file I/O (footer reads + Acero scans).
    private static final int IO_PARALLELISM =
            Integer.getInteger("arrowflight.io.parallelism",
                    Math.max(32, Runtime.getRuntime().availableProcessors() * 8));
    private static final java.util.concurrent.ExecutorService IO_POOL =
            java.util.concurrent.Executors.newFixedThreadPool(IO_PARALLELISM, r -> {
                Thread t = new Thread(r, "parquet-io");
                t.setDaemon(true);
                return t;
            });

    // Number of parallel DuckDB tasks for GROUP BY / aggregation queries.
    // Each task processes a group of files via UNION ALL in one DuckDB query, reducing
    // the per-task native-init overhead versus one DuckDB call per file.
    // Configurable via -Darrowflight.duckdb.groups=N.
    private static final int DUCKDB_GROUPS =
            Integer.getInteger("arrowflight.duckdb.groups", Math.min(8, IO_PARALLELISM));

    // One reusable DuckDB connection per IO-pool thread. DuckDB native init is expensive;
    // reusing the connection across files within the same query avoids repeated startup cost.
    private static final ThreadLocal<Connection> DUCKDB_THREAD_CONN = ThreadLocal.withInitial(() -> {
        try {
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            try (java.sql.Statement s = conn.createStatement()) {
                // Prevent DuckDB's C++ worker threads from calling Arrow JNI get_next callbacks
                // from threads not attached to the JVM, which causes SIGSEGV on GROUP BY queries.
                s.execute("SET threads = 1");
            }
            return conn;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create thread-local DuckDB connection", e);
        }
    });


    private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private final FileSystem fileSystem;
    private final String dataDirectory;
    private final String localhost;

    private Map<String, Path> tableSchemaCache;
    private Map<String, Map<String, Path>> tableCache;
    private Map<String, Map<String, String>> tableDdlCache = new HashMap<>();
    private CalciteCatalogReader catalogReader;

    public ParquetManager(FileSystem fileSystem, String dataDirectory, String localhost) {
        this.fileSystem = fileSystem;
        this.dataDirectory = dataDirectory;
        this.localhost = localhost;

        try {
            initSchemaCache();
            initTableCache();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        initCatalogReader();
    }

    public Map<String, Path> getSchemas(String filterExpression) throws IOException {
        Predicate<String> schemaPredicate = MetadataUtils.createLikePredicate(filterExpression);
        return tableSchemaCache.entrySet().stream()
                .filter(entry -> filterExpression == null || schemaPredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void initSchemaCache() throws IOException {
        LOGGER.info("Initializing schema cache for data directory: {}", dataDirectory);
        Path dirPath = new Path(dataDirectory);

        if (!fileSystem.exists(dirPath)) {
            tableSchemaCache = Collections.emptyMap();
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return;
        }

        if (tableSchemaCache == null) {
            tableSchemaCache = Arrays.stream(fileSystem.listStatus(dirPath))
                    .filter(FileStatus::isDirectory)
                    .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));
        }

        LOGGER.info("Collected schemas: {}", tableSchemaCache);
    }

    public Map<String, Path> getTables(String schema, String filterExpression) throws IOException {
        Map<String, Path> tables = tableCache.getOrDefault(schema, Collections.emptyMap());
        Predicate<String> tablePredicate = MetadataUtils.createLikePredicate(filterExpression);
        return tables.entrySet().stream()
                .filter(entry -> filterExpression == null || tablePredicate.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void initTableCache() throws IOException {
        LOGGER.info("Initializing table cache for data directory: {}", dataDirectory);
        Path schemaPath = new Path(dataDirectory);

        if (!fileSystem.exists(schemaPath)) {
            tableCache = Collections.emptyMap();
            LOGGER.info("Data directory does not exist: {}", dataDirectory);
            return;
        }

        if (tableCache == null) {
            tableCache = new HashMap<>();
            getSchemas(null).forEach((key, value) -> {
                LOGGER.info("Collecting tables for schema: {} at path {}", key, value);
                try {
                    Map<String, Path> tables = Arrays.stream(fileSystem.listStatus(value))
                            .filter(FileStatus::isDirectory)
                            .collect(Collectors.toMap(status -> status.getPath().getName(), FileStatus::getPath));
                    tableCache.put(key, tables);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        LOGGER.info("Collected tables: {}", tableCache);
    }

    public Schema getTableSchema(String schema, String table) {
        return getTableSchema(schema, table, null);
    }

    public Schema getTableSchema(String schema, String table, List<String> columns) {
        try {
            Path tableDirectoryPath = new Path(dataDirectory, schema + "/" + table);
            RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(tableDirectoryPath, true);

            Path parquetPath = null;
            while (it.hasNext()) {
                LocatedFileStatus lfs = it.next();
                if (lfs.isFile() && lfs.getPath().getName().endsWith(".parquet")) {
                    parquetPath = lfs.getPath();
                    break;
                }
            }

            if (parquetPath == null) return new Schema(Collections.emptyList(), null);

            MessageType parquetSchema;
            final long fileLen = fileSystem.getFileStatus(parquetPath).getLen();
            final Path finalPath = parquetPath;
            try (ParquetFileReader reader = ParquetFileReader.open(new org.apache.parquet.io.InputFile() {
                @Override public long getLength() { return fileLen; }
                @Override public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                    return org.apache.parquet.hadoop.util.HadoopStreams.wrap(fileSystem.open(finalPath));
                }
            })) {
                parquetSchema = reader.getFooter().getFileMetaData().getSchema();
            }

            return ParquetSchemaConverter.convert(
                    parquetSchema,
                    cd -> columns == null || columns.isEmpty()
                            || cd.getPath().length == 1 && columns.contains(cd.getPath()[0])
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void initCatalogReader() {
        Objects.requireNonNull(tableSchemaCache, "Initialize schema cache first");
        Objects.requireNonNull(tableCache, "Initialize table cache first");

        StringBuilder ddlBuilder = new StringBuilder();
        tableCache.forEach((schemaName, tablesMap) -> {
            tableDdlCache.putIfAbsent(schemaName, new HashMap<>());
            tablesMap.forEach((tableName, path) -> {
                Schema schema = getTableSchema(schemaName, tableName);
                String ddl = arrowSchemaToDDL(schemaName, tableName, schema);
                tableDdlCache.get(schemaName).put(tableName, ddl);
                ddlBuilder.append(ddl).append(";\n");
            });
        });

        LOGGER.info("Parsed DDL: {}", ddlBuilder.toString());

        try {
            catalogReader = SubstraitCreateStatementParser.processCreateStatementsToCatalog(ddlBuilder.toString());
        } catch (SqlParseException e) {
            throw new IllegalStateException("Can't parse schemas", e);
        }

        if (ddlBuilder.length() == 0) return;

        // ── 1. Substrait / Calcite warm-up ───────────────────────────────────────
        List<String> strippedDdls = new ArrayList<>();
        tableDdlCache.forEach((schemaName, tableMap) ->
                tableMap.forEach((tableName, ddl) ->
                        strippedDdls.add(ddl.replace(schemaName + ".", ""))));
        LOGGER.info("Pre-warming Substrait converter ({} DDL(s))...", strippedDdls.size());
        long t0 = System.currentTimeMillis();
        SubstraitFilterConverter.warmUp(strippedDdls);
        LOGGER.info("Substrait warm-up done in {}ms", System.currentTimeMillis() - t0);

        // ── 2. Acero JNI warm-up ─────────────────────────────────────────────────
        findFirstParquetUri().ifPresent(uri -> {
            LOGGER.info("Pre-warming Acero JNI (file: {})...", uri);
            long ta = System.currentTimeMillis();
            try (BufferAllocator warmAlloc = new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE);
                 FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                         warmAlloc, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                         new String[]{uri});
                 Dataset dataset = factory.finish()) {
                dataset.newScan(new ScanOptions.Builder(1).build());
                LOGGER.info("Acero JNI warm-up done in {}ms", System.currentTimeMillis() - ta);
            } catch (Exception e) {
                LOGGER.warn("Acero JNI warm-up failed (non-fatal): {}", e.getMessage());
            }
        });

        // ── 3. DuckDB thread-local warm-up ───────────────────────────────────────
        int duckWarm = DUCKDB_GROUPS;
        LOGGER.info("Pre-warming {} DuckDB thread-local connections...", duckWarm);
        long td = System.currentTimeMillis();
        List<Future<?>> duckFutures = new ArrayList<>(duckWarm);
        for (int i = 0; i < duckWarm; i++) duckFutures.add(IO_POOL.submit(DUCKDB_THREAD_CONN::get));
        for (Future<?> f : duckFutures) { try { f.get(); } catch (Exception ignored) {} }
        LOGGER.info("DuckDB warm-up done in {}ms", System.currentTimeMillis() - td);
    }

    /** Returns the URI of the first Parquet file found in any known table directory, or empty. */
    private Optional<String> findFirstParquetUri() {
        for (Map<String, Path> tables : tableCache.values()) {
            for (Path tablePath : tables.values()) {
                try {
                    RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(tablePath, true);
                    while (it.hasNext()) {
                        LocatedFileStatus lfs = it.next();
                        if (lfs.isFile() && lfs.getPath().getName().endsWith(".parquet")) {
                            return Optional.of(lfs.getPath().toUri().toString());
                        }
                    }
                } catch (IOException ignored) {}
            }
        }
        return Optional.empty();
    }

    protected String arrowSchemaToDDL(String tableSchema, String tableName, Schema schema) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(schema);

        StringBuilder result = new StringBuilder("CREATE TABLE ");
        if (tableSchema != null) result.append(tableSchema).append(".");
        result.append(tableName).append("(\n");

        List<Field> fields = schema.getFields();
        IntStream.range(0, fields.size()).forEach(i -> {
            Field field = fields.get(i);
            RelDataType relDataType;
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                relDataType = ArrowFieldTypeFactory.toType(new ArrowType.Int(64, true), typeFactory);
            } else {
                relDataType = ArrowFieldTypeFactory.toType(field.getType(), typeFactory);
            }
            if (i > 0) result.append(",\n");
            result.append("\t\"").append(field.getName()).append("\" ").append(relDataType);
        });
        result.append(")");
        return result.toString();
    }

    public Schema getQuerySchema(String query) {
        ParquetQueryParser pq = ParquetQueryParser.parse(query);
        return pq.hasAggregation
                ? buildAggregationSchema(pq)
                : getTableSchema(pq.schema, pq.table, pq.columns);
    }

    /**
     * Builds the Arrow output schema for an aggregation query.
     * COUNT → Int64; SUM → Float64; MIN/MAX/COLUMN → Parquet column type.
     */
    Schema buildAggregationSchema(ParquetQueryParser pq) {
        Schema tableSchema = getTableSchema(pq.schema, pq.table);
        Map<String, Field> colFieldMap = tableSchema.getFields().stream()
                .collect(Collectors.toMap(Field::getName, f -> f, (a, b) -> a));

        List<Field> resultFields = new ArrayList<>();
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            switch (expr.func) {
                case COLUMN -> {
                    Field src = resolveColumn(colFieldMap, expr.inputColumn);
                    resultFields.add(new Field(expr.outputName, FieldType.nullable(src.getType()), null));
                }
                case COUNT_STAR, COUNT ->
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(new ArrowType.Int(64, true)), null));
                case SUM ->
                    resultFields.add(new Field(expr.outputName,
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                case MIN, MAX -> {
                    Field src = resolveColumn(colFieldMap, expr.inputColumn);
                    resultFields.add(new Field(expr.outputName, FieldType.nullable(src.getType()), null));
                }
            }
        }
        return new Schema(resultFields);
    }

    /** Case-insensitive column lookup; throws a clear error if the column is not found. */
    private static Field resolveColumn(Map<String, Field> colFieldMap, String inputColumn) {
        Field src = colFieldMap.get(inputColumn);
        if (src != null) return src;
        // Case-insensitive fallback
        for (Map.Entry<String, Field> entry : colFieldMap.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(inputColumn)) return entry.getValue();
        }
        throw new IllegalArgumentException(
                "Unsupported function in select expression: column '" + inputColumn
                + "' not found in table schema. Available columns: " + colFieldMap.keySet());
    }

    public Map<String, Set<String>> locationsForQuery(String query) throws IOException {
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);
        Map<String, Set<String>> result = new HashMap<>();
        URI dataDirectoryURI = fileSystem.getFileStatus(new Path(dataDirectory)).getPath().toUri();
        Path parquetPath = new Path(dataDirectory, parsedQuery.schema + "/" + parsedQuery.table);
        RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(parquetPath, true);
        while (filesIter.hasNext()) {
            LocatedFileStatus file = filesIter.next();
            if (file.isDirectory() || !file.getPath().getName().toLowerCase().endsWith(".parquet")) continue;
            String relativePath = dataDirectoryURI.relativize(file.getPath().toUri()).toString();
            result.put(relativePath, fileLocality(file).keySet());
        }
        return result;
    }

    /**
     * Read specific Parquet files and send Arrow batches through the listener.
     * Aggregation queries go through the parallel aggregation path.
     */
    public void readParquet(BufferAllocator allocator, String query, String[] fileUris,
            FlightProducer.ServerStreamListener listener, boolean startListener) throws Exception {
        ParquetQueryParser parsedQuery = ParquetQueryParser.parse(query);

        if (parsedQuery.hasAggregation) {
            executeAggregation(allocator, parsedQuery, fileUris, listener, startListener);
            return;
        }

        String schema = parsedQuery.schema;
        String table  = parsedQuery.table;
        List<String> selectedColumns = parsedQuery.columns;
        String filter = parsedQuery.filter;

        List<String> parquetUris = new ArrayList<>();
        if (fileUris != null && fileUris.length > 0) {
            for (String rel : fileUris) {
                Path full = fileSystem.getFileStatus(new Path(dataDirectory, rel)).getPath();
                LOGGER.info("Converting relative path '{}' to absolute {}", rel, full);
                parquetUris.add(full.toUri().toString());
            }
        } else {
            Path tablePath = new Path(dataDirectory, schema + "/" + table);
            RemoteIterator<LocatedFileStatus> filesIter = fileSystem.listFiles(tablePath, true);
            while (filesIter.hasNext()) {
                LocatedFileStatus file = filesIter.next();
                if (file.isDirectory() || !file.getPath().getName().endsWith(".parquet")) continue;
                parquetUris.add(file.getPath().toUri().toString());
            }
        }

        if (parquetUris.isEmpty()) {
            LOGGER.warn("No Parquet files to read for query: {}", query);
            return;
        }

        ByteBuffer byteBuffer = null;
        if (filter != null && !filter.trim().isEmpty()) {
            String ddl = tableDdlCache.getOrDefault(schema, Collections.emptyMap()).get(table);
            ddl = ddl.replace(schema + ".", "");
            LOGGER.info("Parsing filter '{}' for table {}.{}", filter, schema, table);
            byteBuffer = SubstraitFilterConverter.toByteBuffer(filter, Collections.singletonList(ddl));
        }

        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                     allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                     parquetUris.toArray(new String[0]))) {
            Dataset dataset = factory.finish();
            ScanOptions.Builder sb = new ScanOptions.Builder(32768)
                    .columns(selectedColumns.isEmpty() ? Optional.empty()
                            : Optional.of(selectedColumns.toArray(new String[0])));
            if (byteBuffer != null) sb.substraitFilter(byteBuffer);

            Scanner scanner = dataset.newScan(sb.build());
            Schema aceroSchema = scanner.schema();
            LOGGER.info("Build scanner for query: {} with Acero schema: {}", query, aceroSchema);
            checkSchemaAlignment(query, aceroSchema, selectedColumns);
            ArrowReader reader = scanner.scanBatches();
            VectorSchemaRoot vsr = reader.getVectorSchemaRoot();

            if (startListener) listener.start(vsr);

            int read = 0, sent = 0;
            batchLoop:
            while (reader.loadNextBatch()) {
                read++;
                if (vsr.getRowCount() == 0) { vsr.clear(); continue; }
                while (!listener.isReady()) {
                    Thread.sleep(100);
                    if (listener.isCancelled()) { vsr.clear(); break batchLoop; }
                }
                listener.putNext();
                sent++;
                vsr.clear();
            }
            LOGGER.info("Sent {}, read {} batches to client", sent, read);
        }
    }

    // ── aggregation dispatch ──────────────────────────────────────────────────

    /**
     * Routes aggregation queries:
     * <ul>
     *   <li>No files → emit zero/null result with correct schema.</li>
     *   <li>COUNT(*) only, no filter → Parquet footer fast path (zero column I/O).</li>
     *   <li>MIN/MAX/COUNT(col) only, no filter → Parquet footer statistics fast path.</li>
     *   <li>Everything else → {@link #parallelAggregate}: Acero + DuckDB grouped.</li>
     * </ul>
     */
    private void executeAggregation(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

        if (fileUris == null || fileUris.length == 0) {
            emitRowsAsArrow(allocator, pq, Collections.emptyList(), listener, startListener);
            return;
        }

        boolean noGroupByNoFilter = pq.groupByColumnNames.isEmpty()
                && (pq.filter == null || pq.filter.isBlank())
                && !pq.selectExprs.isEmpty();

        // Fast path A: COUNT(*) only — read row counts from Parquet footer (zero column I/O).
        if (noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(
                        e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)) {
            List<Future<Long>> futs = new ArrayList<>(fileUris.length);
            for (String rel : fileUris) futs.add(IO_POOL.submit(() -> footerRowCount(rel)));
            long total = 0;
            for (Future<Long> f : futs) total += f.get();
            LOGGER.debug("COUNT(*) footer fast-path: {} file(s), total={}", fileUris.length, total);
            int n = pq.selectExprs.size();
            Object[] row = new Object[n];
            Arrays.fill(row, total);
            emitRowsAsArrow(allocator, pq, Collections.singletonList(row), listener, startListener);
            return;
        }

        // Fast path B: MIN / MAX / COUNT(col) / COUNT(*) only — read per-row-group statistics
        // from the Parquet footer (zero column I/O).
        boolean statsEligible = noGroupByNoFilter
                && pq.selectExprs.stream().allMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)
                && pq.selectExprs.stream().anyMatch(e ->
                        e.func == ParquetQueryParser.SelectExpr.AggFunc.MIN
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.MAX
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT);
        if (statsEligible) {
            List<Future<Optional<Object[]>>> futs = new ArrayList<>(fileUris.length);
            for (String rel : fileUris) futs.add(IO_POOL.submit(() -> footerStats(rel, pq)));
            Object[] merged = null;
            boolean allHaveStats = true;
            for (Future<Optional<Object[]>> f : futs) {
                Optional<Object[]> opt = f.get();
                if (opt.isEmpty()) { allHaveStats = false; break; }
                if (merged == null) merged = opt.get().clone();
                else mergeAggCols(pq.selectExprs, merged, opt.get(), 0);
            }
            if (allHaveStats) {
                LOGGER.debug("MIN/MAX footer stats fast-path: {} file(s)", fileUris.length);
                List<Object[]> rows = merged != null
                        ? Collections.singletonList(merged) : Collections.emptyList();
                emitRowsAsArrow(allocator, pq, rows, listener, startListener);
                return;
            }
            LOGGER.debug("MIN/MAX stats missing in at least one file; falling back to Acero+DuckDB");
        }

        parallelAggregate(allocator, pq, fileUris, listener, startListener);
    }

    // ── parallel aggregation (Acero COUNT(*) / DuckDB native read_parquet) ──────

    /**
     * Runs aggregation in parallel across files.
     *
     * <ul>
     *   <li>COUNT(*)-only with no GROUP BY (and filter expressible as Substrait): one Acero
     *       task per file — counts rows directly, no DuckDB involved.</li>
     *   <li>Everything else: files are partitioned into at most {@code DUCKDB_GROUPS} groups.
     *       Each group runs a single DuckDB {@code read_parquet([...])} query — no Arrow C
     *       streams, no Acero scan, no native lifecycle to manage.</li>
     * </ul>
     */
    private void parallelAggregate(BufferAllocator allocator, ParquetQueryParser pq,
            String[] fileUris, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws Exception {

        List<String> parquetUris = resolveUris(fileUris);
        boolean hasFilter = pq.filter != null && !pq.filter.isBlank();

        boolean isCountStarOnly = pq.groupByColumnNames.isEmpty()
                && !pq.selectExprs.isEmpty()
                && pq.selectExprs.stream()
                        .allMatch(e -> e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR);

        if (isCountStarOnly) {
            byte[] filterBytes = buildFilterBytes(pq);
            Optional<String[]> cols = buildProjection(pq);
            if (!hasFilter || filterBytes != null) {
                // Per-file row count via Acero; no DuckDB involved.
                int numCountStarCols = pq.selectExprs.size();
                List<Future<List<Object[]>>> futures = new ArrayList<>(parquetUris.size());
                for (String uri : parquetUris) {
                    futures.add(IO_POOL.submit(() -> {
                        try (BufferAllocator child =
                                     allocator.newChildAllocator("par-agg", 0, Long.MAX_VALUE)) {
                            return aggregateFile(child, uri, filterBytes, cols, numCountStarCols);
                        }
                    }));
                }
                List<Object[]> merged = mergePartialRows(pq.selectExprs, pq.groupByColumnNames, futures);
                emitRowsAsArrow(allocator, pq, merged, listener, startListener);
                return;
            }
            // COUNT(*) with filter that Substrait can't express → fall through to DuckDB.
        }

        // DuckDB path: Acero scans files → Arrow C streams → DuckDB aggregates.
        int numGroups = Math.min(DUCKDB_GROUPS, parquetUris.size());
        List<List<String>> groups = partitionIntoGroups(parquetUris, numGroups);
        byte[] filterBytes = buildFilterBytes(pq);
        Optional<String[]> cols = buildProjection(pq);
        List<Future<VectorSchemaRoot>> vsrFutures = new ArrayList<>(groups.size());
        for (List<String> group : groups) {
            String duckSql = buildGroupedDuckSql(pq, group.size(), filterBytes != null);
            vsrFutures.add(IO_POOL.submit(
                    () -> aggregateGroupToVsr(allocator, group, filterBytes, cols, duckSql)));
        }
        List<VectorSchemaRoot> partials = new ArrayList<>(vsrFutures.size());
        try {
            for (Future<VectorSchemaRoot> f : vsrFutures) partials.add(f.get());
            try (VectorSchemaRoot merged = mergeVsrPartials(allocator, pq, partials)) {
                if (startListener) listener.start(merged);
                if (merged.getRowCount() > 0) {
                    while (!listener.isReady()) Thread.sleep(10);
                    listener.putNext();
                }
            }
        } finally {
            partials.forEach(VectorSchemaRoot::close);
        }
    }

    /**
     * Scans one Parquet file with Acero and counts rows (COUNT(*)-only path).
     */
    private static List<Object[]> aggregateFile(
            BufferAllocator allocator, String fileUri,
            byte[] filterBytes, Optional<String[]> cols,
            int numCountStarCols) throws Exception {

        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                     allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                     new String[]{fileUri});
             Dataset dataset = factory.finish()) {

            ScanOptions.Builder optBuilder = new ScanOptions.Builder(65536).columns(cols);
            if (filterBytes != null) {
                ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
                direct.put(filterBytes).flip();
                optBuilder.substraitFilter(direct);
            }
            long count = 0;
            try (ArrowReader reader = dataset.newScan(optBuilder.build()).scanBatches()) {
                while (reader.loadNextBatch())
                    count += reader.getVectorSchemaRoot().getRowCount();
            }
            Object[] row = new Object[numCountStarCols];
            Arrays.fill(row, count);
            return Collections.singletonList(row);
        }
    }

    /**
     * Scans a group of Parquet files with Acero, feeds the Arrow streams into DuckDB,
     * and returns the aggregation result as a {@link VectorSchemaRoot}.
     *
     * <p>Resource lifecycle note: {@code Scanner} objects are kept in an explicit list so
     * the GC cannot finalize them (releasing the native C++ scanner) while DuckDB is still
     * reading the Arrow C stream backed by that scanner. This prevents the intermittent
     * native crash caused by DuckDB's JNI callbacks into a freed C++ scanner.
     */
    private static VectorSchemaRoot aggregateGroupToVsr(
            BufferAllocator allocator, List<String> fileUris,
            byte[] filterBytes, Optional<String[]> cols,
            String duckSql) throws Exception {

        int n = fileUris.size();
        List<FileSystemDatasetFactory> factories = new ArrayList<>(n);
        List<Dataset> datasets = new ArrayList<>(n);
        List<Scanner> scanners = new ArrayList<>(n);   // kept alive to prevent GC during native use
        List<ArrowReader> readers = new ArrayList<>(n); // closed before scanners
        List<ArrowArrayStream> cStreams = new ArrayList<>(n);

        try {
            Connection conn = DUCKDB_THREAD_CONN.get();
            DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);

            for (int i = 0; i < n; i++) {
                FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,
                        new String[]{fileUris.get(i)});
                factories.add(factory);
                Dataset dataset = factory.finish();
                datasets.add(dataset);

                ScanOptions.Builder optBuilder = new ScanOptions.Builder(65536).columns(cols);
                if (filterBytes != null) {
                    ByteBuffer direct = ByteBuffer.allocateDirect(filterBytes.length);
                    direct.put(filterBytes).flip();
                    optBuilder.substraitFilter(direct);
                }
                Scanner scanner = dataset.newScan(optBuilder.build());
                scanners.add(scanner);                   // hold reference — prevents premature GC
                ArrowReader reader = scanner.scanBatches();
                readers.add(reader);
                ArrowArrayStream cStream = ArrowArrayStream.allocateNew(allocator);
                cStreams.add(cStream);
                Data.exportArrayStream(allocator, reader, cStream);
                duckConn.registerArrowStream("t" + i, cStream);
            }

            try (Statement stmt = conn.createStatement()) {
                org.duckdb.DuckDBResultSet drs =
                        (org.duckdb.DuckDBResultSet) stmt.executeQuery(duckSql);
                try (ArrowReader arrowReader = (ArrowReader) drs.arrowExportStream(allocator, 2048)) {
                    return concatBatches(allocator, arrowReader);
                }
            }
        } finally {
            // Close in reverse-dependency order: streams → readers → scanners → datasets → factories.
            for (int i = cStreams.size() - 1; i >= 0; i--)
                try { cStreams.get(i).close(); } catch (Exception ignored) {}
            for (int i = readers.size() - 1; i >= 0; i--)
                try { readers.get(i).close(); } catch (Exception ignored) {}
            for (int i = scanners.size() - 1; i >= 0; i--)
                try { scanners.get(i).close(); } catch (Exception ignored) {}
            for (int i = datasets.size() - 1; i >= 0; i--)
                try { datasets.get(i).close(); } catch (Exception ignored) {}
            for (int i = factories.size() - 1; i >= 0; i--)
                try { factories.get(i).close(); } catch (Exception ignored) {}
        }
    }

    /** Concatenates all batches from an ArrowReader into a single VectorSchemaRoot using copyFromSafe. */
    private static VectorSchemaRoot concatBatches(BufferAllocator allocator, ArrowReader reader)
            throws IOException {
        VectorSchemaRoot src = reader.getVectorSchemaRoot();
        int numCols = src.getSchema().getFields().size();
        List<FieldVector> outVecs = new ArrayList<>(numCols);
        for (Field f : src.getSchema().getFields()) {
            FieldVector v = f.createVector(allocator);
            v.allocateNew();
            outVecs.add(v);
        }
        int outRow = 0;
        while (reader.loadNextBatch()) {
            for (int r = 0; r < src.getRowCount(); r++) {
                for (int c = 0; c < numCols; c++)
                    outVecs.get(c).copyFromSafe(r, outRow, src.getVector(c));
                outRow++;
            }
        }
        for (FieldVector v : outVecs) v.setValueCount(outRow);
        VectorSchemaRoot result = new VectorSchemaRoot(src.getSchema().getFields(), outVecs);
        result.setRowCount(outRow);
        return result;
    }

    /**
     * Merges partial per-group VectorSchemaRoots into a single output VSR
     * with the schema from {@link #buildAggregationSchema}.
     * Closes each partial after consuming it.
     */
    private VectorSchemaRoot mergeVsrPartials(BufferAllocator allocator, ParquetQueryParser pq,
            List<VectorSchemaRoot> partials) {
        int numGbCols = pq.groupByColumnNames.size();
        List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
        Schema outSchema = buildAggregationSchema(pq);

        if (numGbCols == 0) {
            // Scalar aggregates — one output row; merge column-by-column with primitives.
            long[]   longAccum = new long[exprs.size()];
            double[] dblAccum  = new double[exprs.size()];
            Object[] objAccum  = new Object[exprs.size()]; // MIN / MAX
            boolean any = false;

            for (VectorSchemaRoot partial : partials) {
                if (partial.getRowCount() == 0) continue;
                any = true;
                int col = 0;
                for (ParquetQueryParser.SelectExpr expr : exprs) {
                    FieldVector vec = partial.getVector(col);
                    if (!vec.isNull(0)) {
                        switch (expr.func) {
                            case COUNT_STAR, COUNT -> longAccum[col] += toLong(vec, 0);
                            case SUM               -> dblAccum[col]  += toDouble(vec, 0);
                            case MIN -> objAccum[col] = objAccum[col] == null
                                    ? vec.getObject(0) : minOf(objAccum[col], vec.getObject(0));
                            case MAX -> objAccum[col] = objAccum[col] == null
                                    ? vec.getObject(0) : maxOf(objAccum[col], vec.getObject(0));
                        }
                    }
                    col++;
                }
            }

            List<FieldVector> outVecs = new ArrayList<>();
            for (Field f : outSchema.getFields()) {
                FieldVector v = f.createVector(allocator);
                if      (v instanceof FixedWidthVector fv)   fv.allocateNew(1);
                else if (v instanceof VariableWidthVector vv) vv.allocateNew(32);
                outVecs.add(v);
            }
            if (any) {
                int col = 0;
                for (ParquetQueryParser.SelectExpr expr : exprs) {
                    FieldVector v = outVecs.get(col);
                    switch (expr.func) {
                        case COUNT_STAR, COUNT -> ((BigIntVector) v).setSafe(0, longAccum[col]);
                        case SUM               -> ((Float8Vector) v).setSafe(0, dblAccum[col]);
                        case MIN, MAX          -> setVectorValue(v, 0, objAccum[col]);
                    }
                    col++;
                }
            }
            VectorSchemaRoot r = new VectorSchemaRoot(outSchema.getFields(), outVecs);
            r.setRowCount(any ? 1 : 0);
            return r;

        } else {
            // GROUP BY — map keyed by GB-column values, Object[] accumulator for agg values.
            int numAggExprs = (int) exprs.stream()
                    .filter(e -> e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN).count();
            Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();

            for (VectorSchemaRoot partial : partials) {
                int rowCount = partial.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    List<Object> key = new ArrayList<>(numGbCols);
                    for (int c = 0; c < numGbCols; c++)
                        key.add(partial.getVector(c).getObject(r));

                    Object[] accum = byKey.computeIfAbsent(key, k -> new Object[numAggExprs]);
                    int ai = 0;
                    for (ParquetQueryParser.SelectExpr expr : exprs) {
                        if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) continue;
                        FieldVector vec = partial.getVector(numGbCols + ai);
                        if (!vec.isNull(r)) {
                            Object val = vec.getObject(r);
                            switch (expr.func) {
                                case COUNT_STAR, COUNT -> accum[ai] = addLongs(accum[ai], val);
                                case SUM               -> accum[ai] = addDoubles(accum[ai], val);
                                case MIN -> accum[ai] = accum[ai] == null ? val : minOf(accum[ai], val);
                                case MAX -> accum[ai] = accum[ai] == null ? val : maxOf(accum[ai], val);
                            }
                        }
                        ai++;
                    }
                }
            }

            int totalRows = byKey.size();
            List<FieldVector> outVecs = new ArrayList<>();
            for (Field f : outSchema.getFields()) {
                FieldVector v = f.createVector(allocator);
                if      (v instanceof FixedWidthVector fv)   fv.allocateNew(totalRows);
                else if (v instanceof VariableWidthVector vv) vv.allocateNew(totalRows * 16);
                outVecs.add(v);
            }
            int row = 0;
            for (Map.Entry<List<Object>, Object[]> entry : byKey.entrySet()) {
                List<Object> key   = entry.getKey();
                Object[]     accum = entry.getValue();
                int vecIdx = 0, ai = 0;
                for (ParquetQueryParser.SelectExpr expr : exprs) {
                    FieldVector v = outVecs.get(vecIdx++);
                    if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN)
                        setVectorValue(v, row, key.get(pq.groupByColumnNames.indexOf(expr.inputColumn)));
                    else
                        setVectorValue(v, row, accum[ai++]);
                }
                row++;
            }
            for (FieldVector v : outVecs) v.setValueCount(totalRows);
            VectorSchemaRoot result = new VectorSchemaRoot(outSchema.getFields(), outVecs);
            result.setRowCount(totalRows);
            return result;
        }
    }

    private static long toLong(FieldVector vec, int index) {
        if (vec instanceof BigIntVector v)   return v.get(index);
        if (vec instanceof IntVector v)      return v.get(index);
        if (vec instanceof SmallIntVector v) return v.get(index);
        if (vec instanceof TinyIntVector v)  return v.get(index);
        return ((Number) vec.getObject(index)).longValue();
    }

    private static double toDouble(FieldVector vec, int index) {
        if (vec instanceof Float8Vector v) return v.get(index);
        if (vec instanceof Float4Vector v) return v.get(index);
        if (vec instanceof BigIntVector v) return v.get(index);
        return ((Number) vec.getObject(index)).doubleValue();
    }

    // ── partial-result merge ──────────────────────────────────────────────────

    private static List<Object[]> mergePartialRows(
            List<ParquetQueryParser.SelectExpr> exprs,
            List<String> groupByColumnNames,
            List<Future<List<Object[]>>> futures) throws Exception {

        int numGbCols = groupByColumnNames.size();

        if (numGbCols == 0) {
            Object[] merged = null;
            for (Future<List<Object[]>> f : futures) {
                List<Object[]> rows = f.get();
                if (rows.isEmpty()) continue;
                Object[] row = rows.get(0);
                if (merged == null) merged = row.clone();
                else mergeAggCols(exprs, merged, row, 0);
            }
            return merged != null ? Collections.singletonList(merged) : Collections.emptyList();
        }

        Map<List<Object>, Object[]> byKey = new LinkedHashMap<>();
        for (Future<List<Object[]>> f : futures) {
            for (Object[] row : f.get()) {
                List<Object> key = new ArrayList<>(Arrays.asList(row).subList(0, numGbCols));
                Object[] existing = byKey.get(key);
                if (existing == null) byKey.put(key, row.clone());
                else mergeAggCols(exprs, existing, row, numGbCols);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    private static void mergeAggCols(List<ParquetQueryParser.SelectExpr> exprs,
            Object[] into, Object[] from, int numGbCols) {
        int aggIdx = 0;
        for (ParquetQueryParser.SelectExpr expr : exprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) continue;
            int pos = numGbCols + aggIdx++;
            switch (expr.func) {
                case COUNT_STAR, COUNT -> into[pos] = addLongs(into[pos], from[pos]);
                case SUM               -> into[pos] = addDoubles(into[pos], from[pos]);
                case MIN               -> into[pos] = minOf(into[pos], from[pos]);
                case MAX               -> into[pos] = maxOf(into[pos], from[pos]);
                default -> {}
            }
        }
    }

    private static Object addLongs(Object a, Object b) {
        if (a == null) return b == null ? 0L : ((Number) b).longValue();
        if (b == null) return ((Number) a).longValue();
        return ((Number) a).longValue() + ((Number) b).longValue();
    }

    private static Object addDoubles(Object a, Object b) {
        if (a == null) return b == null ? 0.0 : ((Number) b).doubleValue();
        if (b == null) return ((Number) a).doubleValue();
        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private static Object minOf(Object a, Object b) {
        if (a == null) return b;
        if (b == null) return a;
        return ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b;
    }

    @SuppressWarnings("unchecked")
    private static Object maxOf(Object a, Object b) {
        if (a == null) return b;
        if (b == null) return a;
        return ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b;
    }

    // ── emit ──────────────────────────────────────────────────────────────────

    private void emitRowsAsArrow(BufferAllocator allocator, ParquetQueryParser pq,
            List<Object[]> rows, FlightProducer.ServerStreamListener listener,
            boolean startListener) throws InterruptedException {

        Schema aggSchema = buildAggregationSchema(pq);
        int numGbCols = pq.groupByColumnNames.size();

        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : aggSchema.getFields()) {
            FieldVector v = field.createVector(allocator);
            if      (v instanceof FixedWidthVector fv)   fv.allocateNew(rows.size());
            else if (v instanceof VariableWidthVector vv) vv.allocateNew(rows.size() * 16);
            vectors.add(v);
        }

        try (VectorSchemaRoot root = new VectorSchemaRoot(aggSchema.getFields(), vectors)) {
            root.setRowCount(rows.size());
            int vecIdx = 0, aggIdx = 0;
            for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
                FieldVector vec = vectors.get(vecIdx++);
                if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN) {
                    int gbPos = pq.groupByColumnNames.indexOf(expr.inputColumn);
                    for (int r = 0; r < rows.size(); r++)
                        setVectorValue(vec, r, rows.get(r)[gbPos]);
                } else {
                    int pos = numGbCols + aggIdx++;
                    for (int r = 0; r < rows.size(); r++)
                        setVectorValue(vec, r, rows.get(r)[pos]);
                }
            }

            if (startListener) listener.start(root);
            if (!rows.isEmpty()) {
                while (!listener.isReady()) Thread.sleep(10);
                listener.putNext();
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Resolves relative file paths to absolute {@code file://} URIs. */
    private List<String> resolveUris(String[] fileUris) throws IOException {
        List<String> uris = new ArrayList<>(fileUris.length);
        for (String rel : fileUris)
            uris.add(fileSystem.getFileStatus(new Path(dataDirectory, rel))
                    .getPath().toUri().toString());
        return uris;
    }

    private Optional<String[]> buildProjection(ParquetQueryParser pq) {
        Set<String> scanCols = new LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr e : pq.selectExprs) {
            if (e.func != ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && e.func != ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR
                    && e.inputColumn != null) {
                scanCols.add(e.inputColumn);
            }
        }
        if (pq.filter != null && !pq.filter.isBlank()) {
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("\"([^\"]+)\"").matcher(pq.filter);
            while (m.find()) scanCols.add(m.group(1));
        }
        if (scanCols.isEmpty()) {
            Schema tSchema = getTableSchema(pq.schema, pq.table);
            if (!tSchema.getFields().isEmpty()) scanCols.add(tSchema.getFields().get(0).getName());
        }
        return scanCols.isEmpty() ? Optional.empty()
                : Optional.of(scanCols.toArray(new String[0]));
    }

    private byte[] buildFilterBytes(ParquetQueryParser pq) {
        String filter = pq.filter;
        if (filter == null || filter.trim().isEmpty()) return null;
        String ddl = tableDdlCache.getOrDefault(pq.schema, Collections.emptyMap()).get(pq.table);
        if (ddl == null) return null;
        try {
            ddl = ddl.replace(pq.schema + ".", "");
            ByteBuffer bb = SubstraitFilterConverter.toByteBuffer(filter, Collections.singletonList(ddl));
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return bytes;
        } catch (Exception e) {
            LOGGER.warn("Could not convert filter to Substrait; DuckDB will apply it: {}", e.getMessage());
            return null;
        }
    }

    private static String buildGroupedDuckSql(ParquetQueryParser pq, int numFiles,
            boolean filterAlreadyApplied) {
        if (numFiles == 1) return buildDuckSqlWithFrom(pq, "\"t0\"", filterAlreadyApplied);
        StringBuilder from = new StringBuilder("(");
        for (int i = 0; i < numFiles; i++) {
            if (i > 0) from.append(" UNION ALL ");
            from.append("SELECT * FROM \"t").append(i).append('"');
        }
        from.append(')');
        return buildDuckSqlWithFrom(pq, from.toString(), filterAlreadyApplied);
    }

    private static String buildDuckSqlWithFrom(ParquetQueryParser pq, String fromClause,
            boolean filterAlreadyApplied) {
        StringBuilder sql = new StringBuilder("SELECT ");
        boolean first = true;

        for (String gbCol : pq.groupByColumnNames) {
            if (!first) sql.append(", ");
            first = false;
            sql.append('"').append(gbCol).append('"');
        }

        Set<String> gbSet = new LinkedHashSet<>(pq.groupByColumnNames);
        for (ParquetQueryParser.SelectExpr expr : pq.selectExprs) {
            if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COLUMN
                    && gbSet.contains(expr.inputColumn)) continue;
            if (!first) sql.append(", ");
            first = false;
            switch (expr.func) {
                case COUNT_STAR -> sql.append("count(*)");
                case COUNT      -> sql.append("count(\"").append(expr.inputColumn).append("\")");
                case SUM        -> sql.append("sum(\"").append(expr.inputColumn).append("\")");
                case MIN        -> sql.append("min(\"").append(expr.inputColumn).append("\")");
                case MAX        -> sql.append("max(\"").append(expr.inputColumn).append("\")");
                case COLUMN     -> sql.append('"').append(expr.inputColumn).append('"');
            }
        }

        sql.append(" FROM ").append(fromClause);
        if (!filterAlreadyApplied && pq.filter != null && !pq.filter.isBlank())
            sql.append(" WHERE ").append(pq.filter);
        if (!pq.groupByColumnNames.isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < pq.groupByColumnNames.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append('"').append(pq.groupByColumnNames.get(i)).append('"');
            }
        }
        return sql.toString();
    }

    /** Distributes {@code items} into {@code numGroups} buckets via round-robin. */
    private static <T> List<List<T>> partitionIntoGroups(List<T> items, int numGroups) {
        List<List<T>> groups = new ArrayList<>(numGroups);
        for (int i = 0; i < numGroups; i++) groups.add(new ArrayList<>());
        for (int i = 0; i < items.size(); i++) groups.get(i % numGroups).add(items.get(i));
        return groups;
    }

    @SuppressWarnings("unchecked")
    private static void setVectorValue(FieldVector vec, int index, Object value) {
        if (value == null) { vec.setNull(index); return; }
        if      (vec instanceof BigIntVector)   ((BigIntVector)   vec).setSafe(index, ((Number) value).longValue());
        else if (vec instanceof IntVector)      ((IntVector)      vec).setSafe(index, ((Number) value).intValue());
        else if (vec instanceof SmallIntVector) ((SmallIntVector) vec).setSafe(index, ((Number) value).shortValue());
        else if (vec instanceof TinyIntVector)  ((TinyIntVector)  vec).setSafe(index, ((Number) value).byteValue());
        else if (vec instanceof Float8Vector)   ((Float8Vector)   vec).setSafe(index, ((Number) value).doubleValue());
        else if (vec instanceof Float4Vector)   ((Float4Vector)   vec).setSafe(index, ((Number) value).floatValue());
        else if (vec instanceof BitVector)      ((BitVector)      vec).setSafe(index, ((Boolean) value) ? 1 : 0);
        else if (vec instanceof VarCharVector) {
            byte[] bytes = value instanceof Text
                    ? ((Text) value).getBytes()
                    : value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(index, bytes);
        }
    }

    /** Reads only the Parquet footer to get the total row count for a file. Zero column I/O. */
    private long footerRowCount(String rel) throws IOException {
        Path full = new Path(dataDirectory, rel);
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new org.apache.parquet.io.InputFile() {
            @Override public long getLength() { return fileLen; }
            @Override public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                return org.apache.parquet.hadoop.util.HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            long count = 0;
            for (org.apache.parquet.hadoop.metadata.BlockMetaData b : pfr.getFooter().getBlocks())
                count += b.getRowCount();
            return count;
        }
    }

    private Optional<Object[]> footerStats(String rel,
            ParquetQueryParser pq) throws IOException {
        Path full = new Path(dataDirectory, rel);
        final long fileLen = fileSystem.getFileStatus(full).getLen();
        try (ParquetFileReader pfr = ParquetFileReader.open(new org.apache.parquet.io.InputFile() {
            @Override public long getLength() { return fileLen; }
            @Override public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
                return org.apache.parquet.hadoop.util.HadoopStreams.wrap(fileSystem.open(full));
            }
        })) {
            List<ParquetQueryParser.SelectExpr> exprs = pq.selectExprs;
            int n = exprs.size();
            Object[] result = new Object[n];
            long totalRows = 0;

            for (org.apache.parquet.hadoop.metadata.BlockMetaData block
                    : pfr.getFooter().getBlocks()) {
                totalRows += block.getRowCount();

                for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData cc
                        : block.getColumns()) {
                    String colName = cc.getPath().toDotString();

                    for (int i = 0; i < n; i++) {
                        ParquetQueryParser.SelectExpr expr = exprs.get(i);
                        if (!colName.equals(expr.inputColumn)) continue;

                        org.apache.parquet.column.statistics.Statistics<?> stats =
                                cc.getStatistics();
                        if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MIN) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue())
                                return Optional.empty();
                            result[i] = minOf(result[i], stats.genericGetMin());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.MAX) {
                            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue())
                                return Optional.empty();
                            result[i] = maxOf(result[i], stats.genericGetMax());
                        } else if (expr.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT) {
                            if (stats == null || stats.getNumNulls() < 0)
                                return Optional.empty();
                            long nonNulls = block.getRowCount() - stats.getNumNulls();
                            result[i] = (result[i] == null ? 0L
                                    : ((Number) result[i]).longValue()) + nonNulls;
                        }
                    }
                }
            }

            for (int i = 0; i < n; i++) {
                if (exprs.get(i).func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR)
                    result[i] = totalRows;
            }
            return Optional.of(result);
        }
    }

    /**
     * Compares Acero's schema against the schema ParquetSchemaConverter would report for the
     * same table. Any mismatch means the schema sent to Spark via FlightInfo disagrees with the
     * data Acero actually streams — the telltale root cause of ClassCastException in join codegen.
     */
    private void checkSchemaAlignment(String query, Schema aceroSchema, List<String> selectedColumns) {
        try {
            ParquetQueryParser pq = ParquetQueryParser.parse(query);
            if (pq.hasAggregation) return;
            Schema expectedSchema = getTableSchema(pq.schema, pq.table, selectedColumns.isEmpty() ? null : selectedColumns);
            Map<String, org.apache.arrow.vector.types.pojo.ArrowType> expected = new java.util.LinkedHashMap<>();
            for (org.apache.arrow.vector.types.pojo.Field f : expectedSchema.getFields()) expected.put(f.getName(), f.getType());

            boolean mismatch = false;
            for (org.apache.arrow.vector.types.pojo.Field af : aceroSchema.getFields()) {
                org.apache.arrow.vector.types.pojo.ArrowType exp = expected.get(af.getName());
                if (exp != null && !exp.equals(af.getType())) {
                    LOGGER.warn("SCHEMA MISMATCH for column '{}': FlightInfo reports {} but Acero streams {} — " +
                            "Spark codegen will use the wrong type, causing ClassCastException. " +
                            "ParquetSchemaConverter must be fixed or the server JAR is stale.",
                            af.getName(), exp, af.getType());
                    mismatch = true;
                }
            }
            if (!mismatch) {
                LOGGER.debug("Schema alignment OK for query: {}", query);
            }
        } catch (Exception e) {
            LOGGER.debug("Schema alignment check skipped: {}", e.getMessage());
        }
    }

    public LinkedHashMap<String, Long> fileLocality(LocatedFileStatus locatedFileStatus) {
        Objects.requireNonNull(locatedFileStatus);
        return Arrays.stream(locatedFileStatus.getBlockLocations())
                .flatMap(bl -> {
                    try {
                        return Stream.of(bl.getHosts())
                                .map(host -> "localhost".equals(host) ? localhost : host);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.groupingBy(h -> h, Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
    }
}
