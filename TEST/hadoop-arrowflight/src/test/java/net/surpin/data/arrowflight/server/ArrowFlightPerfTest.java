package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance comparison: local Arrow Dataset read (embedded) vs remote Arrow Flight read (gRPC).
 *
 * Can be run two ways:
 *   1. As a JUnit test:  mvn test -DexcludedGroups="" -Dgroups=perf
 *   2. As a main class:  java -cp <fat-jar> ...ArrowFlightPerfTest [data-dir] [schema] [table]
 *
 * Results are printed to stdout. For Spark-based comparison (requires SPARK_USER env var set
 * and Hadoop 3.4+ or a real cluster), see SparkPerfTest.
 */
@Tag("integration")
@Tag("perf")
class ArrowFlightPerfTest {

    private static final int WARMUP_RUNS = 1;
    private static final int BENCH_RUNS  = 3;

    // ─── JUnit entry-point ───────────────────────────────────────────────

    @Test
    void compareLocalArrowVsArrowFlight() throws Exception {
        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        run(dataDir, "test_schema", "test_table");
    }

    // ─── standalone entry-point ──────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        String dataDir = args.length > 0 ? args[0]
                : Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        String schema  = args.length > 1 ? args[1] : "test_schema";
        String table   = args.length > 2 ? args[2] : "test_table";
        run(dataDir, schema, table);
    }

    // ─── benchmark logic ─────────────────────────────────────────────────

    static void run(String dataDir, String schema, String table) throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());

        System.out.println("Data directory : " + dataDir);
        System.out.println("Table          : " + schema + "." + table);
        System.out.println();

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

            // ── start embedded Flight server ──────────────────────────────
            Config hzCfg = new Config();
            hzCfg.setClusterName("perf-" + UUID.randomUUID());
            hzCfg.getNetworkConfig().setPort(findFreePort());
            hzCfg.getNetworkConfig().setPortAutoIncrement(false);
            hzCfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            hzCfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
            HazelcastInstance hz = Hazelcast.newHazelcastInstance(hzCfg);
            LocalFileSystem localFs = new LocalFileSystem();
            localFs.initialize(URI.create("file:///"), new Configuration());
            ParquetManager parquetManager = new ParquetManager(localFs, dataDir, "localhost");

            int port = findFreePort();
            Location location = Location.forGrpcInsecure("localhost", port);
            HadoopFlightSqlService sqlService =
                    new HadoopFlightSqlService(location, parquetManager, allocator, hz);
            FlightServer flightServer = FlightServer.builder(allocator, location, sqlService).build();
            flightServer.start();
            System.out.println("Arrow Flight server started on port " + port);

            // ── build parquet file URIs (for local Dataset Scanner) ───────
            String parquetDir = dataDir + "/" + schema + "/" + table;
            String[] parquetUris = findParquetFiles(localFs, parquetDir);
            System.out.println("Parquet files  : " + Arrays.toString(parquetUris));
            System.out.println();

            // ── Flight client ─────────────────────────────────────────────
            FlightClient flightClient = FlightClient.builder(allocator, location).build();
            FlightSqlClient sqlClient  = new FlightSqlClient(flightClient);

            // ── scenarios ─────────────────────────────────────────────────
            String flightTable = schema + "." + table;
            List<Scenario> scenarios = List.of(
                new Scenario("Full scan",
                    () -> localScan(allocator, parquetUris, null, null),
                    () -> flightCount(sqlClient, flightClient,
                            "SELECT * FROM " + flightTable)),
                new Scenario("Filtered (tinyint_col = 0)",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"tinyint_col\" = 0", parquetManager, schema, table),
                    () -> flightCount(sqlClient, flightClient,
                            "SELECT * FROM " + flightTable + " WHERE tinyint_col = 0")),
                new Scenario("Column projection (3 cols)",
                    () -> localScan(allocator, parquetUris,
                            new String[]{"id", "bool_col", "double_col"}, null),
                    () -> flightCount(sqlClient, flightClient,
                            "SELECT id, bool_col, double_col FROM " + flightTable)),
                new Scenario("Filter + projection",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"int_col\" < 5", parquetManager, schema, table),
                    () -> flightCount(sqlClient, flightClient,
                            "SELECT * FROM " + flightTable + " WHERE int_col < 5")),
                new Scenario("Multi-predicate AND",
                    () -> localScanWithFilter(allocator, parquetUris,
                            "\"id\" > 100 and \"id\" < 900", parquetManager, schema, table),
                    () -> flightCount(sqlClient, flightClient,
                            "SELECT * FROM " + flightTable + " WHERE id > 100 AND id < 900"))
            );

            // ── warmup ────────────────────────────────────────────────────
            System.out.println("Warming up (" + WARMUP_RUNS + " run(s))...");
            for (Scenario s : scenarios) {
                for (int i = 0; i < WARMUP_RUNS; i++) {
                    s.localRead.countRows();
                    s.flightRead.countRows();
                }
            }

            // ── benchmark ─────────────────────────────────────────────────
            System.out.println("Benchmarking (" + BENCH_RUNS + " run(s) each)...\n");
            List<BenchResult> results = new ArrayList<>();
            for (Scenario s : scenarios) {
                long[] localTimes  = new long[BENCH_RUNS];
                long[] flightTimes = new long[BENCH_RUNS];
                long rowCount = 0;

                for (int i = 0; i < BENCH_RUNS; i++) {
                    long t0 = System.currentTimeMillis();
                    rowCount = s.localRead.countRows();
                    localTimes[i] = System.currentTimeMillis() - t0;

                    t0 = System.currentTimeMillis();
                    s.flightRead.countRows();
                    flightTimes[i] = System.currentTimeMillis() - t0;
                }
                results.add(new BenchResult(s.label, median(localTimes), median(flightTimes), rowCount));
            }

            // ── print results ─────────────────────────────────────────────
            printResults(results);

            // ── validate correctness ──────────────────────────────────────
            for (BenchResult r : results) {
                assertTrue(r.localMs >= 0 && r.flightMs >= 0,
                        "Negative time in result: " + r.label);
            }
            System.out.println("\nAll scenarios completed successfully.");

            // ── cleanup ───────────────────────────────────────────────────
            sqlClient.close();
            flightServer.shutdown();
            hz.shutdown();
        }
    }

    // ─── reading helpers ─────────────────────────────────────────────────

    static long localScan(BufferAllocator allocator, String[] uris,
                          String[] columns, Object unused) throws Exception {
        ScanOptions.Builder opts = new ScanOptions.Builder(32768)
                .columns(columns == null ? Optional.empty() : Optional.of(columns));
        return countWithDataset(allocator, uris, opts.build());
    }

    static long localScanWithFilter(BufferAllocator allocator, String[] uris,
                                    String filterExpr, ParquetManager pm,
                                    String schema, String table) throws Exception {
        // Strip schema prefix so Substrait/Calcite sees a simple table name
        String ddl = pm.arrowSchemaToDDL(schema, table, pm.getTableSchema(schema, table))
                .replace(schema + ".", "");
        java.nio.ByteBuffer filter = SubstraitFilterConverter.toByteBuffer(
                filterExpr, Collections.singletonList(ddl));
        ScanOptions opts = new ScanOptions.Builder(32768)
                .columns(Optional.empty())
                .substraitFilter(filter)
                .build();
        return countWithDataset(allocator, uris, opts);
    }

    private static long countWithDataset(BufferAllocator allocator,
                                         String[] uris, ScanOptions opts) throws Exception {
        long count = 0;
        try (FileSystemDatasetFactory factory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uris);
             Dataset dataset = factory.finish();
             Scanner scanner = dataset.newScan(opts);
             ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return count;
    }

    static long flightCount(FlightSqlClient sqlClient, FlightClient client, String query)
            throws Exception {
        long count = 0;
        FlightInfo info = sqlClient.execute(query);
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(ep.getTicket())) {
                while (stream.next()) {
                    count += stream.getRoot().getRowCount();
                }
            }
        }
        return count;
    }

    // ─── output ──────────────────────────────────────────────────────────

    static void printResults(List<BenchResult> results) {
        int W = 110;
        System.out.println("=".repeat(W));
        System.out.printf("%-45s | %14s | %14s | %12s | %8s%n",
                "Scenario", "Local Arrow ms", "Flight ms", "Delta ms", "Rows");
        System.out.println("-".repeat(W));
        for (BenchResult r : results) {
            System.out.printf("%-45s | %14d | %14d | %+12d | %8d%n",
                    r.label, r.localMs, r.flightMs, r.flightMs - r.localMs, r.rowCount);
        }
        System.out.println("=".repeat(W));
        System.out.println("Delta: positive = Flight slower (gRPC overhead), negative = Flight faster");
        System.out.println("Local Arrow = Arrow Dataset JNI scan, same engine used by the server");
        System.out.println("Flight      = same scan served over gRPC from embedded server");
    }

    // ─── filesystem helpers ───────────────────────────────────────────────

    static String[] findParquetFiles(LocalFileSystem fs, String dir) throws IOException {
        List<String> uris = new ArrayList<>();
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
                fs.listFiles(new org.apache.hadoop.fs.Path(dir), true);
        while (it.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus f = it.next();
            if (f.isFile() && f.getPath().getName().endsWith(".parquet")) {
                uris.add(f.getPath().toUri().toString());
            }
        }
        return uris.toArray(new String[0]);
    }

    static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    static long median(long[] arr) {
        long[] copy = arr.clone();
        Arrays.sort(copy);
        return copy[copy.length / 2];
    }

    // ─── data types ───────────────────────────────────────────────────────

    @FunctionalInterface
    interface RowCounter { long countRows() throws Exception; }

    record Scenario(String label, RowCounter localRead, RowCounter flightRead) {}

    record BenchResult(String label, long localMs, long flightMs, long rowCount) {}
}
