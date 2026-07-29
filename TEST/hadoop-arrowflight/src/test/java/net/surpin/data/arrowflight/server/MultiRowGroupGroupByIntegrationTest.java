package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that count(*) aggregation returns the correct total when each Parquet file
 * contains multiple row groups, using per-server endpoints (one endpoint per server).
 *
 * Two servers each register in the server-registry; files are distributed round-robin
 * (file[0] → server A, file[1] → server B).  With 2 files and 2 servers each server
 * gets exactly one file, so the endpoint count equals NUM_PARTITIONS (2).
 * Each endpoint streams one partial count(*) row; the client sums them.
 *
 * A small parquet.block.size (16 KB) ensures each file has several row groups so that
 * any "read only first row group" bug is caught immediately.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultiRowGroupGroupByIntegrationTest {

    private static final int  NUM_ROWS       = 10_000;
    private static final int  NUM_PARTITIONS = 2;
    private static final long ROWS_PER_FILE  = NUM_ROWS / NUM_PARTITIONS;

    private static Path tempDir;

    private static BufferAllocator allocA, allocB;
    private static HazelcastInstance hzA, hzB;
    private static FlightServer serverA, serverB;
    private static FlightClient clientA, clientB;
    private static FlightSqlClient sqlClientA, sqlClientB;
    private static Location locA, locB;

    @BeforeAll
    static void setUp() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        tempDir = generateData();
        startCluster();
    }

    @AfterAll
    static void tearDown() throws Exception {
        closeSilently(sqlClientA, sqlClientB, serverA, serverB, hzA, hzB, allocA, allocB);
        if (tempDir != null) deleteTree(tempDir);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * Sum of count(*) across all endpoints must equal NUM_ROWS.
     */
    @Test @Order(1)
    void totalCountStarReturnsAllRows() throws Exception {
        long count = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        assertEquals(NUM_ROWS, count,
                "Total count(*) across all endpoints must be " + NUM_ROWS + ", got " + count);
    }

    /**
     * With 2 servers and 2 files (round-robin), each server owns one file → 2 endpoints.
     * Each endpoint streams one partial count(*) row equal to ROWS_PER_FILE.
     * Their sum must equal NUM_ROWS.
     */
    @Test @Order(2)
    void eachEndpointCountEqualsRowsPerFile() throws Exception {
        FlightInfo info = sqlClientA.execute("SELECT count(*) FROM test_schema.test_table");
        List<FlightEndpoint> eps = info.getEndpoints();
        assertEquals(NUM_PARTITIONS, eps.size(),
                "Expected one endpoint per server (" + NUM_PARTITIONS + " servers), got " + eps.size());

        long total = 0;
        for (FlightEndpoint ep : eps) {
            long c = streamCount(chooseClient(ep), ep.getTicket());
            assertEquals(ROWS_PER_FILE, c,
                    "Each endpoint must return exactly ROWS_PER_FILE=" + ROWS_PER_FILE + ", got " + c);
            total += c;
        }
        assertEquals(NUM_ROWS, total, "Sum of per-endpoint counts must equal NUM_ROWS");
    }

    /**
     * A second independent query must produce the same total (deterministic assignment).
     */
    @Test @Order(3)
    void countIsStableAcrossIndependentQueries() throws Exception {
        long first  = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        long second = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        assertEquals(NUM_ROWS, first,  "First query total must equal NUM_ROWS");
        assertEquals(NUM_ROWS, second, "Second query total must equal NUM_ROWS");
    }

    // ── data generation ───────────────────────────────────────────────────────

    private static Path generateData() throws Exception {
        Path dir = Files.createTempDirectory("mrg-test-");
        Path tableDir = dir.resolve("test_schema/test_table");
        Files.createDirectories(tableDir);

        SparkSession spark = SparkSession.builder()
                .appName("MultiRowGroupTestGen")
                .master("local[2]")
                .config("parquet.block.size", "16384")
                .config("spark.sql.shuffle.partitions", String.valueOf(NUM_PARTITIONS))
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        try {
            Dataset<Row> df = spark.range(0, NUM_ROWS, 1, NUM_PARTITIONS)
                    .select(
                            functions.col("id").cast("integer").alias("id"),
                            functions.col("id").mod(2).equalTo(0).alias("bool_col"),
                            functions.col("id").mod(10).cast("tinyint").alias("tinyint_col"),
                            functions.col("id").mod(10).cast("smallint").alias("smallint_col"),
                            functions.col("id").mod(10).cast("integer").alias("int_col"),
                            functions.col("id").mod(10).multiply(10).cast("bigint").alias("bigint_col"),
                            functions.rand(47).cast("float").alias("float_col"),
                            functions.rand(48).cast("double").alias("double_col"),
                            functions.concat(
                                    functions.lit("str_"),
                                    functions.lpad(functions.col("id").cast("string"), 7, "0")
                            ).alias("string_col")
                    );
            df.coalesce(NUM_PARTITIONS)
              .write()
              .mode("overwrite")
              .parquet(tableDir.toAbsolutePath().toString());
        } finally {
            spark.stop();
        }
        return dir;
    }

    // ── cluster setup ─────────────────────────────────────────────────────────

    private static void startCluster() throws Exception {
        String clusterName = "mrg-test-" + UUID.randomUUID();
        int hzPortA = freePort(), hzPortB = freePort();
        hzA = Hazelcast.newHazelcastInstance(peerHzConfig(clusterName, hzPortA, hzPortA, hzPortB));
        hzB = Hazelcast.newHazelcastInstance(peerHzConfig(clusterName, hzPortB, hzPortA, hzPortB));

        long deadline = System.currentTimeMillis() + 10_000;
        while (hzA.getCluster().getMembers().size() < 2
                && System.currentTimeMillis() < deadline) Thread.sleep(100);
        assertEquals(2, hzA.getCluster().getMembers().size(),
                "Hazelcast cluster must have 2 members before starting servers");

        String dataDir = tempDir.toAbsolutePath().toString();
        LocalFileSystem fs = newLocalFs();

        allocA = new RootAllocator(Long.MAX_VALUE);
        locA = Location.forGrpcInsecure("localhost", freePort());
        serverA = FlightServer.builder(allocA, locA,
                new HadoopFlightSqlService(locA, new ParquetManager(fs, dataDir, "localhost"), allocA, hzA)).build();
        serverA.start();
        clientA = FlightClient.builder(allocA, locA).build();
        sqlClientA = new FlightSqlClient(clientA);

        allocB = new RootAllocator(Long.MAX_VALUE);
        locB = Location.forGrpcInsecure("localhost", freePort());
        serverB = FlightServer.builder(allocB, locB,
                new HadoopFlightSqlService(locB, new ParquetManager(fs, dataDir, "localhost"), allocB, hzB)).build();
        serverB.start();
        clientB = FlightClient.builder(allocB, locB).build();
        sqlClientB = new FlightSqlClient(clientB);
    }

    // ── multi-endpoint helpers ─────────────────────────────────────────────────

    /** Routes to the client matching the endpoint's location port. */
    private static FlightClient chooseClient(FlightEndpoint ep) {
        if (ep.getLocations().isEmpty()) return clientA;
        int port = ep.getLocations().get(0).getUri().getPort();
        return (locB != null && port == locB.getUri().getPort()) ? clientB : clientA;
    }

    /** Sums count(*) across all endpoints in the FlightInfo. */
    private static long totalCountStar(FlightSqlClient sql, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints())
            total += streamCount(chooseClient(ep), ep.getTicket());
        return total;
    }

    private static long streamCount(FlightClient fc, Ticket ticket) throws Exception {
        long total = 0;
        try (FlightStream stream = fc.getStream(ticket)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                for (int i = 0; i < root.getRowCount(); i++) {
                    Object val = root.getVector(0).getObject(i);
                    if (val != null) total += ((Number) val).longValue();
                }
            }
        }
        return total;
    }

    // ── Hazelcast helpers ─────────────────────────────────────────────────────

    private static Config peerHzConfig(String clusterName, int ownPort, int peerPort1, int peerPort2) {
        Config c = new Config();
        c.setClusterName(clusterName);
        c.getNetworkConfig().setPort(ownPort).setPortAutoIncrement(false);
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + peerPort1)
                .addMember("127.0.0.1:" + peerPort2);
        return c;
    }

    // ── misc helpers ──────────────────────────────────────────────────────────

    private static LocalFileSystem newLocalFs() throws IOException {
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(URI.create("file:///"), new Configuration());
        return fs;
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
        catch (IOException e) { throw new RuntimeException(e); }
    }

    private static void closeSilently(Object... resources) {
        for (Object r : resources) {
            if (r == null) continue;
            try {
                if (r instanceof FlightServer fs)           fs.shutdown();
                else if (r instanceof HazelcastInstance hz) hz.shutdown();
                else if (r instanceof AutoCloseable ac)     ac.close();
            } catch (Exception ignored) {}
        }
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
        }
    }
}
