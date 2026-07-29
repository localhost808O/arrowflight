package net.surpin.data.arrowflight.server;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for aggregation (COUNT/SUM/MIN/MAX/GROUP BY) executed
 * end-to-end through a real Arrow Flight SQL server.
 *
 * <p>Two topologies are exercised:
 * <ol>
 *   <li>Single-node: one Flight server backed by the test_db with a single Parquet file.</li>
 *   <li>Two-node: two Flight servers in a shared Hazelcast cluster, each backed by a temp
 *       directory holding TWO copies of the Parquet file. With Path-2 (per-file endpoints)
 *       each file gets its own endpoint — file[0] → server A (round-robin), file[1] → server B.</li>
 * </ol>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AggregationIntegrationTest {

    // ─── single-node infrastructure ───────────────────────────────────────────

    private static BufferAllocator singleAllocator;
    private static HazelcastInstance singleHz;
    private static FlightServer singleServer;
    private static FlightClient singleClient;
    private static FlightSqlClient singleSqlClient;
    private static Location singleLoc;   // needed by chooseClient()

    // ─── two-node infrastructure ──────────────────────────────────────────────

    private static Path twoFileTempDir;
    private static BufferAllocator allocA, allocB;
    private static HazelcastInstance hzA, hzB;
    private static FlightServer serverA, serverB;
    private static FlightClient clientA, clientB;
    private static FlightSqlClient sqlClientA, sqlClientB;
    private static Location locA, locB;   // needed by chooseClient()

    // ─────────────────────────────────────────────────────────────────────────

    @BeforeAll
    static void startAll() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        startSingleNode();
        startTwoNodes();
    }

    @AfterAll
    static void stopAll() throws Exception {
        closeSilently(singleSqlClient, singleServer, singleHz, singleAllocator);
        closeSilently(sqlClientA, sqlClientB, serverA, serverB, hzA, hzB, allocA, allocB);
        if (twoFileTempDir != null) deleteTree(twoFileTempDir);
    }

    // ─── single-node tests ────────────────────────────────────────────────────

    @Test @Order(1)
    void countStarReturnsPositiveRowCount() throws Exception {
        long count = totalCountStar(singleSqlClient, "SELECT count(*) FROM test_schema.test_table");
        assertTrue(count > 0, "count(*) must be > 0");
    }

    @Test @Order(2)
    void countStarWithGroupByBoolColReturnsTwoGroups() throws Exception {
        Map<Object, Long> groups = totalGroupByCount(singleSqlClient,
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        assertEquals(2, groups.size(),
                "bool_col has two distinct values; expected 2 groups, got: " + groups);
        assertTrue(groups.containsKey(true) || groups.containsKey("true"), "Expected a 'true' group");
        assertTrue(groups.containsKey(false) || groups.containsKey("false"), "Expected a 'false' group");
    }

    @Test @Order(3)
    void groupByCountsSumToTotalCount() throws Exception {
        long total = totalCountStar(singleSqlClient, "SELECT count(*) FROM test_schema.test_table");
        Map<Object, Long> groups = totalGroupByCount(singleSqlClient,
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        long groupSum = groups.values().stream().mapToLong(Long::longValue).sum();
        assertEquals(total, groupSum,
                "Sum of per-group counts must equal count(*); total=" + total + " groupSum=" + groupSum);
    }

    @Test @Order(4)
    void minLessOrEqualMax() throws Exception {
        long[] mm = totalMinMax(singleSqlClient,
                "SELECT min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        assertTrue(mm[0] <= mm[1], "min=" + mm[0] + " should be <= max=" + mm[1]);
    }

    @Test @Order(5)
    void countStarWithFilterReturnsFewer() throws Exception {
        long total    = totalCountStar(singleSqlClient, "SELECT count(*) FROM test_schema.test_table");
        long filtered = totalCountStar(singleSqlClient,
                "SELECT count(*) FROM test_schema.test_table WHERE \"tinyint_col\" = 0");
        assertTrue(filtered > 0, "Filtered count must be > 0");
        assertTrue(filtered < total, "Filtered count " + filtered + " must be < total " + total);
    }

    @Test @Order(6)
    void aggregationSchemaHasCorrectColumnCount() throws Exception {
        FlightInfo info = singleSqlClient.execute(
                "SELECT count(*), min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        assertEquals(3, info.getSchema().getFields().size(),
                "Schema should have 3 fields for count(*), min, max");
    }

    @Test @Order(7)
    void sumBigintColIsNonNegative() throws Exception {
        FlightInfo info = singleSqlClient.execute("SELECT sum(bigint_col) FROM test_schema.test_table");
        assertNotNull(info, "sum(bigint_col) should return a FlightInfo");
    }

    // ─── two-node tests ───────────────────────────────────────────────────────

    /**
     * With Path-2, each file has its own endpoint.  Summing count(*) across all
     * endpoints gives the total.  Two independent queries must agree.
     */
    @Test @Order(10)
    void multiNodeCountStarCombinedEqualsTotal() throws Exception {
        long first  = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        long second = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        assertTrue(first > 0, "Expected non-zero total count");
        assertEquals(first, second,
                "Two independent count(*) queries must return the same total; got " + first + " vs " + second);
    }

    /**
     * GROUP BY across two nodes: merged per-group counts must sum to the plain count(*) total.
     */
    @Test @Order(11)
    void multiNodeGroupBySumsToTotal() throws Exception {
        long total = totalCountStar(sqlClientA, "SELECT count(*) FROM test_schema.test_table");
        Map<Object, Long> groups = totalGroupByCount(sqlClientA,
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        long groupSum = groups.values().stream().mapToLong(Long::longValue).sum();
        assertEquals(total, groupSum,
                "Multi-node group-by counts (" + groupSum + ") must sum to total (" + total + ")");
    }

    /**
     * Merged bool_col groups must number exactly 2 regardless of endpoint count.
     */
    @Test @Order(12)
    void multiNodeGroupByProducesExactlyTwoGroups() throws Exception {
        Map<Object, Long> groups = totalGroupByCount(sqlClientA,
                "SELECT bool_col, count(*) FROM test_schema.test_table GROUP BY bool_col");
        assertEquals(2, groups.size(),
                "bool_col has two distinct values; expected 2 merged groups, got: " + groups);
    }

    /**
     * With Path-2 each file has exactly one endpoint — duplication is architecturally
     * impossible.  Verify total rows from all endpoints is stable across two queries.
     */
    @Test @Order(13)
    void multiNodeSelectStarNoRowDuplication() throws Exception {
        long first  = totalRowCount(sqlClientA, "SELECT * FROM test_schema.test_table");
        long second = totalRowCount(sqlClientA, "SELECT * FROM test_schema.test_table");
        assertTrue(first > 0, "Expected non-zero row count");
        assertEquals(first, second,
                "Two independent SELECT * queries must return the same total; got " + first + " vs " + second);
    }

    /**
     * Combined min/max from two-node setup must equal reference from single-node
     * (both use the same source Parquet, so value ranges are identical).
     */
    @Test @Order(14)
    void multiNodeMinEqualsReference() throws Exception {
        long[] ref     = totalMinMax(singleSqlClient,
                "SELECT min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        long[] twoNode = totalMinMax(sqlClientA,
                "SELECT min(bigint_col), max(bigint_col) FROM test_schema.test_table");
        assertEquals(ref[0], twoNode[0], "Combined min must equal reference min");
        assertEquals(ref[1], twoNode[1], "Combined max must equal reference max");
    }

    // ─── infrastructure setup ─────────────────────────────────────────────────

    private static void startSingleNode() throws Exception {
        singleAllocator = new RootAllocator(Long.MAX_VALUE);
        singleHz = Hazelcast.newHazelcastInstance(standaloneHzConfig());

        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        ParquetManager pm = new ParquetManager(newLocalFs(), dataDir, "localhost");

        int port = freePort();
        singleLoc = Location.forGrpcInsecure("localhost", port);
        singleServer = FlightServer.builder(singleAllocator, singleLoc,
                new HadoopFlightSqlService(singleLoc, pm, singleAllocator, singleHz)).build();
        singleServer.start();

        singleClient = FlightClient.builder(singleAllocator, singleLoc).build();
        singleSqlClient = new FlightSqlClient(singleClient);
    }

    private static void startTwoNodes() throws Exception {
        twoFileTempDir = Files.createTempDirectory("agg-two-node-test");
        Path tableDir = twoFileTempDir.resolve("test_schema/test_table");
        Files.createDirectories(tableDir);
        Path src = Paths.get("src/test/resources/test_db/test_schema/test_table")
                .toAbsolutePath()
                .toFile()
                .listFiles((d, n) -> n.endsWith(".parquet"))[0]
                .toPath();
        Files.copy(src, tableDir.resolve("part-0000.snappy.parquet"));
        Files.copy(src, tableDir.resolve("part-0001.snappy.parquet"));

        String clusterName = "agg-test-" + UUID.randomUUID();
        int hzPortA = freePort(), hzPortB = freePort();
        hzA = Hazelcast.newHazelcastInstance(peerHzConfig(clusterName, hzPortA, hzPortA, hzPortB));
        hzB = Hazelcast.newHazelcastInstance(peerHzConfig(clusterName, hzPortB, hzPortA, hzPortB));

        long deadline = System.currentTimeMillis() + 10_000;
        while (hzA.getCluster().getMembers().size() < 2 && System.currentTimeMillis() < deadline)
            Thread.sleep(100);
        assertEquals(2, hzA.getCluster().getMembers().size(),
                "Hazelcast cluster must have 2 members before starting servers");

        String dataDir = twoFileTempDir.toAbsolutePath().toString();
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

    // ─── multi-endpoint routing helpers ──────────────────────────────────────

    /** Routes to the client whose server port matches the endpoint's location. */
    private static FlightClient chooseClient(FlightEndpoint ep) {
        if (ep.getLocations().isEmpty()) return singleClient;
        int port = ep.getLocations().get(0).getUri().getPort();
        if (locB != null && port == locB.getUri().getPort()) return clientB;
        if (locA != null && port == locA.getUri().getPort()) return clientA;
        return singleClient;
    }

    /** Sums count(*) across every endpoint in the FlightInfo. */
    private static long totalCountStar(FlightSqlClient sql, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints())
            total += streamCountStar(chooseClient(ep), ep.getTicket());
        return total;
    }

    /** Total SELECT * row count across every endpoint. */
    private static long totalRowCount(FlightSqlClient sql, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        long total = 0;
        for (FlightEndpoint ep : info.getEndpoints()) {
            try (FlightStream s = chooseClient(ep).getStream(ep.getTicket())) {
                while (s.next()) total += s.getRoot().getRowCount();
            }
        }
        return total;
    }

    /** Merges GROUP BY count results (SUM per key) from every endpoint. */
    private static Map<Object, Long> totalGroupByCount(FlightSqlClient sql, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        Map<Object, Long> merged = new LinkedHashMap<>();
        for (FlightEndpoint ep : info.getEndpoints())
            collectGroupByCount(chooseClient(ep), ep.getTicket())
                    .forEach((k, v) -> merged.merge(k, v, Long::sum));
        return merged;
    }

    /** MIN of mins, MAX of maxes across every endpoint. */
    private static long[] totalMinMax(FlightSqlClient sql, String query) throws Exception {
        FlightInfo info = sql.execute(query);
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (FlightEndpoint ep : info.getEndpoints()) {
            long[] mm = minMaxFromStream(chooseClient(ep), ep.getTicket());
            if (mm[0] < min) min = mm[0];
            if (mm[1] > max) max = mm[1];
        }
        return new long[]{min, max};
    }

    // ─── low-level stream helpers ─────────────────────────────────────────────

    private static long streamCountStar(FlightClient fc, Ticket ticket) throws Exception {
        long total = 0;
        try (FlightStream stream = fc.getStream(ticket)) {
            while (stream.next()) {
                FieldVector v = stream.getRoot().getVector(0);
                for (int i = 0; i < stream.getRoot().getRowCount(); i++)
                    if (!v.isNull(i)) total += ((Number) v.getObject(i)).longValue();
            }
        }
        return total;
    }

    private static Map<Object, Long> collectGroupByCount(FlightClient fc, Ticket ticket) throws Exception {
        Map<Object, Long> result = new LinkedHashMap<>();
        try (FlightStream stream = fc.getStream(ticket)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                FieldVector keyVec   = root.getVector(0);
                FieldVector countVec = root.getVector(1);
                for (int i = 0; i < root.getRowCount(); i++) {
                    Object key   = keyVec.isNull(i)   ? null : keyVec.getObject(i);
                    long   count = countVec.isNull(i) ? 0 : ((Number) countVec.getObject(i)).longValue();
                    result.merge(key, count, Long::sum);
                }
            }
        }
        return result;
    }

    private static long[] minMaxFromStream(FlightClient fc, Ticket ticket) throws Exception {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        try (FlightStream stream = fc.getStream(ticket)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                FieldVector minVec = root.getVector(0);
                FieldVector maxVec = root.getVector(1);
                for (int i = 0; i < root.getRowCount(); i++) {
                    if (!minVec.isNull(i)) { long v = ((Number) minVec.getObject(i)).longValue(); if (v < min) min = v; }
                    if (!maxVec.isNull(i)) { long v = ((Number) maxVec.getObject(i)).longValue(); if (v > max) max = v; }
                }
            }
        }
        return new long[]{min, max};
    }

    // ─── Hazelcast config helpers ─────────────────────────────────────────────

    private static Config standaloneHzConfig() {
        Config c = new Config();
        c.setClusterName("agg-single-" + UUID.randomUUID());
        c.getNetworkConfig().setPort(freePort()).setPortAutoIncrement(false);
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return c;
    }

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

    // ─── misc helpers ─────────────────────────────────────────────────────────

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
