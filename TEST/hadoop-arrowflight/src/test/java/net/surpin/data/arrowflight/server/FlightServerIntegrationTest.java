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
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.arrow.memory.DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightServerIntegrationTest {

    private static FlightServer server;
    private static BufferAllocator allocator;
    private static HazelcastInstance hazelcast;
    private static FlightClient flightClient;
    private static FlightSqlClient sqlClient;
    private static Location serverLocation;

    @BeforeAll
    static void startServer() throws Exception {
        System.setProperty(ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        allocator = new RootAllocator(Long.MAX_VALUE);

        // Embedded Hazelcast with networking disabled (standalone mode)
        Config hz = new Config();
        hz.setClusterName("test-" + UUID.randomUUID());
        hz.getNetworkConfig().setPort(findFreePort());
        hz.getNetworkConfig().setPortAutoIncrement(false);
        hz.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        hz.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hazelcast = Hazelcast.newHazelcastInstance(hz);
        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        ParquetManager parquetManager = new ParquetManager(localFs, dataDir, "localhost");

        int port = findFreePort();
        serverLocation = Location.forGrpcInsecure("localhost", port);

        HadoopFlightSqlService sqlService =
                new HadoopFlightSqlService(serverLocation, parquetManager, allocator, hazelcast);
        server = FlightServer.builder(allocator, serverLocation, sqlService).build();
        server.start();

        flightClient = FlightClient.builder(allocator, serverLocation).build();
        sqlClient = new FlightSqlClient(flightClient);
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (sqlClient != null) sqlClient.close();
        if (server != null) server.shutdown();
        if (hazelcast != null) hazelcast.shutdown();
        if (allocator != null) allocator.close();
    }

    @Test
    @Order(1)
    void serverIsReachable() {
        assertNotNull(server);
        assertTrue(server.getPort() > 0);
    }

    @Test
    @Order(2)
    void getCatalogsReturnsOneRow() throws Exception {
        FlightInfo info = sqlClient.getCatalogs();
        List<Integer> rowCounts = collectRowCounts(info);
        int total = rowCounts.stream().mapToInt(Integer::intValue).sum();
        assertEquals(1, total, "Expected exactly one catalog");
    }

    @Test
    @Order(3)
    void getSchemasReturnsTestSchema() throws Exception {
        FlightInfo info = sqlClient.getSchemas(null, null);
        List<String> schemaNames = collectFirstColumnStrings(info);
        // catalog_name is the first column; db_schema_name is the second
        // Check using second column (schema names)
        List<String> allStrings = collectAllStrings(info);
        assertTrue(allStrings.contains("test_schema"),
                "Schema list should contain 'test_schema', got: " + allStrings);
    }

    @Test
    @Order(4)
    void getTableTypesReturnsTABLE() throws Exception {
        FlightInfo info = sqlClient.getTableTypes();
        List<String> types = collectFirstColumnStrings(info);
        assertTrue(types.contains("TABLE"), "Table types should include TABLE");
    }

    @Test
    @Order(5)
    void getTablesReturnsTestTable() throws Exception {
        FlightInfo info = sqlClient.getTables(null, null, null, null, false);
        List<String> allStrings = collectAllStrings(info);
        assertTrue(allStrings.contains("test_table"),
                "Tables list should contain 'test_table', got: " + allStrings);
    }

    @Test
    @Order(6)
    void executeSelectStarReturnsAllRows() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT * FROM test_schema.test_table");
        assertNotNull(info.getSchema(), "FlightInfo should have a schema");
        assertFalse(info.getSchema().getFields().isEmpty(), "Schema should have fields");

        int totalRows = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightClient.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }
        assertTrue(totalRows > 0, "Query should return at least one row");
    }

    @Test
    @Order(7)
    void executeWithFilterReducesRowCount() throws Exception {
        int allRows = countRows(sqlClient.execute("SELECT * FROM test_schema.test_table"));
        int filtered = countRows(sqlClient.execute(
                "SELECT * FROM test_schema.test_table WHERE tinyint_col = 0"));

        assertTrue(filtered > 0, "Filtered result should be non-empty");
        assertTrue(filtered < allRows,
                "Filtered result (" + filtered + ") should be less than full scan (" + allRows + ")");
    }

    @Test
    @Order(8)
    void executeWithColumnProjection() throws Exception {
        FlightInfo info = sqlClient.execute("SELECT id, bool_col FROM test_schema.test_table");
        assertEquals(2, info.getSchema().getFields().size(),
                "Projected query should return exactly 2 columns");
    }

    // ─── helpers ──────────────────────────────────────────────────────────

    private int countRows(FlightInfo info) throws Exception {
        int total = 0;
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightClient.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    total += stream.getRoot().getRowCount();
                }
            }
        }
        return total;
    }

    private List<Integer> collectRowCounts(FlightInfo info) throws Exception {
        List<Integer> counts = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightClient.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    counts.add(stream.getRoot().getRowCount());
                }
            }
        }
        return counts;
    }

    private List<String> collectFirstColumnStrings(FlightInfo info) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightClient.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    if (root.getSchema().getFields().isEmpty()) continue;
                    var vec = root.getVector(0);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object val = vec.getObject(i);
                        if (val != null) values.add(val.toString());
                    }
                }
            }
        }
        return values;
    }

    private List<String> collectAllStrings(FlightInfo info) throws Exception {
        List<String> values = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightClient.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    for (int col = 0; col < root.getSchema().getFields().size(); col++) {
                        var vec = root.getVector(col);
                        for (int row = 0; row < root.getRowCount(); row++) {
                            Object val = vec.getObject(row);
                            if (val != null) values.add(val.toString());
                        }
                    }
                }
            }
        }
        return values;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
