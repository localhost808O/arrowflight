package net.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class ParquetManagerIntegrationTest {

    private static ParquetManager manager;

    @BeforeAll
    static void setUp() throws IOException {
        String dataDir = Paths.get("src/test/resources/test_db").toAbsolutePath().toString();
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        manager = new ParquetManager(localFs, dataDir, "localhost");
    }

    @Test
    void getTableSchemaReturnsNonEmptySchema() {
        Schema schema = manager.getTableSchema("test_schema", "test_table");
        assertNotNull(schema);
        assertFalse(schema.getFields().isEmpty(), "Schema should have at least one field");
    }

    @Test
    void getTableSchemaColumnNamesArePresent() {
        Schema schema = manager.getTableSchema("test_schema", "test_table");
        List<String> names = schema.getFields().stream().map(f -> f.getName()).toList();
        assertTrue(names.contains("id"));
        assertTrue(names.contains("bool_col"));
        assertTrue(names.contains("tinyint_col"));
    }

    @Test
    void getTableSchemaWithColumnFilterReturnsSubset() {
        List<String> requested = List.of("id", "bool_col");
        Schema schema = manager.getTableSchema("test_schema", "test_table", requested);
        assertEquals(2, schema.getFields().size());
        List<String> names = schema.getFields().stream().map(f -> f.getName()).toList();
        assertTrue(names.contains("id") && names.contains("bool_col"));
    }

    @Test
    void getTableSchemaEmptyColumnListReturnsAll() {
        Schema full = manager.getTableSchema("test_schema", "test_table");
        Schema withEmpty = manager.getTableSchema("test_schema", "test_table", List.of());
        assertEquals(full.getFields().size(), withEmpty.getFields().size());
    }

    @Test
    void getQuerySchemaSelectStarReturnsAllColumns() {
        Schema full = manager.getTableSchema("test_schema", "test_table");
        Schema fromQuery = manager.getQuerySchema("SELECT * FROM test_schema.test_table");
        assertEquals(full.getFields().size(), fromQuery.getFields().size());
    }

    @Test
    void getQuerySchemaWithProjection() {
        Schema schema = manager.getQuerySchema(
                "SELECT id, bool_col FROM test_schema.test_table");
        assertEquals(2, schema.getFields().size());
    }

    @Test
    void locationsForQueryReturnsRelativePaths() throws IOException {
        Map<String, Set<String>> locations = manager.locationsForQuery(
                "SELECT * FROM test_schema.test_table");
        assertFalse(locations.isEmpty(), "Should find at least one parquet file");
        for (String path : locations.keySet()) {
            assertFalse(path.startsWith("/"), "Paths should be relative: " + path);
        }
    }

    @Test
    void getSchemasReturnsTestSchema() throws IOException {
        Map<String, ?> schemas = manager.getSchemas(null);
        assertTrue(schemas.containsKey("test_schema"));
    }

    @Test
    void readParquetFullScanReturnsRows() throws Exception {
        CountingListener listener = new CountingListener();
        try (BufferAllocator allocator = new RootAllocator()) {
            manager.readParquet(allocator,
                    "SELECT * FROM test_schema.test_table", null, listener, true);
        }
        assertTrue(listener.totalRows > 0, "Should have read at least one row");
    }

    @Test
    void readParquetWithFilterReturnsFewer() throws Exception {
        CountingListener all = new CountingListener();
        CountingListener filtered = new CountingListener();

        try (BufferAllocator allocator = new RootAllocator()) {
            manager.readParquet(allocator,
                    "SELECT * FROM test_schema.test_table", null, all, true);
        }
        try (BufferAllocator allocator = new RootAllocator()) {
            manager.readParquet(allocator,
                    "SELECT * FROM test_schema.test_table WHERE \"tinyint_col\" = 0",
                    null, filtered, true);
        }

        assertTrue(filtered.totalRows > 0, "Filtered result should be non-empty");
        assertTrue(filtered.totalRows < all.totalRows,
                "Filtered should return fewer rows than full scan");
    }

    // ─── minimal ServerStreamListener stub ────────────────────────────────

    static class CountingListener implements ServerStreamListener {
        int totalRows;
        VectorSchemaRoot root;

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider provider, IpcOption option) {
            this.root = root;
        }

        @Override
        public void putNext() {
            totalRows += root.getRowCount();
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            totalRows += root.getRowCount();
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {}

        @Override
        public boolean isReady() { return true; }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public void setOnCancelHandler(Runnable handler) {}

        @Override
        public void error(Throwable ex) { throw new RuntimeException(ex); }

        @Override
        public void completed() {}
    }
}
