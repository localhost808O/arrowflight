package net.surpin.data.arrowflight.server;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class MetadataUtilsTest {

    // ─── createLikePredicate ───────────────────────────────────────────────

    @Test
    void nullPatternMatchesEverything() {
        Predicate<String> p = MetadataUtils.createLikePredicate(null);
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
        assertTrue(p.test("  spaces  "));
    }

    @Test
    void exactPatternMatchesOnlyItself() {
        Predicate<String> p = MetadataUtils.createLikePredicate("test_schema");
        assertTrue(p.test("test_schema"));
        assertFalse(p.test("test_schema2"));
        assertFalse(p.test("TEST_SCHEMA"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnySuffix() {
        Predicate<String> p = MetadataUtils.createLikePredicate("test%");
        assertTrue(p.test("test"));
        assertTrue(p.test("test_schema"));
        assertTrue(p.test("testing123"));
        assertFalse(p.test("my_test"));
        assertFalse(p.test(""));
    }

    @Test
    void percentWildcardMatchesAnyPrefix() {
        Predicate<String> p = MetadataUtils.createLikePredicate("%_table");
        assertTrue(p.test("my_table"));
        assertTrue(p.test("some_other_table"));
        assertFalse(p.test("table"));
    }

    @Test
    void percentOnlyMatchesAll() {
        Predicate<String> p = MetadataUtils.createLikePredicate("%");
        assertTrue(p.test("any_string"));
        assertTrue(p.test(""));
    }

    @Test
    void underscoreMatchesSingleCharacter() {
        Predicate<String> p = MetadataUtils.createLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("ts"));
        assertFalse(p.test("teest"));
    }

    @Test
    void mixedWildcards() {
        Predicate<String> p = MetadataUtils.createLikePredicate("t%_col");
        assertTrue(p.test("tinyint_col"));
        assertTrue(p.test("t_col"));
        assertFalse(p.test("int_col"));
    }

    @Test
    void regexSpecialCharsInPatternAreEscaped() {
        // The pattern "a.b" should match literal "a.b" only, not "axb"
        Predicate<String> p = MetadataUtils.createLikePredicate("a.b");
        assertTrue(p.test("a.b"));
        assertFalse(p.test("axb"));
        assertFalse(p.test("a.bc"));
    }

    // ─── getCatalogsRoot ──────────────────────────────────────────────────

    @Test
    void getCatalogsRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = MetadataUtils.getCatalogsRoot(allocator);
            assertEquals(1, root.getRowCount(), "Catalogs should have exactly one row");
            root.close();
        }
    }

    // ─── getSchemasRoot ───────────────────────────────────────────────────

    @Test
    void getSchemasRootReturnsOneRowPerSchema() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = MetadataUtils.getSchemasRoot(
                    List.of("schema_a", "schema_b"), allocator);
            assertEquals(2, root.getRowCount());
            root.close();
        }
    }

    @Test
    void getSchemasRootHandlesEmptyCollection() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = MetadataUtils.getSchemasRoot(List.of(), allocator);
            assertEquals(0, root.getRowCount());
            root.close();
        }
    }

    // ─── getTableTypesRoot ────────────────────────────────────────────────

    @Test
    void getTableTypesRootReturnsSingleRow() {
        try (BufferAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot root = MetadataUtils.getTableTypesRoot(allocator);
            assertEquals(1, root.getRowCount());
            root.close();
        }
    }

    // ─── buildTypeInfo ────────────────────────────────────────────────────

    @Test
    void buildTypeInfoContainsExpectedTypes() {
        var types = ParquetSchemaConverter.buildTypeInfo();
        assertFalse(types.isEmpty());

        List<String> typeNames = types.stream()
                .map(ParquetSchemaConverter.TypeInfo::typeName)
                .toList();
        assertTrue(typeNames.contains("BIGINT"));
        assertTrue(typeNames.contains("VARCHAR"));
        assertTrue(typeNames.contains("BOOLEAN"));
        assertTrue(typeNames.contains("TIMESTAMP"));
        assertTrue(typeNames.contains("DATE"));
        assertTrue(typeNames.contains("TINYINT"));
        assertTrue(typeNames.contains("SMALLINT"));
        assertTrue(typeNames.contains("INTEGER"));
        assertTrue(typeNames.contains("DOUBLE"));
        assertTrue(typeNames.contains("REAL"));
    }

    @Test
    void buildTypeInfoIsSortedByDataType() {
        var types = ParquetSchemaConverter.buildTypeInfo();
        for (int i = 1; i < types.size(); i++) {
            int prev = types.get(i - 1).dataType();
            int curr = types.get(i).dataType();
            assertTrue(prev <= curr,
                    "Types should be sorted by dataType: " + types.get(i - 1).typeName()
                            + "(" + prev + ") > " + types.get(i).typeName() + "(" + curr + ")");
        }
    }
}
