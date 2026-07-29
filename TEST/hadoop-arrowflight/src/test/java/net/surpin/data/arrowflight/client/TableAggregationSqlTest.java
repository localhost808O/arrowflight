package net.surpin.data.arrowflight.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SQL generation in {@link Table} when aggregation is pushed down.
 * No network or Spark context required — only tests the SQL string produced by
 * {@link Table#getQueryStatement()} after {@link Table#probe} with a {@link PushAggregation}.
 */
class TableAggregationSqlTest {

    @Test
    void countStarWithoutGroupByGeneratesCorrectSql() {
        Table table = Table.forTable("my_schema.my_table", "\"");
        PushAggregation agg = new PushAggregation(new String[]{"count(*)"});
        table.probe(null, null, agg, null);

        String sql = table.getQueryStatement().strip();
        assertTrue(sql.toLowerCase().startsWith("select count(*) from my_schema.my_table"),
                "Expected 'select count(*) from ...', got: " + sql);
        assertFalse(sql.toLowerCase().contains("group by"), "Should have no GROUP BY");
    }

    @Test
    void countStarWithGroupByGeneratesCorrectSql() {
        Table table = Table.forTable("my_schema.my_table", "\"");
        // FlightScanBuilder prepends group-by columns to the expressions list
        PushAggregation agg = new PushAggregation(
                new String[]{"\"bool_col\"", "count(*)"},
                new String[]{"\"bool_col\""});
        table.probe(null, null, agg, null);

        String sql = table.getQueryStatement().strip().toLowerCase();
        assertTrue(sql.startsWith("select \"bool_col\",count(*) from my_schema.my_table"),
                "SELECT should include group-by col then count(*), got: " + sql);
        assertTrue(sql.contains("group by"), "Should have GROUP BY clause");
        assertTrue(sql.contains("\"bool_col\""), "GROUP BY should contain bool_col");
    }

    @Test
    void sumMinMaxGenerateCorrectSql() {
        Table table = Table.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(
                new String[]{"sum(\"amount\")", "min(\"id\")", "max(\"id\")"});
        table.probe(null, null, agg, null);

        String sql = table.getQueryStatement().strip().toLowerCase();
        assertTrue(sql.contains("sum(\"amount\")"), "Should contain SUM expr, got: " + sql);
        assertTrue(sql.contains("min(\"id\")"), "Should contain MIN expr, got: " + sql);
        assertTrue(sql.contains("max(\"id\")"), "Should contain MAX expr, got: " + sql);
        assertFalse(sql.contains("group by"), "No GROUP BY expected");
    }

    @Test
    void aggregationWithFilterGeneratesWhereClause() {
        Table table = Table.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(new String[]{"count(*)"});
        table.probe("\"id\" > 0", null, agg, null);

        String sql = table.getQueryStatement().strip().toLowerCase();
        assertTrue(sql.contains("count(*)"), "Should contain count(*), got: " + sql);
        assertTrue(sql.contains("where"), "Should have WHERE clause, got: " + sql);
        assertTrue(sql.contains("\"id\" > 0"), "Should contain filter condition, got: " + sql);
    }

    @Test
    void sumGroupByGeneratesGroupByClause() {
        Table table = Table.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(
                new String[]{"\"category\"", "sum(\"amount\")"},
                new String[]{"\"category\""});
        table.probe(null, null, agg, null);

        String sql = table.getQueryStatement().strip().toLowerCase();
        assertTrue(sql.contains("group by"), "Should have GROUP BY clause, got: " + sql);
        assertTrue(sql.contains("\"category\""), "GROUP BY should contain category");
        assertTrue(sql.contains("sum(\"amount\")"), "SELECT should contain SUM");
    }

    @Test
    void noAggregationDefaultsToSelectStar() {
        Table table = Table.forTable("t", "\"");
        // No probe → default SELECT *
        String sql = table.getQueryStatement().strip().toLowerCase();
        assertTrue(sql.startsWith("select * from t"),
                "Default should be SELECT *, got: " + sql);
    }

    @Test
    void probeReturnsTrueWhenAggregationChangesQuery() {
        Table table = Table.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(new String[]{"count(*)"});
        boolean changed = table.probe(null, null, agg, null);
        assertTrue(changed, "probe() should return true when aggregation changes the query");
    }

    @Test
    void probeReturnsFalseWhenNothingPushed() {
        Table table = Table.forTable("t", "\"");
        boolean changed = table.probe(null, null, null, null);
        assertFalse(changed, "probe() should return false when nothing is pushed");
    }

    @Test
    void probeReturnsFalseForIdenticalAggregation() {
        Table table = Table.forTable("t", "\"");
        PushAggregation agg = new PushAggregation(new String[]{"count(*)"});
        table.probe(null, null, agg, null);
        // Second identical probe should not signal a change
        boolean changed = table.probe(null, null, agg, null);
        assertFalse(changed, "Second identical probe should return false");
    }
}
