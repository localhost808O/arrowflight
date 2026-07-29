package net.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests aggregation parsing in {@link ParquetQueryParser}: hasAggregation flag,
 * groupByColumnNames extraction, and selectExprs classification.
 */
class ParquetQueryParserAggregationTest {

    @Test
    void countStarSetsHasAggregationTrue() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT count(*) FROM s.t");
        assertTrue(p.hasAggregation, "count(*) should set hasAggregation=true");
    }

    @Test
    void countStarProducesCountStarExpr() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT count(*) FROM s.t");
        assertEquals(1, p.selectExprs.size());
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR, e.func);
        assertNull(e.inputColumn, "COUNT_STAR has no input column");
        assertEquals("count(*)", e.outputName);
    }

    @Test
    void countOfColumnProducesCountExpr() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT count(id) FROM s.t");
        assertTrue(p.hasAggregation);
        assertEquals(1, p.selectExprs.size());
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COUNT, e.func);
        assertEquals("id", e.inputColumn);
    }

    @Test
    void sumProducesSumExpr() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT sum(amount) FROM s.t");
        assertTrue(p.hasAggregation);
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.SUM, e.func);
        assertEquals("amount", e.inputColumn);
    }

    @Test
    void minProducesMinExpr() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT min(id) FROM s.t");
        assertTrue(p.hasAggregation);
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.MIN, e.func);
        assertEquals("id", e.inputColumn);
    }

    @Test
    void maxProducesMaxExpr() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT max(id) FROM s.t");
        assertTrue(p.hasAggregation);
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.MAX, e.func);
        assertEquals("id", e.inputColumn);
    }

    @Test
    void groupByColumnNamesExtracted() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT bool_col, count(*) FROM s.t GROUP BY bool_col");
        assertTrue(p.hasAggregation);
        assertEquals(List.of("bool_col"), p.groupByColumnNames);
    }

    @Test
    void groupByMultipleColumns() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT a, b, count(*) FROM s.t GROUP BY a, b");
        assertEquals(List.of("a", "b"), p.groupByColumnNames);
    }

    @Test
    void groupByAloneSetsHasAggregation() {
        // GROUP BY without explicit aggregate functions still signals aggregation
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT bool_col FROM s.t GROUP BY bool_col");
        assertTrue(p.hasAggregation, "GROUP BY alone should set hasAggregation=true");
    }

    @Test
    void selectStarHasNoAggregation() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT * FROM s.t");
        assertFalse(p.hasAggregation);
        assertTrue(p.groupByColumnNames.isEmpty());
    }

    @Test
    void plainColumnSelectHasNoAggregation() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT id, name FROM s.t");
        assertFalse(p.hasAggregation);
        assertEquals(2, p.selectExprs.size());
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COLUMN, p.selectExprs.get(0).func);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COLUMN, p.selectExprs.get(1).func);
    }

    @Test
    void mixedGroupByAndAggregateExprs() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT bool_col, count(*), sum(int_col) FROM s.t GROUP BY bool_col");
        assertTrue(p.hasAggregation);
        assertEquals(3, p.selectExprs.size());
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COLUMN,    p.selectExprs.get(0).func);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR, p.selectExprs.get(1).func);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.SUM,        p.selectExprs.get(2).func);
        assertEquals("int_col", p.selectExprs.get(2).inputColumn);
    }

    @Test
    void countStarWithFilterExtractsFilter() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT count(*) FROM s.t WHERE id > 0");
        assertTrue(p.hasAggregation);
        assertNotNull(p.filter);
        assertFalse(p.filter.trim().isEmpty(), "Filter should not be empty");
        assertTrue(p.filter.contains("\"id\""), "Filter column should be double-quoted");
    }

    @Test
    void groupByColumnNamesAreUnquoted() {
        // Even when the input uses quoted identifiers, groupByColumnNames renders bare names
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT \"bool_col\", count(*) FROM s.t GROUP BY \"bool_col\"");
        assertEquals(List.of("bool_col"), p.groupByColumnNames,
                "Group-by names should be unquoted bare identifiers");
    }

    @Test
    void countOneIsAnAggregate() {
        // COUNT(1) may be treated as COUNT_STAR or COUNT depending on jOOQ rendering,
        // but it must be detected as an aggregation either way.
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT count(1) FROM s.t");
        assertTrue(p.hasAggregation, "count(1) should set hasAggregation=true");
        assertEquals(1, p.selectExprs.size());
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertTrue(e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR
                        || e.func == ParquetQueryParser.SelectExpr.AggFunc.COUNT,
                "count(1) should map to COUNT_STAR or COUNT, got: " + e.func);
    }

    // ── Regression tests for jOOQ 3.19+ instanceof fix ──────────────────────
    // In jOOQ 3.19+ aggregate classes implement OptionallyOrderedAggregateFunction,
    // NOT org.jooq.AggregateFunction. Detection must use rendered SQL strings.

    @Test
    void uppercaseCountStarIsDetectedAsAggregate() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT COUNT(*) FROM s.t");
        assertTrue(p.hasAggregation, "COUNT(*) uppercase input must set hasAggregation=true");
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.COUNT_STAR, p.selectExprs.get(0).func);
    }

    @Test
    void uppercaseSumIsDetectedAsAggregate() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT SUM(amount) FROM s.t");
        assertTrue(p.hasAggregation, "SUM() uppercase input must set hasAggregation=true");
        ParquetQueryParser.SelectExpr e = p.selectExprs.get(0);
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.SUM, e.func);
        assertEquals("amount", e.inputColumn);
    }

    @Test
    void uppercaseMinIsDetectedAsAggregate() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT MIN(id) FROM s.t");
        assertTrue(p.hasAggregation, "MIN() uppercase input must set hasAggregation=true");
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.MIN, p.selectExprs.get(0).func);
    }

    @Test
    void uppercaseMaxIsDetectedAsAggregate() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT MAX(id) FROM s.t");
        assertTrue(p.hasAggregation, "MAX() uppercase input must set hasAggregation=true");
        assertEquals(ParquetQueryParser.SelectExpr.AggFunc.MAX, p.selectExprs.get(0).func);
    }

    @Test
    void countStarOutputNameIsCountStar() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT count(*) FROM s.t");
        assertEquals("count(*)", p.selectExprs.get(0).outputName);
    }

    @Test
    void sumOutputNameContainsColumnName() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT sum(revenue) FROM s.t");
        String outputName = p.selectExprs.get(0).outputName;
        assertTrue(outputName.toLowerCase().contains("revenue"),
                "SUM outputName should include the column name, got: " + outputName);
    }

    @Test
    void minInputColumnIsExtracted() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT min(price) FROM s.t");
        assertEquals("price", p.selectExprs.get(0).inputColumn);
    }

    @Test
    void maxInputColumnIsExtracted() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT max(score) FROM s.t");
        assertEquals("score", p.selectExprs.get(0).inputColumn);
    }

    @Test
    void selectExprCountMatchesSelectListWidth() {
        ParquetQueryParser p = ParquetQueryParser.parse(
                "SELECT a, b, count(*), sum(c) FROM s.t GROUP BY a, b");
        assertEquals(4, p.selectExprs.size(),
                "selectExprs must have one entry per SELECT expression");
    }

    @Test
    void countStarInputColumnIsNull() {
        ParquetQueryParser p = ParquetQueryParser.parse("SELECT COUNT(*) FROM s.t");
        assertNull(p.selectExprs.get(0).inputColumn,
                "COUNT_STAR has no input column — inputColumn must be null");
    }
}
