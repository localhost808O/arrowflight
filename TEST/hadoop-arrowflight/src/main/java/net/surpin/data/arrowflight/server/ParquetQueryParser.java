package net.surpin.data.arrowflight.server;

import org.jooq.*;
import org.jooq.conf.ParseNameCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Парсер SQL запросов с использованием jOOQ Parser.
 */
public class ParquetQueryParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetQueryParser.class);

    /** One entry per SELECT output column, describing how it is computed. */
    public static final class SelectExpr {
        public enum AggFunc { COLUMN, COUNT_STAR, COUNT, SUM, MIN, MAX }

        /** What kind of expression this is. */
        public final AggFunc func;
        /** Unquoted input column name (null for COUNT_STAR; may equal outputName for COLUMN). */
        public final String inputColumn;
        /** Name to use for the output Arrow field (e.g. "count(*)", "min(id)", "bool_col"). */
        public final String outputName;

        SelectExpr(AggFunc func, String inputColumn, String outputName) {
            this.func = func;
            this.inputColumn = inputColumn;
            this.outputName = outputName;
        }
    }

    public final String schema;
    public final String table;
    public final List<String> columns;
    public final String filter;

    /** True when the query contains aggregate functions or a GROUP BY clause. */
    public final boolean hasAggregation;
    /** Unquoted GROUP BY column names, in order. Empty when there is no GROUP BY. */
    public final List<String> groupByColumnNames;
    /** One entry per SELECT output expression, in order. */
    public final List<SelectExpr> selectExprs;

    private ParquetQueryParser(String schema, String table, List<String> columns, String filter,
            boolean hasAggregation, List<String> groupByColumnNames, List<SelectExpr> selectExprs) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.filter = filter;
        this.hasAggregation = hasAggregation;
        this.groupByColumnNames = Collections.unmodifiableList(groupByColumnNames);
        this.selectExprs = Collections.unmodifiableList(selectExprs);
    }
    
    /**
     * Парсинг SQL запроса с использованием jOOQ.
     */
    public static ParquetQueryParser parse(String query) {
        // The Spark DataSource V2 connector wraps user queries as "select COLS from (QUERY) t [where FILTER]"
        // when the table option contains a full SELECT statement. Flatten this before jOOQ parsing.
        String flattened = flattenSubqueryWrapper(query);
        if (!flattened.equals(query)) {
            LOGGER.info("Flattened subquery wrapper; parsing: {}", flattened);
            return parse(flattened);
        }

        LOGGER.info("Парсинг SQL запроса: {}", query);

        // Создание DSL контекста с парсером
        Settings settings = new Settings()
                .withRenderFormatted(true)
                .withParseNameCase(ParseNameCase.AS_IS);
        DSLContext ctx = DSL.using(SQLDialect.DEFAULT, settings);
        
        try {
            // Парсинг запроса
            Query parsedQuery = ctx.parser().parseQuery(query);
            
            if (parsedQuery instanceof Select<?> select) {
                ParquetQueryParser parser = parseSelect(select);
                LOGGER.info("Parsed query: {}", parser);
                return parser;
            } else {
                throw new IllegalArgumentException("Поддерживаются только SELECT запросы: " + query);
            }
            
        } catch (Exception e) {
            LOGGER.error("Ошибка при парсинге SQL запроса", e);
            throw new RuntimeException("Некорректный SQL запрос: " + query, e);
        }
    }
    
    /**
     * Парсинг Select запроса.
     */
    private static ParquetQueryParser parseSelect(Select<?> select) {
        List<? extends Table<?>> fromTables = select.$from();
        if (fromTables.size() != 1) {
            throw new IllegalArgumentException("Can only select from ONE table, but got query: " + select.getSQL());
        }

        // DSL context that renders identifiers WITHOUT SQL quotes → bare column names.
        DSLContext noQuoteCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.NEVER));

        String schema = Optional.ofNullable(fromTables.get(0).getSchema()).map(Schema::getName).orElse(null);
        String table = fromTables.get(0).getName();

        // Parse GROUP BY — render each GroupField without quotes to get the bare column name.
        List<String> gbCols = new ArrayList<>();
        for (GroupField gf : select.$groupBy()) {
            gbCols.add(noQuoteCtx.render(gf).trim());
        }

        // Parse SELECT fields — populate both the legacy `columns` list and the richer `selectExprs`.
        List<String> columns = new ArrayList<>();
        List<SelectExpr> exprs = new ArrayList<>();
        boolean hasAgg = !gbCols.isEmpty();

        // Must use $select() (raw parse tree), NOT select.fields().
        // select.fields() returns TableFieldImpl wrappers that renderInlined as
        // "alias_XXXXXXXX.name", hiding the actual aggregate expression.
        // $select() gives the real Count/Sum/Min/Max objects with correct rendering.
        for (SelectFieldOrAsterisk sfoa : select.$select()) {
            if (!(sfoa instanceof org.jooq.Field<?>)) {
                // Asterisk / QualifiedAsterisk (SELECT *): leave columns empty → all columns
                continue;
            }
            org.jooq.Field<?> f = (org.jooq.Field<?>) sfoa;
            columns.add(f.getName());

            // Use jOOQ QOM (Query Object Model) typed AST nodes to identify aggregate functions.
            // Verified against jOOQ 3.21.1: COUNT(*) → QOM.Count with $field()==null,
            // COUNT(literal) → QOM.Count with $field() instanceof Param, COUNT(col) → QOM.Count
            // with a non-Param field. QOM.Sum is non-generic; QOM.Min/Max are generic.
            String outputName = noQuoteCtx.renderInlined(f).trim();

            if (f instanceof org.jooq.impl.QOM.Count count) {
                hasAgg = true;
                org.jooq.Field<?> arg = count.$field();
                // null field = COUNT(*); Param field = COUNT(1), COUNT(2), etc. → same as COUNT(*)
                if (arg == null || arg instanceof org.jooq.Param<?>) {
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT_STAR, null, "count(*)"));
                } else {
                    String col = noQuoteCtx.renderInlined(arg).trim();
                    exprs.add(new SelectExpr(SelectExpr.AggFunc.COUNT, col, outputName));
                }
            } else if (f instanceof org.jooq.impl.QOM.Sum sum) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(sum.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.SUM, col, outputName));
            } else if (f instanceof org.jooq.impl.QOM.Min<?> min) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(min.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.MIN, col, outputName));
            } else if (f instanceof org.jooq.impl.QOM.Max<?> max) {
                hasAgg = true;
                String col = noQuoteCtx.renderInlined(max.$field()).trim();
                exprs.add(new SelectExpr(SelectExpr.AggFunc.MAX, col, outputName));
            } else {
                exprs.add(new SelectExpr(SelectExpr.AggFunc.COLUMN, outputName, outputName));
            }
        }

        // WHERE filter — always render with ALWAYS-quoted identifiers for Substrait compatibility.
        Condition where = select.$where();
        DSLContext quotedCtx = DSL.using(SQLDialect.DEFAULT,
                new Settings().withRenderQuotedNames(RenderQuotedNames.ALWAYS));
        String filter = quotedCtx.renderInlined(where);

        return new ParquetQueryParser(schema, table, columns, filter, hasAgg, gbCols, exprs);
    }
    
    /**
     * Получение имени схемы.
     */
    public String getSchema() {
        return schema;
    }
    
    /**
     * Получение имени таблицы.
     */
    public String getTable() {
        return table;
    }
    
    /**
     * Получение списка колонок (пустой список означает все колонки).
     */
    public List<String> getColumns() {
        return columns;
    }
    
    /**
     * Получение фильтра WHERE.
     */
    public String getFilter() {
        return filter;
    }

    /**
     * If the query has the form "select COLS from (INNER) alias [where OUTER_FILTER]" —
     * the wrapper the Spark DataSource V2 connector adds when the table option is a full
     * SELECT statement — rebuild a flat query against the real table:
     * "select COLS from schema.table [where INNER_FILTER AND OUTER_FILTER]".
     *
     * Returns the original query string unchanged when the pattern is not matched.
     */
    private static String flattenSubqueryWrapper(String query) {
        String q = query.trim();

        // Locate "from (" using case-insensitive search.
        java.util.regex.Matcher fromParen =
                java.util.regex.Pattern.compile("(?i)\\bfrom\\s*\\(").matcher(q);
        if (!fromParen.find()) return q;

        int openParen = fromParen.end() - 1;           // index of the '('
        String beforeFrom = q.substring(0, fromParen.start()); // "select COLS "

        // Walk forward to find the matching closing ')'.
        int depth = 1, i = openParen + 1;
        while (i < q.length() && depth > 0) {
            char c = q.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            i++;
        }
        if (depth != 0) return q; // unbalanced — don't touch

        String innerSQL  = q.substring(openParen + 1, i - 1).trim();
        String afterParen = q.substring(i).trim(); // "alias [where FILTER]"

        // The first token after ')' must be a plain identifier (the alias).
        java.util.regex.Matcher aliasM =
                java.util.regex.Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)(.*)$",
                        java.util.regex.Pattern.DOTALL).matcher(afterParen);
        if (!aliasM.matches()) return q; // no alias found — leave unchanged
        String outerRest = aliasM.group(2).trim(); // everything after alias: "[where FILTER]"

        // Inner query must itself be a SELECT (sanity check).
        if (!innerSQL.trim().toLowerCase().startsWith("select")) return q;

        // Find the real "schema.table" reference inside the inner SELECT using a simple regex.
        // Pattern: FROM word.word or FROM word (supports quoted and unquoted identifiers)
        java.util.regex.Matcher innerFrom =
                java.util.regex.Pattern.compile("(?i)\\bfrom\\s+(\\S+)").matcher(innerSQL);
        if (!innerFrom.find()) return q;
        String realTableRef = innerFrom.group(1).replaceAll("\\s*$", "");

        // Any WHERE in the inner query.
        String innerWhere = "";
        java.util.regex.Matcher innerWhereM =
                java.util.regex.Pattern.compile("(?i)\\bwhere\\b(.+)$",
                        java.util.regex.Pattern.DOTALL).matcher(innerSQL);
        if (innerWhereM.find()) {
            innerWhere = innerWhereM.group(1).trim();
        }

        // Build the flat query.
        StringBuilder flat = new StringBuilder(beforeFrom);
        flat.append("from ").append(realTableRef);

        // Merge filters.
        String outerWhere = "";
        if (outerRest.toLowerCase().startsWith("where ")) {
            outerWhere = outerRest.substring(5).trim();
        } else if (!outerRest.isEmpty()) {
            // Unexpected trailing content — bail out to avoid producing broken SQL.
            return q;
        }

        boolean hasInner = !innerWhere.isEmpty();
        boolean hasOuter = !outerWhere.isEmpty();
        if (hasInner && hasOuter) {
            flat.append(" where (").append(innerWhere).append(") and (").append(outerWhere).append(")");
        } else if (hasInner) {
            flat.append(" where ").append(innerWhere);
        } else if (hasOuter) {
            flat.append(" where ").append(outerWhere);
        }

        return flat.toString();
    }

    @Override
    public String toString() {
        return "ParquetQueryParser{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                ", filter='" + filter + '\'' +
                ", hasAggregation=" + hasAggregation +
                ", groupByColumnNames=" + groupByColumnNames +
                '}';
    }

    //    /**
//     * https://github.com/substrait-io/substrait-java/blob/main/examples/isthmus-api/src/main/java/io/substrait/examples/FromSql.java
//     *
//     */
//    public static Plan getPlan() throws SqlParseException {
//        String sql = "SELECT * from nation";
//        String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
//                "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
//        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
//
//        final CalciteCatalogReader catalogReader = SubstraitCreateStatementParser.processCreateStatementsToCatalog(nation);
//        Plan substraitPlan = sqlToSubstrait.convert(sql, catalogReader);
//
//        // Create the proto plan to display to stdout - as it has a better format
//        final PlanProtoConverter planToProto = new PlanProtoConverter();
//        final io.substrait.proto.Plan protoPlan = planToProto.toProto(substraitPlan);
//        System.out.println(protoPlan);
//
//        byte[] buffer = protoPlan.toByteArray();
//
//        return substraitPlan;
//    }
//
//    public static void main(String[] args) throws Exception {
//        System.out.print(getPlan());
//    }
}
