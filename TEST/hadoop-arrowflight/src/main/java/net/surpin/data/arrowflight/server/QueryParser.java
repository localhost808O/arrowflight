package net.surpin.data.arrowflight.server;

import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.*;
import io.substrait.proto.Rel.RelTypeCase;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Парсер SQL запросов с использованием Substrait Plan.
 */
public class QueryParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);

    public final String schema;
    public final String table;
    public final List<String> columns;
    public final Plan protoPlan;

    private QueryParser(String schema, String table, List<String> columns, Plan protoPlan) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.protoPlan = protoPlan;
    }

    /**
     * Парсинг SQL запроса с использованием Substrait.
     */
    public static QueryParser parse(String query) {
        return parse(query, (String) null);
    }

    /**
     * Парсинг SQL запроса с использованием Substrait.
     *
     * @param query     SQL запрос
     * @param schemaDdl DDL для создания схемы (опционально, если null - используется пустая схема)
     */
    public static QueryParser parse(String query, String schemaDdl) {
        Objects.requireNonNull(query);

        final CalciteCatalogReader catalogReader;
        try {
            if (schemaDdl != null && !schemaDdl.isEmpty()) {
                catalogReader = SubstraitCreateStatementParser.processCreateStatementsToCatalog(schemaDdl);
            } else {
                catalogReader = SubstraitCreateStatementParser.processCreateStatementsToCatalog("");
            }
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("Wrong DDL: " + schemaDdl, e);
        }

        return parseInternal(query, catalogReader);
    }

    /**
     * Внутренний метод парсинга с предварительно созданным catalogReader.
     */
    public static QueryParser parseInternal(String query, CalciteCatalogReader catalogReader) {
        LOGGER.debug("Парсинг SQL запроса: {}", query);

        try {
            SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
            io.substrait.plan.Plan substraitPlan = sqlToSubstrait.convert(query, catalogReader);

            final PlanProtoConverter planToProto = new PlanProtoConverter();
            final Plan protoPlan = planToProto.toProto(substraitPlan);

            // Парсинг плана
            return parsePlan(protoPlan);

        } catch (SqlParseException e) {
            LOGGER.error("Ошибка при парсинге SQL запроса", e);
            throw new IllegalArgumentException("Некорректный SQL запрос: " + query, e);
        }
    }

    /**
     * Парсинг Substrait Plan для извлечения информации о запросе.
     */
    private static QueryParser parsePlan(Plan plan) {
        // Substrait plan содержит root rel (final output)
        PlanRel rootPlanRel = plan.getRelations(0);

        // Обход плана для извлечения деталей
        QueryParserInfo info = extractQueryDetails(rootPlanRel);
        String schema = info.schema;
        String table = info.table;
        List<String> columns = info.columns;

        return new QueryParser(schema, table, columns, plan);
    }

    /**
     * Класс для возврата нескольких значений из extractQueryDetails.
     */
    private static class QueryParserInfo {
        String schema;
        String table;
        List<String> columns;
    }

    /**
     * Извлечение деталей запроса из PlanRel.
     */
    private static QueryParserInfo extractQueryDetails(PlanRel planRel) {
        // Проверяем, есть ли root (RelRoot)
        if (planRel.hasRoot()) {
            RelRoot root = planRel.getRoot();
            Rel inputRel = root.getInput();
            // Проверяем тип входного rel - если UPDATE/WRITE/DDL - выбрасываем исключение
            if (inputRel.hasUpdate()) {
                throw new IllegalArgumentException("UPDATE запросы не поддерживаются");
            }
            if (inputRel.hasWrite()) {
                throw new IllegalArgumentException("WRITE запросы не поддерживаются");
            }
            if (inputRel.hasDdl()) {
                throw new IllegalArgumentException("DDL запросы не поддерживаются");
            }
            return extractQueryDetails(inputRel);
        }

        // Иначе берем rel напрямую
        Rel rel = planRel.getRel();
        return extractQueryDetails(rel);
    }

    /**
     * Извлечение деталей запроса из Rel.
     */
    private static QueryParserInfo extractQueryDetails(Rel rel) {
        QueryParserInfo info = new QueryParserInfo();
        info.columns = new ArrayList<>();

        RelTypeCase relTypeCase = rel.getRelTypeCase();

        if (relTypeCase == RelTypeCase.READ) {
            return extractReadRelInfo(rel.getRead());
        } else if (relTypeCase == RelTypeCase.FILTER) {
            return extractFilterRelInfo(rel.getFilter());
        } else if (relTypeCase == RelTypeCase.PROJECT) {
            return extractProjectRelInfo(rel.getProject());
        } else if (relTypeCase == RelTypeCase.FETCH) {
            throw new IllegalArgumentException("LIMIT запросы не поддерживаются");
        } else if (relTypeCase == RelTypeCase.AGGREGATE) {
            throw new IllegalArgumentException("Aggregate queries not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.SORT) {
            throw new IllegalArgumentException("Sort queries not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.JOIN) {
            throw new IllegalArgumentException("Join queries not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.SET) {
            throw new IllegalArgumentException("Set operations not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.EXTENSION_SINGLE ||
                relTypeCase == RelTypeCase.EXTENSION_MULTI ||
                relTypeCase == RelTypeCase.EXTENSION_LEAF) {
            throw new IllegalArgumentException("Extension relations not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.CROSS) {
            throw new IllegalArgumentException("Cross join not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.HASH_JOIN) {
            throw new IllegalArgumentException("Hash join not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.MERGE_JOIN) {
            throw new IllegalArgumentException("Merge join not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.NESTED_LOOP_JOIN) {
            throw new IllegalArgumentException("Nested loop join not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.WINDOW) {
            throw new IllegalArgumentException("Window functions not supported: " + rel);
        } else if (relTypeCase == RelTypeCase.UPDATE) {
            throw new IllegalArgumentException("UPDATE запросы не поддерживаются");
        } else if (relTypeCase == RelTypeCase.WRITE) {
            throw new IllegalArgumentException("WRITE запросы не поддерживаются");
        } else if (relTypeCase == RelTypeCase.DDL) {
            throw new IllegalArgumentException("DDL запросы не поддерживаются");
        } else if (relTypeCase == RelTypeCase.RELTYPE_NOT_SET) {
            LOGGER.warn("Неизвестный тип Rel: {}", relTypeCase);
            return info;
        } else {
            LOGGER.warn("Неизвестный тип Rel: {}", relTypeCase);
            return info;
        }
    }

    /**
     * Извлечение информации из ReadRel.
     */
    private static QueryParserInfo extractReadRelInfo(ReadRel readRel) {
        QueryParserInfo info = new QueryParserInfo();
        info.columns = new ArrayList<>();

        // Извлечение имени таблицы
        if (readRel.hasNamedTable()) {
            ReadRel.NamedTable namedTable = readRel.getNamedTable();
            // Имена таблицы: [schema, table] или просто [table]
            List<String> tableNames = namedTable.getNamesList();
            if (!tableNames.isEmpty()) {
                info.table = tableNames.get(tableNames.size() - 1); // Последнее имя - это имя таблицы
                if (tableNames.size() > 1) {
                    info.schema = tableNames.get(tableNames.size() - 2); // Предпоследнее - схема
                }
            }
        }

        // Извлечение колонок из baseSchema ( NamedStruct )
        if (readRel.hasBaseSchema()) {
            NamedStruct baseSchema = readRel.getBaseSchema();
            info.columns.addAll(baseSchema.getNamesList());
        }

        return info;
    }

    /**
     * Извлечение информации из FilterRel.
     */
    private static QueryParserInfo extractFilterRelInfo(FilterRel filterRel) {
        return extractQueryDetails(filterRel.getInput());
    }

    /**
     * Извлечение информации из ProjectRel.
     */
    private static QueryParserInfo extractProjectRelInfo(ProjectRel projectRel) {
        QueryParserInfo info = new QueryParserInfo();
        info.columns = new ArrayList<>();

        // Получаем информацию из входного rel
        QueryParserInfo inputInfo = extractQueryDetails(projectRel.getInput());

        // Имена колонок из базовой схемы
        List<String> inputColumnNames = inputInfo.columns;

        // Извлекаем колонки из проекции
        // expressions содержит Expression для каждой выходной колонки
        List<Expression> expressions = projectRel.getExpressionsList();
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);

            // Проверка на оконную функцию
            if (expr.hasWindowFunction()) {
                throw new IllegalArgumentException("Оконные функции не поддерживаются");
            }

            // Проверяем, является ли выражение FieldReference
            if (expr.hasSelection()) {
                Expression.FieldReference fieldRef = expr.getSelection();
                if (fieldRef.hasDirectReference()) {
                    Expression.ReferenceSegment refSegment = fieldRef.getDirectReference();

                    // Извлекаем индекс колонки из StructField
                    if (refSegment.hasStructField()) {
                        Expression.ReferenceSegment.StructField structField = refSegment.getStructField();
                        int colIndex = structField.getField();

                        // Получаем имя колонки по индексу
                        if (colIndex >= 0 && colIndex < inputColumnNames.size()) {
                            info.columns.add(inputColumnNames.get(colIndex));
                        } else {
                            info.columns.add("col_" + i); // заглушка
                        }
                    } else {
                        info.columns.add("col_" + i); // заглушка
                    }
                } else {
                    info.columns.add("col_" + i); // заглушка
                }
            } else {
                info.columns.add("col_" + i); // заглушка
            }
        }

        info.schema = inputInfo.schema;
        info.table = inputInfo.table;

        return info;
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
     * Получение proto плана.
     */
    public Plan getProtoPlan() {
        return protoPlan;
    }

    public static void main(String[] args) throws Exception {
        String sql = "SELECT * from nation";
        String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
                "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";

        QueryParser parser = QueryParser.parse(sql, nation);
        System.out.println("Schema: " + parser.getSchema());
        System.out.println("Table: " + parser.getTable());
        System.out.println("Columns: " + parser.getColumns());
        System.out.println("Proto Plan: " + parser.getProtoPlan());

        // Test with specific columns
        String sql2 = "SELECT N_NAME, N_REGIONKEY from nation WHERE N_NATIONKEY > 5";
        QueryParser parser2 = QueryParser.parse(sql2, nation);
        System.out.println("\n--- Test 2 ---");
        System.out.println("Schema: " + parser2.getSchema());
        System.out.println("Table: " + parser2.getTable());
        System.out.println("Columns: " + parser2.getColumns());
        System.out.println("Proto Plan: " + parser2.getProtoPlan());
    }
}
