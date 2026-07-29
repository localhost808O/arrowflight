package net.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Тесты для QueryParser.
 */
class QueryParserTest {

    // DDL для тестовой схемы
    private static final String NATION_DDL = "CREATE TABLE NATION (" +
            "N_NATIONKEY BIGINT NOT NULL, " +
            "N_NAME CHAR(25), " +
            "N_REGIONKEY BIGINT NOT NULL, " +
            "N_COMMENT VARCHAR(152))";

    private static final String ORDERS_DDL = "CREATE TABLE ORDERS (" +
            "O_ORDERKEY BIGINT NOT NULL, " +
            "O_CUSTKEY BIGINT NOT NULL, " +
            "O_ORDERSTATUS CHAR(1), " +
            "O_TOTALPRICE DOUBLE, " +
            "O_ORDERDATE DATE)";

    /**
     * Проверка парсинга простого SELECT *.
     */
    @Test
    void testSimpleSelectAll() {
        String sql = "SELECT * FROM NATION";
        QueryParser parser = QueryParser.parse(sql, NATION_DDL);

        assertNotNull(parser);
        assertNull(parser.getSchema()); // В DDL не указано имя схемы
        assertEquals("NATION", parser.getTable());
        assertEquals(List.of("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"), parser.getColumns());
        assertNotNull(parser.getProtoPlan());
    }

    /**
     * Проверка парсинга SELECT с конкретными колонками.
     */
    @Test
    void testSelectWithColumns() {
        String sql = "SELECT N_NAME, N_REGIONKEY FROM NATION";
        QueryParser parser = QueryParser.parse(sql, NATION_DDL);

        assertNotNull(parser);
        assertEquals("NATION", parser.getTable());
        assertEquals(List.of("N_NAME", "N_REGIONKEY"), parser.getColumns());
    }

    /**
     * Проверка парсинга SELECT с фильтром (WHERE).
     */
    @Test
    void testSelectWithFilter() {
        String sql = "SELECT N_NAME FROM NATION WHERE N_NATIONKEY > 5";
        QueryParser parser = QueryParser.parse(sql, NATION_DDL);

        assertNotNull(parser);
        assertEquals("NATION", parser.getTable());
        assertEquals(List.of("N_NAME"), parser.getColumns());
        assertNotNull(parser.getProtoPlan()); // proto plan должен быть непустым
    }

    /**
     * Проверка парсинга SELECT с несколькими условиями через AND.
     */
    @Test
    void testSelectWithAndFilter() {
        String sql = "SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_NATIONKEY > 5 AND N_REGIONKEY < 10";
        QueryParser parser = QueryParser.parse(sql, NATION_DDL);

        assertNotNull(parser);
        assertEquals("NATION", parser.getTable());
        assertEquals(List.of("N_NAME", "N_REGIONKEY"), parser.getColumns());
    }

    /**
     * Проверка парсинга SELECT с JOIN (не поддерживается).
     */
    @Test
    void testSelectWithJoinShouldThrow() {
        String sql = "SELECT N.N_NAME, O.O_ORDERKEY FROM NATION N JOIN ORDERS O ON N.N_NATIONKEY = O.O_CUSTKEY";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL + ";" + ORDERS_DDL);
        });

        assertTrue(exception.getMessage().contains("Join") || exception.getMessage().contains("join"),
                "Сообщение об ошибке должно содержать 'Join'");
    }

    /**
     * Проверка парсинга SELECT с LIMIT (не поддерживается).
     */
    @Test
    void testSelectWithLimitShouldThrow() {
        String sql = "SELECT N_NAME FROM NATION LIMIT 10";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        assertTrue(exception.getMessage().contains("LIMIT"),
                "Сообщение об ошибке должно содержать информацию о LIMIT: " + exception.getMessage());
    }

    /**
     * Проверка парсинга UPDATE (не поддерживается).
     */
    @Test
    void testUpdateShouldThrow() {
        String sql = "UPDATE NATION SET N_NAME = 'TEST' WHERE N_NATIONKEY = 1";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        assertTrue(exception.getMessage().contains("UPDATE") || exception.getMessage().contains("update"),
                "Сообщение об ошибке должно содержать информацию об UPDATE: " + exception.getMessage());
    }

    /**
     * Проверка парсинга INSERT (не поддерживается).
     */
    @Test
    void testInsertShouldThrow() {
        String sql = "INSERT INTO NATION (N_NATIONKEY, N_NAME) VALUES (1, 'TEST')";

        // INSERT выбрасывает CalciteContextException - это валидация на уровне Calcite
        assertThrows(Exception.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });
    }

    /**
     * Проверка парсинга DELETE (не поддерживается).
     */
    @Test
    void testDeleteShouldThrow() {
        String sql = "DELETE FROM NATION WHERE N_NATIONKEY = 1";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        // DELETE вызывает Write запрос, который не поддерживается
        assertTrue(exception.getMessage().contains("WRITE"),
                "Сообщение об ошибке должно содержать информацию о WRITE: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с агрегацией (не поддерживается).
     */
    @Test
    void testSelectWithAggregateShouldThrow() {
        String sql = "SELECT COUNT(*), AVG(O_TOTALPRICE) FROM ORDERS";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, ORDERS_DDL);
        });

        assertTrue(exception.getMessage().contains("Aggregate") || exception.getMessage().contains("aggregate"),
                "Сообщение об ошибке должно содержать информацию об агрегации: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с GROUP BY (не поддерживается).
     */
    @Test
    void testSelectWithGroupByShouldThrow() {
        String sql = "SELECT O_ORDERSTATUS, COUNT(*) FROM ORDERS GROUP BY O_ORDERSTATUS";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, ORDERS_DDL);
        });

        assertTrue(exception.getMessage().contains("Group") || exception.getMessage().contains("group") ||
                exception.getMessage().contains("Aggregate"),
                "Сообщение об ошибке должно содержать информацию о GROUP BY: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с ORDER BY (не поддерживается).
     */
    @Test
    void testSelectWithOrderByShouldThrow() {
        String sql = "SELECT N_NAME FROM NATION ORDER BY N_NAME";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        assertTrue(exception.getMessage().contains("Sort") || exception.getMessage().contains("sort") ||
                exception.getMessage().contains("order"),
                "Сообщение об ошибке должно содержать информацию о сортировке: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с HAVING (не поддерживается).
     */
    @Test
    void testSelectWithHavingShouldThrow() {
        String sql = "SELECT O_ORDERSTATUS, COUNT(*) FROM ORDERS GROUP BY O_ORDERSTATUS HAVING COUNT(*) > 5";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, ORDERS_DDL);
        });

        assertTrue(exception.getMessage().contains("Having") || exception.getMessage().contains("having") ||
                exception.getMessage().contains("Aggregate"),
                "Сообщение об ошибке должно содержать информацию о HAVING: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с UNION (не поддерживается).
     */
    @Test
    void testSelectWithUnionShouldThrow() {
        String sql = "SELECT N_NAME FROM NATION WHERE N_NATIONKEY = 1 " +
                "UNION SELECT N_NAME FROM NATION WHERE N_NATIONKEY = 2";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        assertTrue(exception.getMessage().contains("UNION") || exception.getMessage().contains("union") ||
                exception.getMessage().contains("Set"),
                "Сообщение об ошибке должно содержать информацию о UNION: " + exception.getMessage());
    }

    /**
     * Проверка парсинга с оконной функцией (не поддерживается).
     */
    @Test
    void testSelectWithWindowFunctionShouldThrow() {
        String sql = "SELECT N_NAME, ROW_NUMBER() OVER (PARTITION BY N_REGIONKEY) FROM NATION";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            QueryParser.parse(sql, NATION_DDL);
        });

        assertTrue(exception.getMessage().contains("Window") || exception.getMessage().contains("window") ||
                exception.getMessage().contains("Оконные"),
                "Сообщение об ошибке должно содержать информацию о оконной функции: " + exception.getMessage());
    }

    /**
     * Проверка, что для простых валидных запросов всё работает.
     */
    @Test
    void testSimpleQueryWorks() {
        String sql = "SELECT N_NAME FROM NATION WHERE N_NATIONKEY = 1";
        QueryParser parser = QueryParser.parse(sql, NATION_DDL);

        assertEquals("NATION", parser.getTable());
        assertEquals(List.of("N_NAME"), parser.getColumns());
        assertNotNull(parser.getProtoPlan());
    }
}
