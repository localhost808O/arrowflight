package net.surpin.data.arrowflight.server;

import io.substrait.proto.Expression;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.FunctionArgument;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SubstraitFilterConverterTest {

    private static final String CREATE_TABLE =
            "CREATE TABLE test_table(\n" +
            "\t\"id\" INTEGER,\n" +
            "\t\"bool_col\" BOOLEAN,\n" +
            "\t\"tinyint_col\" TINYINT,\n" +
            "\t\"smallint_col\" SMALLINT,\n" +
            "\t\"int_col\" INTEGER,\n" +
            "\t\"bigint_col\" BIGINT,\n" +
            "\t\"float_col\" REAL,\n" +
            "\t\"double_col\" DOUBLE,\n" +
            "\t\"string_col\" VARCHAR)";

    @Test
    void simpleEqualityProducesNonEmptyBuffer() throws SqlParseException {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" = 42", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0, "Serialized filter should be non-empty");
        assertEquals(0, buf.position(), "Buffer should be rewound to position 0");
    }

    @Test
    void isNotNullProducesNonEmptyBuffer() throws SqlParseException {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" is not null", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void compoundAndFilterHandledCorrectly() throws SqlParseException {
        // Exercises the CAST-aware fix and binary AND folding
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" is not null and \"smallint_col\" = 0", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void tinyintComparisonWithCastWorkaround() throws SqlParseException {
        // tinyint_col = 0 triggers Calcite CAST coercion — exercises the CAST-aware converter
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"tinyint_col\" = 0", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void nAryAndIsFoldedToBinaryPairs() throws Exception {
        // 3-arg AND must be folded to nested binary and_kleene calls
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" > 0 and \"id\" < 1000 and \"bool_col\" = true",
                List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);

        // Decode the Substrait proto and assert every scalar function has <= 2 arguments.
        // Acero rejects and_kleene / or_kleene with more than 2 arguments.
        byte[] bytes = new byte[buf.capacity()];
        buf.rewind();
        buf.get(bytes);
        ExtendedExpression proto = ExtendedExpression.parseFrom(bytes);
        for (var ref : proto.getReferredExprList()) {
            if (ref.hasExpression()) {
                assertNoBinaryViolations(ref.getExpression());
            }
        }
    }

    @Test
    void fourArgAndIsFoldedToBinaryPairs() throws Exception {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" > 0 and \"id\" < 1000 and \"bool_col\" = true and \"string_col\" = 'x'",
                List.of(CREATE_TABLE));
        assertNotNull(buf);
        byte[] bytes = new byte[buf.capacity()];
        buf.rewind();
        buf.get(bytes);
        ExtendedExpression proto = ExtendedExpression.parseFrom(bytes);
        for (var ref : proto.getReferredExprList()) {
            if (ref.hasExpression()) {
                assertNoBinaryViolations(ref.getExpression());
            }
        }
    }

    private static void assertNoBinaryViolations(Expression expr) {
        if (expr.hasScalarFunction()) {
            List<FunctionArgument> args = expr.getScalarFunction().getArgumentsList();
            assertTrue(args.size() <= 2,
                    "Scalar function has " + args.size() + " arguments; Acero requires <= 2 for and_kleene/or_kleene");
            for (FunctionArgument arg : args) {
                if (arg.hasValue()) {
                    assertNoBinaryViolations(arg.getValue());
                }
            }
        } else if (expr.hasIfThen()) {
            for (var clause : expr.getIfThen().getIfsList()) {
                assertNoBinaryViolations(clause.getIf());
                assertNoBinaryViolations(clause.getThen());
            }
            if (expr.getIfThen().hasElse()) {
                assertNoBinaryViolations(expr.getIfThen().getElse());
            }
        } else if (expr.hasCast()) {
            assertNoBinaryViolations(expr.getCast().getInput());
        }
    }

    @Test
    void tinyintInequalityDoesNotThrowCastError() throws SqlParseException {
        // Calcite may insert CAST(col:i8?) when coercing tinyint for inequality;
        // the strip-cast fallback must handle it without IllegalArgumentException.
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"tinyint_col\" > 5", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void tinyintIsNullDoesNotThrowCastError() throws SqlParseException {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"tinyint_col\" is null", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void stringEqualityFilter() throws SqlParseException {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"string_col\" = 'hello'", List.of(CREATE_TABLE));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void multipleCreateStatements() throws SqlParseException {
        String other = "CREATE TABLE other(\"x\" INTEGER)";
        // Should resolve against test_table (the first CREATE matching the filter column)
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" = 1", List.of(CREATE_TABLE, other));
        assertNotNull(buf);
        assertTrue(buf.capacity() > 0);
    }

    @Test
    void bufferIsDirectAllocated() throws SqlParseException {
        ByteBuffer buf = SubstraitFilterConverter.toByteBuffer(
                "\"id\" = 0", List.of(CREATE_TABLE));
        assertTrue(buf.isDirect(), "ByteBuffer must be direct for Arrow native use");
    }
}
