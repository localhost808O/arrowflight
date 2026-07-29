package net.surpin.data.arrowflight.server;

import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * Converts a SQL filter expression to a Substrait {@link ByteBuffer} suitable for
 * {@link org.apache.arrow.dataset.scanner.ScanOptions.Builder#substraitFilter}.
 *
 * <p>Works around three isthmus 0.91.0 limitations:
 * <ol>
 *   <li>{@code SqlExpressionToSubstrait} builds its {@code RexExpressionConverter} with only
 *       {@code ScalarFunctionConverter}, omitting {@code CallConverters.CAST}. This causes
 *       {@code CAST} nodes inserted by Calcite's type coercion (e.g. {@code "smallint_col"=0})
 *       to throw {@code IllegalArgumentException: Unable to convert call CAST(…)}.
 *       Fix: inject a CAST-aware wrapper via {@code ConverterProvider}.
 *   <li>Arrow Acero only implements binary {@code and_kleene} / {@code or_kleene}. Calcite
 *       flattens {@code A AND B AND C} into a flat 3-arg {@code AND} node; isthmus serialises
 *       it as 3-arg {@code and_kleene}, which Acero rejects. Fix: left-fold n-ary AND/OR into
 *       nested binary calls before conversion.
 *   <li>For some nullable types (e.g. {@code CAST(col:i8?)} where isthmus has no type mapping),
 *       {@code CallConverters.CAST} returns empty, causing the same
 *       {@code IllegalArgumentException}. Fix: when the official cast converter cannot handle a
 *       {@code CAST} node, strip the cast and pass the inner expression directly — Acero handles
 *       numeric coercions natively inside its filter kernels.
 * </ol>
 */
public final class SubstraitFilterConverter {

    private static final ConverterProvider CAST_AWARE_PROVIDER = new ConverterProvider() {
        @Override
        public ScalarFunctionConverter getScalarFunctionConverter() {
            ScalarFunctionConverter base = super.getScalarFunctionConverter();
            CallConverter castConv = CallConverters.CAST.apply(getTypeConverter());
            RexBuilder rexBuilder = new RexBuilder(getTypeFactory());
            return new ScalarFunctionConverter(Collections.emptyList(), getTypeFactory()) {
                @Override
                public Optional<io.substrait.expression.Expression> convert(
                        RexCall call, Function<RexNode, io.substrait.expression.Expression> visitor) {
                    // Acero only supports binary and_kleene/or_kleene; fold n-ary AND/OR left
                    if ((call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR)
                            && call.getOperands().size() > 2) {
                        RexNode folded = call.getOperands().get(0);
                        for (int i = 1; i < call.getOperands().size(); i++) {
                            folded = rexBuilder.makeCall(call.getOperator(), folded, call.getOperands().get(i));
                        }
                        return Optional.of(visitor.apply(folded));
                    }
                    // For CAST: try official converter; if it has no mapping for the type
                    // (e.g. Calcite inserts CAST(col:i8?) for nullable-tinyint coercion),
                    // strip the cast — Acero handles numeric coercions inside filter kernels.
                    if (call.getKind() == SqlKind.CAST) {
                        return castConv.convert(call, visitor)
                                .or(() -> Optional.of(visitor.apply(call.getOperands().get(0))));
                    }
                    return castConv.convert(call, visitor).or(() -> base.convert(call, visitor));
                }
            };
        }
    };

    private SubstraitFilterConverter() {
    }

    /**
     * Pre-loads the Calcite and Substrait-Java class graph so that the first real
     * {@link #toByteBuffer} call does not pay the ~1.5 s JVM class-loading cost.
     *
     * <p>Must be called once during server startup before any query arrives.
     * Uses schema-stripped DDL statements (same form as {@link
     * ParquetManager#buildFilterBytes} passes).
     *
     * @param strippedCreateStatements DDL without schema prefix, e.g. {@code CREATE TABLE t(col1 INTEGER)}
     */
    public static void warmUp(List<String> strippedCreateStatements) {
        if (strippedCreateStatements.isEmpty()) return;
        // Build a filter that references a real column so the type-coercion code path is
        // exercised (a pure literal comparison like "1 = 1" won't fully exercise it).
        String firstDdl = strippedCreateStatements.get(0);
        String dummyFilter = "1 = 1"; // safe fallback
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile("\"([^\"]+)\"").matcher(firstDdl);
        if (m.find()) {
            dummyFilter = "\"" + m.group(1) + "\" IS NOT NULL";
        }
        try {
            toByteBuffer(dummyFilter, strippedCreateStatements);
        } catch (Exception ignored) {
            // Failure is fine — we only need to trigger class loading, not a valid result.
        }
    }

    /**
     * Converts {@code sqlExpression} to a Substrait {@link ByteBuffer} ready for Arrow scanning.
     *
     * @param sqlExpression    SQL WHERE-clause expression (no SELECT/WHERE keywords)
     * @param createStatements DDL CREATE TABLE statements that define the schema
     * @return direct {@link ByteBuffer} positioned at 0, containing the serialised
     *         {@code ExtendedExpression} protobuf
     */
    public static ByteBuffer toByteBuffer(String sqlExpression, List<String> createStatements)
            throws SqlParseException {
        SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(CAST_AWARE_PROVIDER);
        byte[] bytes = converter.convert(sqlExpression, createStatements).toByteArray();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.rewind();
        return buffer;
    }
}
