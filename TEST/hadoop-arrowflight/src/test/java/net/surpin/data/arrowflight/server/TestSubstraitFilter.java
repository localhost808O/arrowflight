package net.surpin.data.arrowflight.server;

import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;

public class TestSubstraitFilter {

    // SqlExpressionToSubstrait (isthmus 0.91.0) bug: RexExpressionConverter is built with only
    // ScalarFunctionConverter, omitting CAST from getCallConverters(). We fix it by overriding
    // getScalarFunctionConverter() to return a wrapper that intercepts CAST calls first.
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
                    return castConv.convert(call, visitor).or(() -> base.convert(call, visitor));
                }
            };
        }
    };

    private static final String CREATE_STATEMENT =
            "CREATE TABLE test_1tb_table(\n" +
            "\t\"id\" INTEGER,\n" +
            "\t\"bool_col\" BOOLEAN,\n" +
            "\t\"tinyint_col\" TINYINT,\n" +
            "\t\"smallint_col\" SMALLINT,\n" +
            "\t\"int_col\" INTEGER,\n" +
            "\t\"bigint_col\" BIGINT,\n" +
            "\t\"float_col\" REAL,\n" +
            "\t\"double_col\" DOUBLE,\n" +
            "\t\"date_string_col\" VARCHAR,\n" +
            "\t\"string_col\" VARCHAR,\n" +
            "\t\"timestamp_col\" BIGINT,\n" +
            "\t\"year\" INTEGER,\n" +
            "\t\"month\" INTEGER);";

    ByteBuffer getFilterExpression(String sqlExpression) throws SqlParseException {
        SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(CAST_AWARE_PROVIDER);
        byte[] bytes = converter.convert(sqlExpression, List.of(CREATE_STATEMENT)).toByteArray();

        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.rewind();
        return buffer;
    }

    void filterDataset(String sqlExpression) throws Exception {
        URL resource = TestSubstraitFilter.class.getClassLoader().getResource(
                "part-00000-7b025801-1c73-42d1-804c-0a29f02e9942-c000.snappy.parquet");
        if (resource == null) {
            throw new IllegalStateException("Parquet test resource not found on classpath");
        }
        String uri = resource.toURI().toString();

        ScanOptions options =
                new ScanOptions.Builder(/*batchSize*/ 32768)
                        .columns(Optional.empty())
                        .substraitFilter(getFilterExpression(sqlExpression))
                        .build();
        try (BufferAllocator allocator = new RootAllocator();
             DatasetFactory datasetFactory =
                     new FileSystemDatasetFactory(
                             allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
             Dataset dataset = datasetFactory.finish();
             Scanner scanner = dataset.newScan(options);
             ArrowReader reader = scanner.scanBatches()) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            System.out.println("Schema: " + schema);
            for (Field field : schema.getFields()) {
                System.out.println("Field: " + field.getName() + " -> " + field.getFieldType().getType());
            }
            while (reader.loadNextBatch()) {
                if (reader.getVectorSchemaRoot().getRowCount() == 0) {
                    continue;
                }
                System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        TestSubstraitFilter test = new TestSubstraitFilter();

        System.out.println("\n=== Filter 1: id IS NOT NULL AND smallint_col=0 ===");
        test.filterDataset("(\"id\" is not null and \"smallint_col\"=0)");

        System.out.println("\n=== Filter 2: id>0 AND id<1000 ===");
        test.filterDataset("\"id\">0 and \"id\"<1000");

        System.out.println("\n=== Filter 3: id IS NOT NULL AND id>0 AND id<1000 ===");
        test.filterDataset("\"id\" is not null and \"id\">0 and \"id\"<1000");
    }
}
