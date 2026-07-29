package net.surpin.data.arrowflight.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.arrow.vector.ipc.message.MessageSerializer.serializeMetadata;

public class MetadataUtils {
    public static final String CATALOG_NAME = "PARQUET_ARROW_FLIGHT_CATALOG";
    public static final String TABLE_TYPE = "TABLE";

    public static VectorSchemaRoot getCatalogsRoot(final BufferAllocator allocator) {
        List<Map<String, Object>> data = List.of(
                Map.of("TABLE_CATALOG", CATALOG_NAME)
        );
        return getRoot(data, allocator, "catalog_name", "TABLE_CATALOG");
    }

    public static VectorSchemaRoot getSchemasRoot(Collection<String> schemas, BufferAllocator allocator) {
        final VarCharVector catalogsVector = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemasVector = new VarCharVector("db_schema_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);

        final List<FieldVector> vectors = ImmutableList.of(catalogsVector, schemasVector);
        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
                catalogsVector, "TABLE_CATALOG",
                schemasVector, "TABLE_SCHEM"
        );

        List<? extends Map<String, ?>> data = schemas.stream().map(schema -> Map.of(
                "TABLE_CATALOG", CATALOG_NAME,
                "TABLE_SCHEM", schema
        )).toList();


        saveToVectors(vectorToColumnName, data);

        final int rows = vectors.stream()
                        .map(FieldVector::getValueCount)
                        .findAny()
                        .orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));

        return new VectorSchemaRoot(vectors);
    }

    public static VectorSchemaRoot getTableTypesRoot(BufferAllocator allocator) {
        List<Map<String, Object>> data = List.of(
                Map.of("TABLE_TYPE", TABLE_TYPE)
        );

        return getRoot(data, allocator, "table_type", "TABLE_TYPE");
    }

    public static VectorSchemaRoot getTablesRoot(
            Map<String, Map<String, Schema>> tables,
            final BufferAllocator allocator,
            boolean includeSchema,
            final String schemaFilterPattern,
            final String tableFilterPattern) {
        Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");

        Predicate<String> schemaNamePredicate = createLikePredicate(schemaFilterPattern);
        Predicate<String> tableNamePredicate = createLikePredicate(tableFilterPattern);

        final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
        final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
        final VarCharVector tableNameVector = new VarCharVector("table_name", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final VarCharVector tableTypeVector = new VarCharVector("table_type", FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        final VarBinaryVector tableSchemaVector = includeSchema ? new VarBinaryVector("table_schema", FieldType.notNullable(Types.MinorType.VARBINARY.getType()), allocator) : null;

        final List<FieldVector> vectors = new ArrayList<>(4);
        vectors.add(catalogNameVector);
        vectors.add(schemaNameVector);
        vectors.add(tableNameVector);
        vectors.add(tableTypeVector);
        if (tableSchemaVector != null) {
            vectors.add(tableSchemaVector);
        }

        vectors.forEach(FieldVector::allocateNew);

        final Map<FieldVector, String> vectorToColumnName =
                ImmutableMap.of(
                        catalogNameVector, "TABLE_CAT",
                        schemaNameVector, "TABLE_SCHEM",
                        tableNameVector, "TABLE_NAME",
                        tableTypeVector, "TABLE_TYPE");

        List<Map<String, ?>> data = new LinkedList<>();
        for (Map.Entry<String, Map<String, Schema>> schemaEntry : tables.entrySet()) {
            String schemaName = schemaEntry.getKey();
            if (!schemaNamePredicate.test(schemaName)) {
                continue;
            }

            for (Map.Entry<String, Schema> tableEntry : schemaEntry.getValue().entrySet()) {
                String tableName = tableEntry.getKey();
                if (!tableNamePredicate.test(tableName)) {
                    continue;
                }

                Schema schema = tableEntry.getValue();
                data.add(Map.of(
                        "TABLE_CAT", CATALOG_NAME,
                        "TABLE_SCHEM", schemaName,
                        "TABLE_NAME", tableName,
                        "TABLE_TYPE", TABLE_TYPE
                ));
                if (tableSchemaVector != null) {
                    saveToVector(ByteString.copyFrom(serializeMetadata(schema, IpcOption.DEFAULT)).toByteArray(), tableSchemaVector, data.size() - 1);
                }
            }
        }

        saveToVectors(vectorToColumnName, data);
        final int rows = vectors.stream()
                .map(FieldVector::getValueCount)
                .findAny()
                .orElseThrow(IllegalStateException::new);
        vectors.forEach(vector -> vector.setValueCount(rows));

        return new VectorSchemaRoot(vectors);
    }

    public static VectorSchemaRoot getTypeInfoRoot(FlightSql.CommandGetXdbcTypeInfo request, final BufferAllocator allocator) {
        Preconditions.checkNotNull(allocator, "BufferAllocator cannot be null.");

        VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, allocator);

        Map<FieldVector, String> mapper = new HashMap<>();
        mapper.put(root.getVector("type_name"), "TYPE_NAME");
        mapper.put(root.getVector("data_type"), "DATA_TYPE");
        mapper.put(root.getVector("column_size"), "PRECISION");
        mapper.put(root.getVector("literal_prefix"), "LITERAL_PREFIX");
        mapper.put(root.getVector("literal_suffix"), "LITERAL_SUFFIX");
        mapper.put(root.getVector("create_params"), "CREATE_PARAMS");
        mapper.put(root.getVector("nullable"), "NULLABLE");
        mapper.put(root.getVector("case_sensitive"), "CASE_SENSITIVE");
        mapper.put(root.getVector("searchable"), "SEARCHABLE");
        mapper.put(root.getVector("unsigned_attribute"), "UNSIGNED_ATTRIBUTE");
        mapper.put(root.getVector("fixed_prec_scale"), "FIXED_PREC_SCALE");
        mapper.put(root.getVector("auto_increment"), "AUTO_INCREMENT");
        mapper.put(root.getVector("local_type_name"), "LOCAL_TYPE_NAME");
        mapper.put(root.getVector("minimum_scale"), "MINIMUM_SCALE");
        mapper.put(root.getVector("maximum_scale"), "MAXIMUM_SCALE");
        mapper.put(root.getVector("sql_data_type"), "SQL_DATA_TYPE");
        mapper.put(root.getVector("datetime_subcode"), "SQL_DATETIME_SUB");
        mapper.put(root.getVector("num_prec_radix"), "NUM_PREC_RADIX");

        List<? extends Map<String, ?>> types = ParquetSchemaConverter.buildTypeInfo().stream()
                .filter(type -> !request.hasDataType() || type.dataType() == request.getDataType())
                .map(ParquetSchemaConverter.TypeInfo::toMap).toList();

        saveToVectors(mapper, types);

        return root;
    }

    private static VectorSchemaRoot getRoot(
            List<? extends Map<String, ?>> data,
            BufferAllocator allocator,
            String fieldVectorName,
            String columnName) {
        final VarCharVector dataVector = new VarCharVector(fieldVectorName, FieldType.notNullable(Types.MinorType.VARCHAR.getType()), allocator);
        saveToVectors(ImmutableMap.of(dataVector, columnName), data);
        dataVector.setValueCount(dataVector.getValueCount());
        return new VectorSchemaRoot(singletonList(dataVector));
    }

    private static <T extends FieldVector> void saveToVectors(final Map<T, String> vectorToColumnName, List<? extends Map<String, ?>> data) {
        saveToVectors(vectorToColumnName, data, none -> true);
    }

    @SuppressWarnings("StringSplitter")
    private static <T extends FieldVector> int saveToVectors(
            Map<T, String> vectorToColumnName,
            List<? extends Map<String, ?>> data,
            Predicate<Map<String, ?>> rowPredicate) {
        Objects.requireNonNull(vectorToColumnName, "vectorToColumnName cannot be null.");
        Objects.requireNonNull(data, "data cannot be null.");
        final Set<Map.Entry<T, String>> entrySet = vectorToColumnName.entrySet();
        int rows = 0;

        for (Map<String, ?> row : data) {
            if (!rowPredicate.test(row)) {
                continue;
            }
            for (final Map.Entry<T, String> vectorToColumn : entrySet) {
                final T vector = vectorToColumn.getKey();
                final String columnName = vectorToColumn.getValue();
                if (vector instanceof VarCharVector) {
                    saveToVector((String) row.get(columnName), (VarCharVector) vector, rows);
                } else if (vector instanceof IntVector) {
                    saveToVector((Integer) row.get(columnName), (IntVector) vector, rows);
                } else if (vector instanceof UInt1Vector) {
                    saveToVector((Byte) row.get(columnName), (UInt1Vector) vector, rows);
                } else if (vector instanceof BitVector) {
                    saveToVector((Byte) row.get(columnName), (BitVector) vector, rows);
                } else if (vector instanceof ListVector) {
                    String createParamsValues = (String) row.get(columnName);

                    UnionListWriter writer = ((ListVector) vector).getWriter();

                    BufferAllocator allocator = vector.getAllocator();
                    final ArrowBuf buf = allocator.buffer(1024);

                    writer.setPosition(rows);
                    writer.startList();

                    if (createParamsValues != null) {
                        String[] split = createParamsValues.split(",");

                        IntStream.range(0, split.length)
                                .forEach(
                                        i -> {
                                            byte[] bytes = split[i].getBytes(StandardCharsets.UTF_8);
                                            Preconditions.checkState(
                                                    bytes.length < 1024,
                                                    "The amount of bytes is greater than what the ArrowBuf supports");
                                            buf.setBytes(0, bytes);
                                            writer.varChar().writeVarChar(0, bytes.length, buf);
                                        });
                    }
                    buf.close();
                    writer.endList();
                } else {
                    throw CallStatus.INVALID_ARGUMENT
                            .withDescription("Provided vector not supported")
                            .toRuntimeException();
                }
            }
            rows++;
        }
        for (final Map.Entry<T, String> vectorToColumn : entrySet) {
            vectorToColumn.getKey().setValueCount(rows);
        }

        return rows;
    }

    private static void saveToVector(final Byte data, final UInt1Vector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final Byte data, final BitVector vector, final int index) {
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(final String data, final VarCharVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
    }

    private static void saveToVector(final Integer data, final IntVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void saveToVector(
            final byte[] data, final VarBinaryVector vector, final int index) {
        preconditionCheckSaveToVector(vector, index);
        vectorConsumer(
                data,
                vector,
                fieldVector -> fieldVector.setNull(index),
                (theData, fieldVector) -> fieldVector.setSafe(index, theData));
    }

    private static void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
        Objects.requireNonNull(vector, "vector cannot be null.");
        Preconditions.checkState(index >= 0, "Index must be a positive number!");
    }

    private static <T, V extends FieldVector> void vectorConsumer(
            final T data,
            final V vector,
            final Consumer<V> consumerIfNullable,
            final BiConsumer<T, V> defaultConsumer) {
        if (Objects.isNull(data)) {
            consumerIfNullable.accept(vector);
            return;
        }
        defaultConsumer.accept(data, vector);
    }

    public static Predicate<String> createLikePredicate(String sqlPattern) {
        if (sqlPattern == null) {
            return s -> true;
        }

        String regex = "^" + Pattern.quote(sqlPattern)
                .replace("%", "\\E.*\\Q")
                .replace("_", "\\E.\\Q") + "$";

        return Pattern.compile(regex).asPredicate();
    }
}
