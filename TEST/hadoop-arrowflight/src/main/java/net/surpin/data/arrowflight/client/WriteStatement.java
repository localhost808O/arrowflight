package net.surpin.data.arrowflight.client;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.*;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.time.format.DateTimeFormatter;

/**
 * The write statement for writing data to remote flight service
 */
public class WriteStatement implements Serializable {
    //the execute interface
    @FunctionalInterface
    private interface Execute<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the convert interface
    @FunctionalInterface
    private interface Convert<A, B, C, D, R> {
        R apply(A a, B b, C c, D d);
    }
    //the values variable
    private static final String varValues = "${param_Values}";
    //the to-object-converter container (transient — re-initialized lazily after deserialization)
    private transient Map<String, Execute<Object, Field, DataType, String>> converters;

    //data schema
    private final StructType dataSchema;
    //arrow schema
    private final String arrowSchema;
    //the parameter name in the WriteStatement
    private final String[] params;

    //the statement in the format of either merge into or insert into sql statement
    private String stmt;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.converters = new java.util.HashMap<>();
    }

    //the following is the mapping between arrow-types to data-types that the target flight end-point supports
    private String mapTinyInt = "int";
    private String mapSmallInt = "int";
    private String mapInt = "int";
    private String mapBigInt = "bigint";
    private String mapDate = "date";
    private String mapTime = "time";
    private String mapTimestamp = "timestamp";
    private String mapFloat4 = "float";
    private String mapFloat8 = "double";
    private String mapVarchar = "varchar";
    private String mapLargeVarchar = "varchar";
    private String mapDecimal = "decimal";
    private String mapDecimal256 = "decimal";
    private String mapUInt1 = "int";
    private String mapUInt2 = "int";
    private String mapUInt4 = "int";
    private String mapUInt8 = "int";
    private String mapBit = "boolean";

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);
        this.stmt = String.format("insert into %s(%s) %s", tableName, String.join(",", WriteStatement.quote.apply(this.params, columnQuote)), WriteStatement.varValues);
    }

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param mergeByColumns - the name of columns for merging by
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, String[] mergeByColumns, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);

        Map<String, Integer> entries = new java.util.LinkedHashMap<>();
        for (String field: WriteStatement.quote.apply(this.params, columnQuote)) {
            entries.put(field, 0);
        }
        for (String field: WriteStatement.quote.apply(mergeByColumns, columnQuote)) {
            entries.put(field, 1);
        }
        String matchOn = String.join(" and ", entries.entrySet().stream().filter(e -> e.getValue() == 1).map(e -> String.format("t.%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String setUpdate = String.join(",", entries.entrySet().stream().filter(e -> e.getValue() == 0).map(e -> String.format("%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String varInsert = String.join(",", entries.keySet().toArray(new String[0]));
        String valInsert = String.join(",", entries.keySet().stream().map(integer -> String.format("s.%s", integer)).toArray(String[]::new));
        this.stmt = String.format("merge into %s t using (%s) s(%s) on %s when matched then update set %s when not matched then insert (%s) values(%s)", tableName, WriteStatement.varValues, varInsert, matchOn, setUpdate, varInsert, valInsert);
    }

    //initialize properties
    private WriteStatement(StructType dataSchema, Schema arrowSchema, Map<String, String> typeMapping) {
        this.dataSchema = dataSchema;
        this.arrowSchema = arrowSchema.toJson();
        this.params = dataSchema.fieldNames();
        this.converters = new java.util.HashMap<>();

        this.mapTinyInt = typeMapping.getOrDefault("tinyint", "int");
        this.mapSmallInt = typeMapping.getOrDefault("smallint", "int");
        this.mapInt = typeMapping.getOrDefault("int", "int");
        this.mapBigInt = typeMapping.getOrDefault("bigint", "bigint");
        this.mapDate = typeMapping.getOrDefault("date", "date");
        this.mapTime = typeMapping.getOrDefault("time", "time");
        this.mapTimestamp = typeMapping.getOrDefault("timestamp", "timestamp");
        this.mapFloat4 = typeMapping.getOrDefault("float4", "float");
        this.mapFloat8 = typeMapping.getOrDefault("float8", "double");
        this.mapVarchar = typeMapping.getOrDefault("varchar", "varchar");
        this.mapLargeVarchar = typeMapping.getOrDefault("largevarchar", "varchar");
        this.mapDecimal = typeMapping.getOrDefault("decimal", "decimal");
        this.mapDecimal256 = typeMapping.getOrDefault("decimal256", "decimal");
        this.mapUInt1 = typeMapping.getOrDefault("uint1", "int");
        this.mapUInt2 = typeMapping.getOrDefault("uint2", "int");
        this.mapUInt4 = typeMapping.getOrDefault("uint4", "int");
        this.mapUInt8 = typeMapping.getOrDefault("uint8", "int");
        this.mapBit = typeMapping.getOrDefault("bit", "boolean");
    }

    /**
     * Get the data schema
     * @return - the data schema
     */
    public StructType getDataSchema() {
        return this.dataSchema;
    }

    /**
     * Get the arrow-schema
     * @return - arrow schema
     * @throws IOException - thrown when the arrow-schema is invalid
     */
    public Schema getArrowSchema() throws IOException {
        return Schema.fromJSON(this.arrowSchema);
    }

    /**
     * Get the statement
     * @return - the merge into or insert into statement
     */
    public String getStatement() {
        return this.stmt.replace(WriteStatement.varValues, String.format("values(%s)", String.join(",", Arrays.stream(this.params).map(param -> "?").toArray(String[]::new))));
    }

    /**
     * Fill the statment with data
     * @param rows - the rows of data
     * @param arrowFields - the fields of output
     * @return - a statement with data
     */
    public String fillStatement(InternalRow[] rows, Field[] arrowFields) {
        Function<String, Optional<Field>> find = (name) -> Arrays.stream(arrowFields).filter(x -> x.getName().equalsIgnoreCase(name)).findFirst();
        StructField[] dataFields = this.dataSchema.fields();
        Object[] columns = IntStream.range(0, dataFields.length).mapToObj(idx -> {
            Optional<Field> arrowField = find.apply(dataFields[idx].name());
            if (!arrowField.isPresent()) {
                throw new RuntimeException("The arrow field is not available.");
            }
            return this.fillColumns(rows, idx, dataFields[idx].dataType(), arrowField.get());
        }).toArray(Object[]::new);

        String[] values = IntStream.range(0, rows.length).mapToObj(i -> String.format("(%s)", String.join(",", Arrays.stream(columns).map(column -> ((String[]) column)[i]).toArray(String[]::new)))).toArray(String[]::new);
        return this.stmt.replace(WriteStatement.varValues, String.format("values%s", String.join(",", values)));
    }

    //convert the values of a specific column
    private String[] fillColumns(InternalRow[] rows, int idxColumn, DataType dataType, Field arrowField) {
        String key = String.format("%s-%s", dataType.getClass().getTypeName().replaceAll("\\$$", ""), Types.getMinorTypeForArrowType(arrowField.getType()).getClass().getTypeName());
        if (!this.converters.containsKey(key)) {
            this.initialize();
        }
        return Arrays.stream(rows).map(row -> this.converters.get(key).apply(row.get(idxColumn, dataType), arrowField, dataType)).toArray(String[]::new);
    }

    //initialize all converters
    private void initialize() {
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToDateDay.apply(o, f, t, this.mapDate));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToDateDay.apply(o, f, t, this.mapDate));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampNanoTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampNanoTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampMicroTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampMicroTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampSecTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampSecTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampMilliTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampMilliTZ.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampNano.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampNano.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampMicro.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampMicro.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampSec.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampSec.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimestampMilli.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToTimestampMilli.apply(o, f, t, this.mapTimestamp));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement.stringToTimeMicro.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimeMicro.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), (o, f, t) -> WriteStatement.stringToTimeSec.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimeSec.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), (o, f, t) -> WriteStatement.stringToTimeNano.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimeNano.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.stringToTimeMilli.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToTimeMilli.apply(o, f, t, this.mapTime));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.dateToDateMilli.apply(o, f, t, this.mapDate));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement.timestampToDateMilli.apply(o, f, t, this.mapDate));
        this.converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal256.apply(o, f, t, this.mapDecimal256));
        this.converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToDecimal.apply(o, f, t, this.mapDecimal));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat8.apply(o, f, t, this.mapFloat8));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToFloat4.apply(o, f, t, this.mapFloat4));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBigint.apply(o, f, t, this.mapBigInt));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBigint.apply(o, f, t, this.mapBigInt));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBigint.apply(o, f, t, this.mapBigInt));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBigint.apply(o, f, t, this.mapBigInt));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint8.apply(o, f, t, this.mapUInt8));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint8.apply(o, f, t, this.mapUInt8));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint8.apply(o, f, t, this.mapUInt8));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint8.apply(o, f, t, this.mapUInt8));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint4.apply(o, f, t, this.mapUInt4));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint4.apply(o, f, t, this.mapUInt4));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint4.apply(o, f, t, this.mapUInt4));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToInt.apply(o, f, t, this.mapInt));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToInt.apply(o, f, t, this.mapInt));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToInt.apply(o, f, t, this.mapInt));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint2.apply(o, f, t, this.mapUInt2));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint2.apply(o, f, t, this.mapUInt2));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT1.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToUint1.apply(o, f, t, this.mapUInt1));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToSmallint.apply(o, f, t, this.mapSmallInt));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToSmallint.apply(o, f, t, this.mapSmallInt));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.TINYINT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToTinyint.apply(o, f, t, this.mapTinyInt));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.primitiveToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", MapType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", ArrayType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", StructType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToVarchar.apply(o, f, t, this.mapVarchar));
        this.converters.put(String.format("%s-%s", MapType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", ArrayType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", StructType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement.complexToLargevarchar.apply(o, f, t, this.mapLargeVarchar));
        this.converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.booleanToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.stringToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
        this.converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement.numberToBit.apply(o, f, t, this.mapBit));
    }

    //conversion - Number to DECIMAL256
    private static final Convert<Object, Field, DataType, String, String> numberToDecimal256 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to DECIMAL
    private static final Convert<Object, Field, DataType, String, String> numberToDecimal = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to FLOAT8
    private static final Convert<Object, Field, DataType, String, String> numberToFloat8 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to FLOAT4
    private static final Convert<Object, Field, DataType, String, String> numberToFloat4 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to BIGINT
    private static final Convert<Object, Field, DataType, String, String> numberToBigint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT8
    private static final Convert<Object, Field, DataType, String, String> numberToUint8 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT4
    private static final Convert<Object, Field, DataType, String, String> numberToUint4 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to INT
    private static final Convert<Object, Field, DataType, String, String> numberToInt = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT2
    private static final Convert<Object, Field, DataType, String, String> numberToUint2 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT1
    private static final Convert<Object, Field, DataType, String, String> numberToUint1 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to SMALLINT
    private static final Convert<Object, Field, DataType, String, String> numberToSmallint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to TINYINT
    private static final Convert<Object, Field, DataType, String, String> numberToTinyint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Primitive to VARCHAR
    private static final Convert<Object, Field, DataType, String, String> primitiveToVarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", o.toString().replace("'", "''"));
    //conversion - Primitive to LARGEVARCHAR
    private static final Convert<Object, Field, DataType, String, String> primitiveToLargevarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", o.toString().replace("'", "''"));
    //conversion - Complex to VARCHAR
    private static final Convert<Object, Field, DataType, String, String> complexToVarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", WriteStatement.toJson.apply(o, t).replace("'", "''"));
    //conversion - Complex to LARGEVARCHAR
    private static final Convert<Object, Field, DataType, String, String> complexToLargevarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", WriteStatement.toJson.apply(o, t).replace("'", "''"));
    //conversion - String to TIMESEC
    private static final Convert<Object, Field, DataType, String, String> stringToTimeSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String) o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss")), n);
    //conversion - Timestamp to TIMESEC
    private static final Convert<Object, Field, DataType, String, String> timestampToTimeSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss")), n);
    //conversion - String to TIMENANO
    private static final Convert<Object, Field, DataType, String, String> stringToTimeNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String) o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMENANO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimeNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")), n);
    //conversion - String to TIMEMICRO
    private static final Convert<Object, Field, DataType, String, String> stringToTimeMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String) o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")), n);
    //conversion - Timestamp to TIMEMICRO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimeMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")), n);
    //conversion - String to TIMEMILLI
    private static final Convert<Object, Field, DataType, String, String> stringToTimeMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String) o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMEMILLI
    private static final Convert<Object, Field, DataType, String, String> timestampToTimeMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")), n);
    //conversion - Date to DATEMILLI
    private static final Convert<Object, Field, DataType, String, String> dateToDateMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Timestamp to DATEMILLI
    private static final Convert<Object, Field, DataType, String, String> timestampToDateMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(DateTimeUtils.daysToMicros((o instanceof Number) ? (int) o : Integer.parseInt(o.toString()), ZoneId.systemDefault())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Timestamp to DateDay
    private static final Convert<Object, Field, DataType, String, String> timestampToDateDay = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Date to DateDay
    private static final Convert<Object, Field, DataType, String, String> dateToDateDay = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Boolean to BIT
    private static final Convert<Object, Field, DataType, String, String> booleanToBit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to BIT
    private static final Convert<Object, Field, DataType, String, String> numberToBit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : (((int) o) != 0) ? "true" : "false", n);
    //conversion - String to BIT
    private static final Convert<Object, Field, DataType, String, String> stringToBit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString().equalsIgnoreCase("true") ? "true" : "false", n);
    //conversion - Timestamp to TIMESTAMPMILLI
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Date to TIMESTAMPMILLI
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMESTAMPSEC
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Date to TIMESTAMPSEC
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Timestamp to TIMESTAMPNANO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Date to TIMESTAMPNANO
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long) o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Date to TIMESTAMPMICRO
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int) o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMILLITZ
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampMilliTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.microsToMicrosTZ.apply((o instanceof Number) ? (long) o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Date to TIMESTAMPMILLITZ
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampMilliTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.daysToMicrosTZ.apply((o instanceof Number) ? (int) o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMESTAMPSEC
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampSecTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.microsToMicrosTZ.apply((o instanceof Number) ? (long) o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Date to TIMESTAMPSEC
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampSecTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.daysToMicrosTZ.apply((o instanceof Number) ? (int) o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Timestamp to TIMESTAMPNANO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampNanoTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.microsToMicrosTZ.apply((o instanceof Number) ? (long) o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Date to TIMESTAMPNANO
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampNanoTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.daysToMicrosTZ.apply((o instanceof Number) ? (int) o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final Convert<Object, Field, DataType, String, String> timestampToTimestampMicroTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.microsToMicrosTZ.apply((o instanceof Number) ? (long) o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Date to TIMESTAMPMICRO
    private static final Convert<Object, Field, DataType, String, String> dateToTimestampMicroTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement.daysToMicrosTZ.apply((o instanceof Number) ? (int) o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //micros to micros-TZ
    private static final Execute<Long, Field, DataType, Long> microsToMicrosTZ = (micros, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp) field.getFieldType().getType();
        return DateTimeUtils.convertTz(micros, ZoneId.systemDefault(), ZoneId.of(arrowType.getTimezone()));
    };
    //days to micros-TZ
    private static final Execute<Integer, Field, DataType, Long> daysToMicrosTZ = (days, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp) field.getFieldType().getType();
        return DateTimeUtils.daysToMicros(days, ZoneId.of(arrowType.getTimezone()));
    };
    //to-json
    private static final BiFunction<Object, DataType, String> toJson = (o, dt) -> {
        StringBuilder sb = new StringBuilder();
        try {
            if (dt instanceof StructType && o instanceof UnsafeRow) {
                StructField[] fields = ((StructType) dt).fields();
                UnsafeRow data = (UnsafeRow) o;
                sb.append(String.format("{ %s }", String.join(", ", IntStream.range(0, fields.length).mapToObj(idx -> String.format("\"%s\": %s", fields[idx].name(), WriteStatement.toJson.apply(data.get(idx, fields[idx].dataType()), fields[idx].dataType()))).toArray(String[]::new))));
            } else if (dt instanceof MapType && o instanceof UnsafeMapData) {
                MapType mt = (MapType) dt;
                UnsafeMapData data = (UnsafeMapData) o;
                UnsafeArrayData keys = data.keyArray();
                UnsafeArrayData values = data.valueArray();
                sb.append(String.format("{ \"map\": [%s] }", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> String.format("{ \"key\": %s, \"value\": %s }", WriteStatement.toJson.apply(keys.get(idx, mt.keyType()), mt.keyType()), WriteStatement.toJson.apply(values.get(idx, mt.valueType()), mt.valueType()))).toArray(String[]::new))));
            } else if (dt instanceof ArrayType && o instanceof UnsafeArrayData) {
                ArrayType at = (ArrayType) dt;
                UnsafeArrayData data = (UnsafeArrayData) o;
                sb.append(String.format("[%s]", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> WriteStatement.toJson.apply(data.get(idx, at.elementType()), at.elementType())).toArray(String[]::new))));
            } else {
                sb.append(String.format("\"%s\"", o));
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(WriteStatement.class).warn(e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
        return sb.toString().replace("'", "''");
    };
    //quote all fields in the collection
    private static final BiFunction<String[], String, String[]> quote = (fields, quote) -> Arrays.stream(fields).map(field -> String.format("%s%s%s", quote, field, quote)).toArray(String[]::new);
}
