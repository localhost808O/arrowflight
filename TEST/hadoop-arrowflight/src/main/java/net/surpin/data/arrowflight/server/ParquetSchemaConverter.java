package net.surpin.data.arrowflight.server;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Конвертер схемы Parquet в Arrow Schema.
 */
public class ParquetSchemaConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetSchemaConverter.class);

    /**
     * Конвертация типа Parquet в поле Arrow.
     *
     */
    public static Field convert(ColumnDescriptor columnDesc) {
        String name = String.join(".", columnDesc.getPath());
        Type parquetType = columnDesc.getPrimitiveType();

        return convert(name, parquetType);
    }

    public static Field convert(String path, Type type) {
        boolean nullable = type.getRepetition() != Type.Repetition.REQUIRED;

        if (type instanceof GroupType groupType) {
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                fields.add(convert(groupType.getFieldName(i), groupType.getType(i)));
            }
            return new Field(path, fieldType(nullable, new ArrowType.Struct()), fields);
        }

        LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();

        if (logicalType == null) {
            return convertPrimitive(path, type.asPrimitiveType(), nullable);
        } else if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Map(false)), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.List()), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Utf8()), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
            throw new IllegalArgumentException("Unsupported parquet LogicalTypeAnnotation: " + logicalType);
        } else if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            throw new IllegalArgumentException("Unsupported parquet LogicalTypeAnnotation: " + logicalType);
        } else if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return new Field(path, fieldType(nullable, new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale())), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            // Parquet DATE is days since Unix epoch (INT32) → Arrow Date32 (DAY)
            return new Field(path, fieldType(nullable, new ArrowType.Date(DateUnit.DAY)), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            // Honour the annotation's actual unit; MILLIS fits in 32 bits, MICROS/NANOS need 64
            TimeUnit unit = toArrowTimeUnit(time.getUnit());
            int bitWidth = (time.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) ? 32 : 64;
            return new Field(path, fieldType(nullable, new ArrowType.Time(unit, bitWidth)), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            // Honour the annotation's actual unit and UTC-adjustment flag
            TimeUnit unit = toArrowTimeUnit(ts.getUnit());
            String timezone = ts.isAdjustedToUTC() ? "UTC" : null;
            return new Field(path, fieldType(nullable, new ArrowType.Timestamp(unit, timezone)), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation integer) {
            return new Field(path, fieldType(nullable, new ArrowType.Int(integer.getBitWidth(), integer.isSigned())), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Utf8()), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Binary()), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Utf8()), null);
        } else if (logicalType instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) {
            return new Field(path, fieldType(nullable, new ArrowType.Interval(IntervalUnit.DAY_TIME)), null);
        } else {
            throw new IllegalArgumentException("Unsupported parquet LogicalTypeAnnotation: " + logicalType);
        }
    }

    private static FieldType fieldType(boolean nullable, ArrowType arrowType) {
        return nullable ? FieldType.nullable(arrowType) : FieldType.notNullable(arrowType);
    }

    private static TimeUnit toArrowTimeUnit(LogicalTypeAnnotation.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> TimeUnit.MILLISECOND;
            case MICROS -> TimeUnit.MICROSECOND;
            case NANOS  -> TimeUnit.NANOSECOND;
        };
    }

    /**
     * Конвертация primitive типа Parquet.
     *
     *      * INT64
     *      * INT32
     *      * BOOLEAN
     *      * BINARY
     *      * FLOAT
     *      * DOUBLE
     *      * INT96
     *      * FIXED_LEN_BYTE_ARRAY
     */
    private static Field convertPrimitive(String path, PrimitiveType parquetType, boolean nullable) {
        // When a column has an old-style Parquet annotation (OriginalType / ConvertedType) but
        // the caller already consumed getLogicalTypeAnnotation() and got null (parquet-java may
        // not populate logicalTypeAnnotation for legacy OriginalType metadata), fall back here
        // so INT_8/INT_16/UINT_8/UINT_16/DATE/DECIMAL etc. are mapped correctly — matching what
        // Acero's C++ Parquet reader returns for the same column.
        OriginalType origType = parquetType.getOriginalType();
        if (origType != null) {
            switch (parquetType.getPrimitiveTypeName()) {
                case INT32:
                    switch (origType) {
                        case INT_8:   return new Field(path, fieldType(nullable, new ArrowType.Int(8,  true)),  null);
                        case INT_16:  return new Field(path, fieldType(nullable, new ArrowType.Int(16, true)),  null);
                        case UINT_8:  return new Field(path, fieldType(nullable, new ArrowType.Int(8,  false)), null);
                        case UINT_16: return new Field(path, fieldType(nullable, new ArrowType.Int(16, false)), null);
                        case DATE:    return new Field(path, fieldType(nullable, new ArrowType.Date(DateUnit.DAY)), null);
                        case DECIMAL: {
                            org.apache.parquet.schema.DecimalMetadata dm = parquetType.getDecimalMetadata();
                            return new Field(path, fieldType(nullable, new ArrowType.Decimal(dm.getPrecision(), dm.getScale())), null);
                        }
                        default: break;
                    }
                    break;
                case INT64:
                    switch (origType) {
                        case UINT_64:         return new Field(path, fieldType(nullable, new ArrowType.Int(64, false)), null);
                        case TIMESTAMP_MILLIS: return new Field(path, fieldType(nullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
                        case TIMESTAMP_MICROS: return new Field(path, fieldType(nullable, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);
                        case DECIMAL: {
                            org.apache.parquet.schema.DecimalMetadata dm = parquetType.getDecimalMetadata();
                            return new Field(path, fieldType(nullable, new ArrowType.Decimal(dm.getPrecision(), dm.getScale())), null);
                        }
                        default: break;
                    }
                    break;
                case BINARY:
                    if (origType == OriginalType.UTF8 || origType == OriginalType.ENUM || origType == OriginalType.JSON) {
                        return new Field(path, fieldType(nullable, new ArrowType.Utf8()), null);
                    }
                    if (origType == OriginalType.DECIMAL) {
                        org.apache.parquet.schema.DecimalMetadata dm = parquetType.getDecimalMetadata();
                        return new Field(path, fieldType(nullable, new ArrowType.Decimal(dm.getPrecision(), dm.getScale())), null);
                    }
                    break;
                default: break;
            }
        }

        switch (parquetType.getPrimitiveTypeName()) {
            case INT64:
                return new Field(path, fieldType(nullable, new ArrowType.Int(64, true)), null);
            case INT32:
                return new Field(path, fieldType(nullable, new ArrowType.Int(32, true)), null);
            case BOOLEAN:
                return new Field(path, fieldType(nullable, new ArrowType.Bool()), null);
            case BINARY:
                return new Field(path, fieldType(nullable, new ArrowType.Binary()), null);
            case FLOAT:
                return new Field(path, fieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
            case DOUBLE:
                return new Field(path, fieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            case INT96:
                // Legacy Hive/Impala timestamp: 12-byte INT96 = nanoseconds since epoch, not UTC-adjusted
                return new Field(path, fieldType(nullable, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)), null);
            case FIXED_LEN_BYTE_ARRAY:
                return new Field(path, fieldType(nullable, new ArrowType.FixedSizeBinary(parquetType.getTypeLength())), null);
            default:
                throw new IllegalArgumentException("Unsupported parquet PrimitiveType: " + parquetType);
        }
    }

    public static Schema convert(MessageType parquetSchema) {
        return convert(parquetSchema, null);
    }

    public static Schema convert(MessageType parquetSchema, Predicate<ColumnDescriptor> includeColumn) {
        List<Field> fields = new ArrayList<>();
        for (ColumnDescriptor columnDescriptor : parquetSchema.getColumns()) {
            if (includeColumn == null || includeColumn.test(columnDescriptor)) {
                fields.add(convert(columnDescriptor));
            }
        }

        return new Schema(fields, null);
    }

    /**
     *  from https://github.com/Alipsa/JParq/blob/main/src/main/java/se/alipsa/jparq/JParqDatabaseMetaData.java
     */
    public static List<TypeInfo> buildTypeInfo() {
        List<TypeInfo> types = new ArrayList<>();
        types.add(new TypeInfo("ARRAY", Types.ARRAY, null, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typePredNone, false, false, false, "ARRAY", 0, 0, null, null, null));
        types.add(new TypeInfo("BIGINT", Types.BIGINT, 19, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "BIGINT", 0, 0, null, null, 10));
        types.add(new TypeInfo("BINARY", Types.BINARY, Integer.MAX_VALUE, null, null, "length", DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "BINARY", 0, 0, null, null, null));
        types.add(new TypeInfo("BOOLEAN", Types.BOOLEAN, 1, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "BOOLEAN", 0, 0, null, null, 2));
        types.add(new TypeInfo("DATE", Types.DATE, 10, "'", "'", null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "DATE", 0, 0, null, null, null));
        types.add(new TypeInfo("DECIMAL", Types.DECIMAL, 38, null, null, "precision, scale", DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "DECIMAL", 0, 38, null, null, 10));
        types.add(new TypeInfo("DOUBLE", Types.DOUBLE, 15, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "DOUBLE", 0, 0, null, null, 10));
        types.add(new TypeInfo("INTEGER", Types.INTEGER, 10, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "INTEGER", 0, 0, null, null, 10));
        types.add(new TypeInfo("REAL", Types.REAL, 7, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "REAL", 0, 0, null, null, 10));
        types.add(new TypeInfo("SMALLINT", Types.SMALLINT, 5, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "SMALLINT", 0, 0, null, null, 10));
        types.add(new TypeInfo("STRUCT", Types.STRUCT, null, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typePredNone, false, false, false, "STRUCT", 0, 0, null, null, null));
        types.add(new TypeInfo("TIME", Types.TIME, 15, "'", "'", null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "TIME", 0, 6, null, null, null));
        types.add(new TypeInfo("TIMESTAMP", Types.TIMESTAMP, 26, "'", "'", null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "TIMESTAMP", 0, 6, null, null, null));
        types.add(new TypeInfo("TINYINT", Types.TINYINT, 3, null, null, null, DatabaseMetaData.typeNullable, false, DatabaseMetaData.typeSearchable, false, false, false, "TINYINT", 0, 0, null, null, 10));
        types.add(new TypeInfo("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, "'", "'", "length", DatabaseMetaData.typeNullable, true, DatabaseMetaData.typeSearchable, false, false, false, "VARCHAR", 0, 0, null, null, null));
        types.sort(Comparator.comparingInt(TypeInfo::dataType).thenComparing(TypeInfo::typeName));
        return List.copyOf(types);
    }

    public record TypeInfo(String typeName, int dataType, Integer precision, String literalPrefix, String literalSuffix,
                            String createParams, int nullable, boolean caseSensitive, int searchable, boolean unsignedAttribute,
                            boolean fixedPrecScale, boolean autoIncrement, String localTypeName, Integer minimumScale, Integer maximumScale,
                            Integer sqlDataType, Integer sqlDatetimeSub, Integer numPrecRadix) {

        Object[] toRow() {
            return new Object[]{
                    typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive,
                    searchable, unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale,
                    sqlDataType, sqlDatetimeSub, numPrecRadix
            };
        }

        Map<String, ?> toMap() {
            Map<String, Object> result = new HashMap<>();
            result.put("TYPE_NAME", this.typeName());
            result.put("DATA_TYPE", this.dataType());
            result.put("PRECISION", this.precision());
            result.put("LITERAL_PREFIX", this.literalPrefix());
            result.put("LITERAL_SUFFIX", this.literalSuffix());
            result.put("CREATE_PARAMS", this.createParams());
            result.put("NONULLS", this.nullable());
            result.put("CASE_SENSATIVE", this.caseSensitive());
            result.put("SEARCHABLE", this.searchable());
            result.put("UNSIGNED_ATTRIBUTE", this.unsignedAttribute());
            result.put("FIXED_PREC_SCALE", this.fixedPrecScale());
            result.put("AUTO_INCREMENT", this.autoIncrement());
            result.put("LOCAL_TYPE_NAME", this.localTypeName());
            result.put("MINIMUM_SCALE", this.minimumScale());
            result.put("MAXIMUM_SCALE", this.maximumScale());
            result.put("SQL_DATA_TYPE", this.sqlDataType());
            result.put("SQL_DATETIME_SUB", this.sqlDatetimeSub());
            result.put("NUM_PREC_RADIX", this.numPrecRadix());

            return result;
        }
    }
}
