package net.surpin.data.arrowflight.server;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetSchemaConverterTest {

    // ─── Primitive types (no logical annotation) ──────────────────────────

    @Test
    void int96MapsToTimestampNanosecondNoTimezone() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT96).named("ts"));
        assertEquals(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), f.getType());
        assertTrue(f.isNullable());
    }

    @Test
    void int64MapsToInt64Signed() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("n"));
        assertEquals(new ArrowType.Int(64, true), f.getType());
    }

    @Test
    void int32MapsToInt32Signed() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("n"));
        assertEquals(new ArrowType.Int(32, true), f.getType());
    }

    @Test
    void booleanMapsToArrowBool() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("b"));
        assertEquals(new ArrowType.Bool(), f.getType());
    }

    @Test
    void binaryMapsToArrowBinary() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("bin"));
        assertEquals(new ArrowType.Binary(), f.getType());
    }

    @Test
    void floatMapsToSinglePrecision() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("flt"));
        assertInstanceOf(ArrowType.FloatingPoint.class, f.getType());
        assertEquals(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE,
                ((ArrowType.FloatingPoint) f.getType()).getPrecision());
    }

    @Test
    void doubleMapsToDoublePrecision() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("dbl"));
        assertInstanceOf(ArrowType.FloatingPoint.class, f.getType());
        assertEquals(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE,
                ((ArrowType.FloatingPoint) f.getType()).getPrecision());
    }

    @Test
    void fixedLenByteArrayMapsToFixedSizeBinary() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(16).named("uuid"));
        assertInstanceOf(ArrowType.FixedSizeBinary.class, f.getType());
        assertEquals(16, ((ArrowType.FixedSizeBinary) f.getType()).getByteWidth());
    }

    // ─── Nullability ──────────────────────────────────────────────────────

    @Test
    void requiredTypeIsNotNullable() {
        Field f = convert(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"));
        assertFalse(f.isNullable());
    }

    @Test
    void optionalTypeIsNullable() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("id"));
        assertTrue(f.isNullable());
    }

    // ─── Logical type annotations ─────────────────────────────────────────

    @Test
    void dateMapsToArrowDateDay() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.dateType()).named("dt"));
        assertEquals(new ArrowType.Date(DateUnit.DAY), f.getType());
    }

    @Test
    void timeMillisMapsTo32BitMillisecond() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("t_ms"));
        ArrowType.Time time = (ArrowType.Time) f.getType();
        assertEquals(TimeUnit.MILLISECOND, time.getUnit());
        assertEquals(32, time.getBitWidth());
    }

    @Test
    void timeMicrosMapsTo64BitMicrosecond() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("t_us"));
        ArrowType.Time time = (ArrowType.Time) f.getType();
        assertEquals(TimeUnit.MICROSECOND, time.getUnit());
        assertEquals(64, time.getBitWidth());
    }

    @Test
    void timeNanosMapsTo64BitNanosecond() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
                .named("t_ns"));
        ArrowType.Time time = (ArrowType.Time) f.getType();
        assertEquals(TimeUnit.NANOSECOND, time.getUnit());
        assertEquals(64, time.getBitWidth());
    }

    @Test
    void timestampUtcAdjustedMapsToUtcTimezone() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("ts_utc"));
        ArrowType.Timestamp ts = (ArrowType.Timestamp) f.getType();
        assertEquals(TimeUnit.MICROSECOND, ts.getUnit());
        assertEquals("UTC", ts.getTimezone());
    }

    @Test
    void timestampNotUtcAdjustedMapsToNullTimezone() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("ts_local"));
        ArrowType.Timestamp ts = (ArrowType.Timestamp) f.getType();
        assertEquals(TimeUnit.MILLISECOND, ts.getUnit());
        assertNull(ts.getTimezone());
    }

    @Test
    void intLogicalTypePreservesBitWidthAndSignedness() {
        Field int8 = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(8, true)).named("i8"));
        assertEquals(new ArrowType.Int(8, true), int8.getType());

        Field uint16 = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(16, false)).named("u16"));
        assertEquals(new ArrowType.Int(16, false), uint16.getType());
    }

    @Test
    void stringLogicalTypeMapsToUtf8() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType()).named("s"));
        assertEquals(new ArrowType.Utf8(), f.getType());
    }

    @Test
    void jsonLogicalTypeMapsToUtf8() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.jsonType()).named("j"));
        assertEquals(new ArrowType.Utf8(), f.getType());
    }

    @Test
    void bsonLogicalTypeMapsToBinary() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.bsonType()).named("bson"));
        assertEquals(new ArrowType.Binary(), f.getType());
    }

    @Test
    void decimalLogicalTypePreservesPrecisionAndScale() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(3, 10)).named("dec"));
        assertInstanceOf(ArrowType.Decimal.class, f.getType());
        ArrowType.Decimal dec = (ArrowType.Decimal) f.getType();
        assertEquals(10, dec.getPrecision());
        assertEquals(3, dec.getScale());
    }

    // ─── Schema-level conversion ──────────────────────────────────────────

    @Test
    void convertMessageTypeProducesCorrectFieldCount() {
        MessageType parquetSchema = new MessageType("msg",
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).named("name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT96).named("ts")
        );
        Schema arrowSchema = ParquetSchemaConverter.convert(parquetSchema);
        assertEquals(3, arrowSchema.getFields().size());
    }

    @Test
    void columnPredicateFiltersFields() {
        MessageType parquetSchema = new MessageType("msg",
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).named("name"),
                Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("score")
        );
        Schema arrowSchema = ParquetSchemaConverter.convert(parquetSchema,
                col -> col.getPath()[0].equals("id") || col.getPath()[0].equals("score"));
        assertEquals(2, arrowSchema.getFields().size());
        List<String> names = arrowSchema.getFields().stream().map(Field::getName).toList();
        assertTrue(names.contains("id") && names.contains("score"));
        assertFalse(names.contains("name"));
    }

    @Test
    void nullPredicateMeansAllColumns() {
        MessageType parquetSchema = new MessageType("msg",
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("a"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("b")
        );
        Schema arrowSchema = ParquetSchemaConverter.convert(parquetSchema, null);
        assertEquals(2, arrowSchema.getFields().size());
    }

    // ─── Legacy OriginalType backward-compatibility ───────────────────────
    //
    // Parquet files written by older writers (pre-1.12 Parquet format) carry
    // ConvertedType / OriginalType annotations instead of the new LogicalType.
    // parquet-java may not populate getLogicalTypeAnnotation() for those fields,
    // causing convertPrimitive() to be reached with a non-null getOriginalType().
    // These tests pin the fallback behaviour added to fix the ClassCastException
    // "Byte cannot be cast to Integer" that occurred when Spark's join codegen
    // called getInt() on a TinyIntVector value because the schema incorrectly
    // reported the column as INT32/IntegerType instead of INT8/ByteType.

    @Test
    void legacyInt8OriginalTypeMapsToInt8Signed() {
        // Reproduces the root cause of the Byte→Integer ClassCastException.
        // Old Parquet writers encode TINYINT as INT32 + OriginalType.INT_8.
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.INT_8).named("tinyint_col"));
        assertEquals(new ArrowType.Int(8, true), f.getType(),
                "INT32 + INT_8 must map to Arrow INT8 (ByteType in Spark), not INT32 (IntegerType)");
    }

    @Test
    void legacyInt16OriginalTypeMapsToInt16Signed() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.INT_16).named("smallint_col"));
        assertEquals(new ArrowType.Int(16, true), f.getType());
    }

    @Test
    void legacyUint8OriginalTypeMapsToInt8Unsigned() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.UINT_8).named("uint8_col"));
        assertEquals(new ArrowType.Int(8, false), f.getType());
    }

    @Test
    void legacyUint16OriginalTypeMapsToInt16Unsigned() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.UINT_16).named("uint16_col"));
        assertEquals(new ArrowType.Int(16, false), f.getType());
    }

    @Test
    void legacyDateOriginalTypeMapsToArrowDateDay() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.DATE).named("dt"));
        assertEquals(new ArrowType.Date(DateUnit.DAY), f.getType());
    }

    @Test
    void legacyTimestampMillisOriginalTypeMapsToMillisecondTimestamp() {
        // Old TIMESTAMP_MILLIS is always UTC-adjusted per the Parquet spec.
        // parquet-java converts it to TimestampLogicalTypeAnnotation(adjustedToUTC=true),
        // which our converter maps to timezone="UTC".
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(OriginalType.TIMESTAMP_MILLIS).named("ts_ms"));
        ArrowType.Timestamp ts = (ArrowType.Timestamp) f.getType();
        assertEquals(TimeUnit.MILLISECOND, ts.getUnit());
        assertEquals("UTC", ts.getTimezone());
    }

    @Test
    void legacyTimestampMicrosOriginalTypeMapsToMicrosecondTimestamp() {
        // Same as above: old TIMESTAMP_MICROS is UTC-adjusted.
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(OriginalType.TIMESTAMP_MICROS).named("ts_us"));
        ArrowType.Timestamp ts = (ArrowType.Timestamp) f.getType();
        assertEquals(TimeUnit.MICROSECOND, ts.getUnit());
        assertEquals("UTC", ts.getTimezone());
    }

    @Test
    void legacyUtf8OriginalTypeMapsToArrowUtf8() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(OriginalType.UTF8).named("str_col"));
        assertEquals(new ArrowType.Utf8(), f.getType());
    }

    @Test
    void legacyDecimalOnInt32MapsToArrowDecimal() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                .as(OriginalType.DECIMAL).precision(9).scale(2).named("dec32"));
        assertInstanceOf(ArrowType.Decimal.class, f.getType());
        ArrowType.Decimal dec = (ArrowType.Decimal) f.getType();
        assertEquals(9, dec.getPrecision());
        assertEquals(2, dec.getScale());
    }

    @Test
    void legacyDecimalOnInt64MapsToArrowDecimal() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                .as(OriginalType.DECIMAL).precision(18).scale(4).named("dec64"));
        assertInstanceOf(ArrowType.Decimal.class, f.getType());
        ArrowType.Decimal dec = (ArrowType.Decimal) f.getType();
        assertEquals(18, dec.getPrecision());
        assertEquals(4, dec.getScale());
    }

    @Test
    void legacyDecimalOnBinaryMapsToArrowDecimal() {
        Field f = convert(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(OriginalType.DECIMAL).precision(38).scale(10).named("dec_bin"));
        assertInstanceOf(ArrowType.Decimal.class, f.getType());
        ArrowType.Decimal dec = (ArrowType.Decimal) f.getType();
        assertEquals(38, dec.getPrecision());
        assertEquals(10, dec.getScale());
    }

    // ─── Regression: JOIN on tinyint_col ClassCastException ──────────────
    //
    // Reproduces: SELECT count(*) FROM t1, t2 WHERE t1.id = t2.tinyint_col
    // Root cause: a Parquet file written by an old writer encodes tinyint_col
    // as INT32 + OriginalType.INT_8.  Before the fix, convertPrimitive() ignored
    // OriginalType and returned ArrowType.Int(32) → Spark IntegerType.  Acero,
    // however, honours INT_8 and returns TinyIntVector → Byte.  Spark's
    // whole-stage codegen called getInt() on a Byte value → ClassCastException.
    // After the fix the schema must agree with Acero: tinyint_col → Int(8, true).

    @Test
    void joinOnTinyintColSchemaAgreesWithAcero() {
        MessageType parquetSchema = new MessageType("flight_table",
                Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
                Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                        .as(OriginalType.INT_8).named("tinyint_col")
        );
        Schema arrowSchema = ParquetSchemaConverter.convert(parquetSchema);

        Field id = arrowSchema.findField("id");
        Field tinyint = arrowSchema.findField("tinyint_col");

        // id is a plain INT32 — must stay Int(32)
        assertEquals(new ArrowType.Int(32, true), id.getType(),
                "id (plain INT32) must map to Arrow INT32");

        // tinyint_col carries OriginalType.INT_8 — must be Int(8), not Int(32),
        // so that Spark's codegen calls getByte() rather than getInt() on the
        // TinyIntVector that Acero returns.
        assertEquals(new ArrowType.Int(8, true), tinyint.getType(),
                "INT32+INT_8 must map to Arrow INT8 (ByteType), not INT32 (IntegerType)");
        assertNotEquals(new ArrowType.Int(32, true), tinyint.getType(),
                "tinyint_col must NOT be reported as INT32 — that causes ClassCastException in JOIN codegen");
    }

    // ─── helpers ──────────────────────────────────────────────────────────

    private static Field convert(org.apache.parquet.schema.Type type) {
        return ParquetSchemaConverter.convert(type.getName(), type);
    }
}
