package net.surpin.data.arrowflight.client;

import com.google.common.collect.Streams;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.arrow.vector.util.JsonStringHashMap;
import scala.collection.JavaConverters;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Defines utility methods for transformation & conversion
 */
public final class ArrowConversion implements Serializable {
    //the fromConversion interface
    @FunctionalInterface
    private interface ConvertFrom<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the toConversion interface
    @FunctionalInterface
    private interface ConvertTo<A, B, C, D> {
        void apply(A a, B b, C c, D d);
    }
    //the cast method
    @SuppressWarnings("unchecked")
    private static <V extends org.apache.arrow.vector.FieldVector> V cast(org.apache.arrow.vector.FieldVector fv) {
        try {
            return (V) fv;
        } catch (Exception e) {
            throw new RuntimeException(String.format("ArrowVector casting to [%s] failed.", fv.getClass().getTypeName()), e);
        }
    }

    //the from-converter container
    private final transient Map<String, ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector>> fromConverters = new java.util.HashMap<>();
    //the to-converter container
    private final transient Map<String, ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType>> toConverters = new java.util.HashMap<>();
    //the to-object-converter container
    private final transient Map<String, ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType>> toObjectConverters = new java.util.HashMap<>();

    /**
     * Construct a Vector object
     */
    private ArrowConversion() {
        this.initializeFrom();
        this.initializeTo();
    }

    /**
     * Convert an arrow FieldVector into a custom FieldVector
     * @param vector - the arrow FieldVector
     * @param type - the type of all elements in the vector
     * @param rowCount - the number of rows in the vector
     * @return - a custom FieldVector hodling java-type objects
     */
    public FieldVector convert(org.apache.arrow.vector.FieldVector vector, FieldType type, int rowCount) {
        String key = String.format("%s-%s", vector.getClass().getTypeName(), type.getTypeID());
        if (!this.fromConverters.containsKey(key)) {
            throw new RuntimeException(String.format("The %s doesn't have a converter defined.", key));
        }
        return this.fromConverters.get(key).apply(vector, rowCount, type);
    }

    /**
     * Populate an arrow FieldVector with data in rows
     * @param vector - the target arrow vector
     * @param rows - the rows containing source data
     * @param idxColumn - the index of column in rows whose data is to be used for the population
     * @param type - the data-type in the target column.
     */
    public void populate(org.apache.arrow.vector.FieldVector vector, InternalRow[] rows, int idxColumn, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this.toConverters.containsKey(key)) {
            throw new RuntimeException(String.format("The %s doesn't have a converter defined.", key));
        }
        this.toConverters.get(key).apply(vector, rows, idxColumn, type);
    }
    //populate the value object into the vector
    private void populateObject(org.apache.arrow.vector.FieldVector vector, int index, Object value, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this.toObjectConverters.containsKey(key)) {
            throw new RuntimeException(String.format("The %s doesn't have a converter defined.", key));
        }
        this.toObjectConverters.get(key).apply(vector, index, value, type);
    }

    /**
     * Initialize from-converters
     */
    private void initializeFrom() {
        this.fromConverters.put(String.format("%s-%s", TinyIntVector.class.getTypeName(), FieldType.IDs.BYTE), ArrowConversion.fromTinyInt);
        this.fromConverters.put(String.format("%s-%s", SmallIntVector.class.getTypeName(), FieldType.IDs.SHORT), ArrowConversion.fromSmallInt);
        this.fromConverters.put(String.format("%s-%s", IntVector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromInt);
        this.fromConverters.put(String.format("%s-%s", BigIntVector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromBigInt);
        this.fromConverters.put(String.format("%s-%s", UInt1Vector.class.getTypeName(), FieldType.IDs.SHORT), ArrowConversion.fromUInt1);
        this.fromConverters.put(String.format("%s-%s", UInt2Vector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromUInt2);
        this.fromConverters.put(String.format("%s-%s", UInt4Vector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromUInt4Int);
        this.fromConverters.put(String.format("%s-%s", UInt4Vector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromUInt4Long);
        this.fromConverters.put(String.format("%s-%s", UInt8Vector.class.getTypeName(), FieldType.IDs.LONG), ArrowConversion.fromUInt8Long);
        this.fromConverters.put(String.format("%s-%s", UInt8Vector.class.getTypeName(), FieldType.IDs.BIGINT), ArrowConversion.fromUInt8Bigint);
        this.fromConverters.put(String.format("%s-%s", Float4Vector.class.getTypeName(), FieldType.IDs.FLOAT), ArrowConversion.fromFloat4);
        this.fromConverters.put(String.format("%s-%s", Float8Vector.class.getTypeName(), FieldType.IDs.DOUBLE), ArrowConversion.fromFloat8);
        this.fromConverters.put(String.format("%s-%s", DecimalVector.class.getTypeName(), FieldType.IDs.DECIMAL), ArrowConversion.fromDecimal);
        this.fromConverters.put(String.format("%s-%s", Decimal256Vector.class.getTypeName(), FieldType.IDs.DECIMAL), ArrowConversion.fromDecimal256);
        this.fromConverters.put(String.format("%s-%s", VarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), ArrowConversion.fromVarChar);
        this.fromConverters.put(String.format("%s-%s", LargeVarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), ArrowConversion.fromLargeVarChar);
        this.fromConverters.put(String.format("%s-%s", BitVector.class.getTypeName(), FieldType.IDs.BOOLEAN), ArrowConversion.fromBit);
        this.fromConverters.put(String.format("%s-%s", DateDayVector.class.getTypeName(), FieldType.IDs.DATE), ArrowConversion.fromDateDay);
        this.fromConverters.put(String.format("%s-%s", DateMilliVector.class.getTypeName(), FieldType.IDs.DATE), ArrowConversion.fromDateMilli);
        this.fromConverters.put(String.format("%s-%s", TimeSecVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeSec);
        this.fromConverters.put(String.format("%s-%s", TimeMilliVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeMilli);
        this.fromConverters.put(String.format("%s-%s", TimeMicroVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeMicro);
        this.fromConverters.put(String.format("%s-%s", TimeNanoVector.class.getTypeName(), FieldType.IDs.TIME), ArrowConversion.fromTimeNano);
        this.fromConverters.put(String.format("%s-%s", TimeStampMicroVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMicro);
        this.fromConverters.put(String.format("%s-%s", TimeStampMicroTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMicroTZ);
        this.fromConverters.put(String.format("%s-%s", TimeStampMilliVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMilli);
        this.fromConverters.put(String.format("%s-%s", TimeStampMilliTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampMilliTZ);
        this.fromConverters.put(String.format("%s-%s", TimeStampSecVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampSec);
        this.fromConverters.put(String.format("%s-%s", TimeStampSecTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampSecTZ);
        this.fromConverters.put(String.format("%s-%s", TimeStampNanoVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampNano);

        this.fromConverters.put(String.format("%s-%s", TimeStampNanoVector.class.getTypeName(), FieldType.IDs.INT), ArrowConversion.fromTimeStampNanoToInt);

        this.fromConverters.put(String.format("%s-%s", TimeStampNanoTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), ArrowConversion.fromTimeStampNanoTZ);
        this.fromConverters.put(String.format("%s-%s", IntervalYearVector.class.getTypeName(), FieldType.IDs.PERIOD_YEAR_MONTH), ArrowConversion.fromIntervalYear);
        this.fromConverters.put(String.format("%s-%s", IntervalDayVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), ArrowConversion.fromIntervalDay);
        this.fromConverters.put(String.format("%s-%s", DurationVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), ArrowConversion.fromDuration);
        this.fromConverters.put(String.format("%s-%s", IntervalMonthDayNanoVector.class.getTypeName(), FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME), ArrowConversion.fromMonthDay);
        this.fromConverters.put(String.format("%s-%s", NullVector.class.getTypeName(), FieldType.IDs.NULL), ArrowConversion.fromNull);
        this.fromConverters.put(String.format("%s-%s", VarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromVarBinary);
        this.fromConverters.put(String.format("%s-%s", LargeVarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromLargeVarBinary);
        this.fromConverters.put(String.format("%s-%s", FixedSizeBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), ArrowConversion.fromFixedSizeBinary);
        this.fromConverters.put(String.format("%s-%s", LargeListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromLargeList);
        this.fromConverters.put(String.format("%s-%s", FixedSizeListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromFixedSizeList);
        this.fromConverters.put(String.format("%s-%s", MapVector.class.getTypeName(), FieldType.IDs.MAP), ArrowConversion.fromMap);
        this.fromConverters.put(String.format("%s-%s", ListVector.class.getTypeName(), FieldType.IDs.LIST), ArrowConversion.fromList);
        this.fromConverters.put(String.format("%s-%s", StructVector.class.getTypeName(), FieldType.IDs.STRUCT), ArrowConversion.fromStruct);
    }
    //convert arrow TinyIntVector to FieldVector for BYTE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTinyInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TinyIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow SmallIntVector to FieldVector for SHORT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromSmallInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<SmallIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow IntVector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<IntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow BigIntVector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromBigInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<BigIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt1Vector to FieldVector for SHORT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt1 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt1Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt2Vector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt2 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt2Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for INT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt4Int = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt4Long = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt4Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for LONG
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt8Long = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for BIGINT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromUInt8Bigint = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<UInt8Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow Float4Vector to FieldVector for FLOAT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFloat4 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<Float4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow Float8Vector to FieldVector for DOUBLE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFloat8 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<Float8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DecimalVector to FieldVector for DECIMAL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDecimal = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<DecimalVector>cast(vector))::getObject).map(bd -> (bd == null) ? null : ArrowConversion.bigDecimalToDecimal.apply(bd)).toArray(Object[]::new));
    //convert arrow Decimal256Vector to FieldVector for DECIMAL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDecimal256 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<Decimal256Vector>cast(vector))::getObject).map(bd -> (bd == null) ? null : ArrowConversion.bigDecimalToDecimal.apply(bd)).toArray(Object[]::new));
    //convert arrow VarCharVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<VarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : ArrowConversion.stringToUtf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow LargeVarCharVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<LargeVarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : ArrowConversion.stringToUtf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow BitVector to FieldVector for STRING
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromBit = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<BitVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DateDayVector to FieldVector for DATE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDateDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<DateDayVector>cast(vector))::getObject).map(dd -> (dd == null) ? null : ArrowConversion.dateDayToInt.apply(dd)).toArray(Object[]::new));
    //convert arrow DateMilliVector to FieldVector for DATE
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDateMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<DateMilliVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : ArrowConversion.localDateTimeToInt.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeSecVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeSecVector>cast(vector))::getObject).map(ts -> (ts == null) ? null : ArrowConversion.timeSecToString.apply(ts)).toArray(Object[]::new));
    //convert arrow TimeMilliVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeMilliVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : ArrowConversion.timeMilliToString.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeMicroVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeMicroVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : ArrowConversion.timeMicroToString.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeNanoVector to FieldVector for TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeNanoVector>cast(vector))::getObject).map(tn -> (tn == null) ? null : ArrowConversion.timeNanoToString.apply(tn)).toArray(Object[]::new));
    //convert arrow TimeStampMicroVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeStampMicroVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : ArrowConversion.localDateTimeToLong.apply(tsm)).toArray(Object[]::new));
    //convert arrow TimeStampMicroTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMicroTZ = (vector, size, type) -> {
        TimeStampMicroTZVector value = ArrowConversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : ArrowConversion.timestampMicroTZToLong.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampMilliVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeStampMilliVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : ArrowConversion.timestampToLong.apply(Timestamp.valueOf(tsm))).toArray(Object[]::new));
    //convert arrow TimeStampMilliTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampMilliTZ = (vector, size, type) -> {
        TimeStampMilliTZVector value = ArrowConversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : ArrowConversion.timestampMilliTZToLong.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampSecVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeStampSecVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : ArrowConversion.localDateTimeToLong.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeStampSecTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampSecTZ = (vector, size, type) -> {
        TimeStampSecTZVector value = ArrowConversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : ArrowConversion.timestampSecTZToLong.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampNanoVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeStampNanoVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : ArrowConversion.localDateTimeToLong.apply(ldt)).toArray(Object[]::new));

    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNanoToInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<TimeStampNanoVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : ArrowConversion.localDateTimeToInt.apply(ldt)).toArray(Object[]::new));


    //convert arrow TimeStampNanoTZVector to FieldVector for TIMESTAMP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromTimeStampNanoTZ = (vector, size, type) -> {
        TimeStampNanoTZVector value = ArrowConversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : ArrowConversion.timestampNanoTZToLong.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow IntervalYearVector to FieldVector for PERIOD_YEAR_MONTH
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromIntervalYear = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<IntervalYearVector>cast(vector))::getObject).map(p -> (p == null) ? null : ArrowConversion.periodToInt.apply(p)).toArray(Object[]::new));
    //convert arrow IntervalDayVector to FieldVector for DURATION_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromIntervalDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<IntervalDayVector>cast(vector))::getObject).map(d -> (d == null) ? null : ArrowConversion.durationToLong.apply(d)).toArray(Object[]::new));
    //convert arrow DurationVector to FieldVector for DURATION_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromDuration = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<DurationVector>cast(vector))::getObject).map(d -> (d == null) ? null : ArrowConversion.durationToLong.apply(d)).toArray(Object[]::new));
    //convert arrow IntervalMonthDayNanoVector to FieldVector for PERIOD_DURATION_MONTH_DAY_TIME
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromMonthDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<IntervalMonthDayNanoVector>cast(vector))::getObject).map(pd -> (pd == null) ? null : ArrowConversion.translatePeriodDuration.apply(pd)).toArray(Object[]::new));
    //convert arrow NullVector to FieldVector for NULL
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromNull = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<NullVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow VarBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<VarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeVarBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<LargeVarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow FixedSizeBinaryVector to FieldVector for BYTES
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFixedSizeBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<FixedSizeBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromLargeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<LargeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : ArrowConversion.translateList.apply(e, ArrowConversion.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow FixedSizeListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromFixedSizeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<FixedSizeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : ArrowConversion.translateList.apply(e, ArrowConversion.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow MapVector to FieldVector for MAP
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromMap = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<MapVector>cast(vector))::getObject).map(e -> (e == null) ? null : ArrowConversion.translateMap.apply(e, ArrowConversion.cast(vector), (FieldType.MapType) type)).toArray(Object[]::new));
    //convert arrow ListVector to FieldVector for LIST
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<ListVector>cast(vector))::getObject).map(e -> (e == null) ? null : ArrowConversion.translateList.apply(e, ArrowConversion.cast(vector), (FieldType.ListType) type)).toArray(Object[]::new));
    //convert arrow StructVector to FieldVector for STRUCT
    private static final ConvertFrom<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> fromStruct = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((ArrowConversion.<StructVector>cast(vector))::getObject).map(e -> (e == null) ? null : ArrowConversion.mapElseStruct.apply(e, ArrowConversion.cast(vector), (FieldType.StructType) type)).toArray(Object[]::new));

    /**
     * Initialize to-converters
     */
    private void initializeTo() {
        this.toConverters.put(MapVector.class.getTypeName(), ArrowConversion.toMap);
        this.toObjectConverters.put(MapVector.class.getTypeName(), ArrowConversion.toMapObject);
        this.toConverters.put(ListVector.class.getTypeName(), ArrowConversion.toList);
        this.toObjectConverters.put(ListVector.class.getTypeName(), ArrowConversion.toListObject);
        this.toConverters.put(StructVector.class.getTypeName(), ArrowConversion.toStruct);
        this.toObjectConverters.put(StructVector.class.getTypeName(), ArrowConversion.toStructObject);
        this.toConverters.put(IntervalDayVector.class.getTypeName(), ArrowConversion.toIntervalDay);
        this.toObjectConverters.put(IntervalDayVector.class.getTypeName(), ArrowConversion.toIntervalDayObject);
        this.toConverters.put(IntervalYearVector.class.getTypeName(), ArrowConversion.toIntervalYear);
        this.toObjectConverters.put(IntervalYearVector.class.getTypeName(), ArrowConversion.toIntervalYearObject);
        this.toConverters.put(DurationVector.class.getTypeName(), ArrowConversion.toDuration);
        this.toObjectConverters.put(DurationVector.class.getTypeName(), ArrowConversion.toDurationObject);
        this.toConverters.put(TimeStampNanoTZVector.class.getTypeName(), ArrowConversion.toTimeStampNanoTZ);
        this.toObjectConverters.put(TimeStampNanoTZVector.class.getTypeName(), ArrowConversion.toTimeStampNanoTZObject);
        this.toConverters.put(TimeStampNanoVector.class.getTypeName(), ArrowConversion.toTimeStampNano);
        this.toObjectConverters.put(TimeStampNanoVector.class.getTypeName(), ArrowConversion.toTimeStampNanoObject);
        this.toConverters.put(TimeStampSecTZVector.class.getTypeName(), ArrowConversion.toTimeStampSecTZ);
        this.toObjectConverters.put(TimeStampSecTZVector.class.getTypeName(), ArrowConversion.toTimeStampSecTZObject);
        this.toConverters.put(TimeStampSecVector.class.getTypeName(), ArrowConversion.toTimeStampSec);
        this.toObjectConverters.put(TimeStampSecVector.class.getTypeName(), ArrowConversion.toTimeStampSecObject);
        this.toConverters.put(TimeStampMilliTZVector.class.getTypeName(), ArrowConversion.toTimeStampMilliTZ);
        this.toObjectConverters.put(TimeStampMilliTZVector.class.getTypeName(), ArrowConversion.toTimeStampMilliTZObject);
        this.toConverters.put(TimeStampMilliVector.class.getTypeName(), ArrowConversion.toTimeStampMilli);
        this.toObjectConverters.put(TimeStampMilliVector.class.getTypeName(), ArrowConversion.toTimeStampMilliObject);
        this.toConverters.put(TimeStampMicroTZVector.class.getTypeName(), ArrowConversion.toTimeStampMicroTZ);
        this.toObjectConverters.put(TimeStampMicroTZVector.class.getTypeName(), ArrowConversion.toTimeStampMicroTZObject);
        this.toConverters.put(TimeStampMicroVector.class.getTypeName(), ArrowConversion.toTimeStampMicro);
        this.toObjectConverters.put(TimeStampMicroVector.class.getTypeName(), ArrowConversion.toTimeStampMicroObject);
        this.toConverters.put(TimeNanoVector.class.getTypeName(), ArrowConversion.toTimeNano);
        this.toObjectConverters.put(TimeNanoVector.class.getTypeName(), ArrowConversion.toTimeNanoObject);
        this.toConverters.put(TimeMicroVector.class.getTypeName(), ArrowConversion.toTimeMicro);
        this.toObjectConverters.put(TimeMicroVector.class.getTypeName(), ArrowConversion.toTimeMicroObject);
        this.toConverters.put(TimeMilliVector.class.getTypeName(), ArrowConversion.toTimeMilli);
        this.toObjectConverters.put(TimeMilliVector.class.getTypeName(), ArrowConversion.toTimeMilliObject);
        this.toConverters.put(TimeSecVector.class.getTypeName(), ArrowConversion.toTimeSec);
        this.toObjectConverters.put(TimeSecVector.class.getTypeName(), ArrowConversion.toTimeSecObject);
        this.toConverters.put(DateMilliVector.class.getTypeName(), ArrowConversion.toDateMilli);
        this.toObjectConverters.put(DateMilliVector.class.getTypeName(), ArrowConversion.toDateMilliObject);
        this.toConverters.put(DateDayVector.class.getTypeName(), ArrowConversion.toDateDay);
        this.toObjectConverters.put(DateDayVector.class.getTypeName(), ArrowConversion.toDateDayObject);
        this.toConverters.put(BitVector.class.getTypeName(), ArrowConversion.toBit);
        this.toObjectConverters.put(BitVector.class.getTypeName(), ArrowConversion.toBitObject);
        this.toConverters.put(LargeVarCharVector.class.getTypeName(), ArrowConversion.toLargeVarChar);
        this.toObjectConverters.put(LargeVarCharVector.class.getTypeName(), ArrowConversion.toLargeVarCharObject);
        this.toConverters.put(VarCharVector.class.getTypeName(), ArrowConversion.toVarChar);
        this.toObjectConverters.put(VarCharVector.class.getTypeName(), ArrowConversion.toVarCharObject);
        this.toConverters.put(Decimal256Vector.class.getTypeName(), ArrowConversion.toDecimal256);
        this.toObjectConverters.put(Decimal256Vector.class.getTypeName(), ArrowConversion.toDecimal256Object);
        this.toConverters.put(DecimalVector.class.getTypeName(), ArrowConversion.toDecimal);
        this.toObjectConverters.put(DecimalVector.class.getTypeName(), ArrowConversion.toDecimalObject);
        this.toConverters.put(Float8Vector.class.getTypeName(), ArrowConversion.toFloat8);
        this.toObjectConverters.put(Float8Vector.class.getTypeName(), ArrowConversion.toFloat8Object);
        this.toConverters.put(Float4Vector.class.getTypeName(), ArrowConversion.toFloat4);
        this.toObjectConverters.put(Float4Vector.class.getTypeName(), ArrowConversion.toFloat4Object);
        this.toConverters.put(UInt8Vector.class.getTypeName(), ArrowConversion.toUInt8);
        this.toObjectConverters.put(UInt8Vector.class.getTypeName(), ArrowConversion.toUInt8Object);
        this.toConverters.put(UInt4Vector.class.getTypeName(), ArrowConversion.toUInt4);
        this.toObjectConverters.put(UInt4Vector.class.getTypeName(), ArrowConversion.toUInt4Object);
        this.toConverters.put(UInt2Vector.class.getTypeName(), ArrowConversion.toUInt2);
        this.toObjectConverters.put(UInt2Vector.class.getTypeName(), ArrowConversion.toUInt2Object);
        this.toConverters.put(UInt1Vector.class.getTypeName(), ArrowConversion.toUInt1);
        this.toObjectConverters.put(UInt1Vector.class.getTypeName(), ArrowConversion.toUInt1Object);
        this.toConverters.put(BigIntVector.class.getTypeName(), ArrowConversion.toBigInt);
        this.toObjectConverters.put(BigIntVector.class.getTypeName(), ArrowConversion.toBigIntObject);
        this.toConverters.put(IntVector.class.getTypeName(), ArrowConversion.toInt);
        this.toObjectConverters.put(IntVector.class.getTypeName(), ArrowConversion.toIntObject);
        this.toConverters.put(SmallIntVector.class.getTypeName(), ArrowConversion.toSmallInt);
        this.toObjectConverters.put(SmallIntVector.class.getTypeName(), ArrowConversion.toSmallIntObject);
        this.toConverters.put(TinyIntVector.class.getTypeName(), ArrowConversion.toTinyInt);
        this.toObjectConverters.put(TinyIntVector.class.getTypeName(), ArrowConversion.toTinyIntObject);
    }
    //convert BYTE to arrow TinyIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTinyInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTinyIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableTinyIntHolder nullTinyInt = new NullableTinyIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTinyIntObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TinyIntVector>cast(vector).setSafe(row, ArrowConversion.nullTinyInt);
        } else {
            ArrowConversion.<TinyIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Byte) value : Byte.parseByte(value.toString()));
        }
    };
    //convert SHORT to arrow SmallIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toSmallInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toSmallIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableSmallIntHolder nullSmallInt = new NullableSmallIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toSmallIntObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<SmallIntVector>cast(vector).setSafe(row, ArrowConversion.nullSmallInt);
        } else {
            ArrowConversion.<SmallIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Short) value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow IntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableIntHolder nullInt = new NullableIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<IntVector>cast(vector).setSafe(row, ArrowConversion.nullInt);
        } else {
            ArrowConversion.<IntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow BigIntVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toBigInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toBigIntObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableBigIntHolder nullBigInt = new NullableBigIntHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toBigIntObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<BigIntVector>cast(vector).setSafe(row, ArrowConversion.nullBigInt);
        } else {
            ArrowConversion.<BigIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert SHORT to arrow UInt1Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt1 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt1Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt1Holder nullUInt1 = new NullableUInt1Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt1Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<UInt1Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt1);
        } else {
            ArrowConversion.<UInt1Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Short) value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow UInt2Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt2 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt2Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt2Holder nullUInt2 = new NullableUInt2Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt2Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<UInt2Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt2);
        } else {
            ArrowConversion.<UInt2Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert INT to arrow UInt4Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt4Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt4Holder nullUInt4 = new NullableUInt4Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt4Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<UInt4Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt4);
        } else {
            ArrowConversion.<UInt4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow UInt8Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toUInt8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toUInt8Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt8Holder nullUInt8 = new NullableUInt8Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toUInt8Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<UInt8Vector>cast(vector).setSafe(row, ArrowConversion.nullUInt8);
        } else {
            ArrowConversion.<UInt8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert FLOAT to arrow Float4Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toFloat4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toFloat4Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat4Holder nullFloat4 = new NullableFloat4Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toFloat4Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<Float4Vector>cast(vector).setSafe(row, ArrowConversion.nullFloat4);
        } else {
            ArrowConversion.<Float4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Float) value : Float.parseFloat(value.toString()));
        }
    };
    //convert DOUBLE to arrow Float8Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toFloat8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toFloat8Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat8Holder nullFloat8 = new NullableFloat8Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toFloat8Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<Float8Vector>cast(vector).setSafe(row, ArrowConversion.nullFloat8);
        } else {
            ArrowConversion.<Float8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Double) value : Double.parseDouble(value.toString()));
        }
    };
    //convert DECIMAL to arrow DecimalVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDecimal = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDecimalObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimalHolder nullDecimal = new NullableDecimalHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDecimalObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<DecimalVector>cast(vector).setSafe(row, ArrowConversion.nullDecimal);
        } else {
            ArrowConversion.<DecimalVector>cast(vector).setSafe(row, (value instanceof Decimal) ? ((Decimal) value).toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert DECIMAL to arrow Decimal256Vector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDecimal256 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDecimal256Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimal256Holder nullDecimal256 = new NullableDecimal256Holder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDecimal256Object = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<Decimal256Vector>cast(vector).setSafe(row, ArrowConversion.nullDecimal256);
        } else {
            ArrowConversion.<Decimal256Vector>cast(vector).setSafe(row, (value instanceof Decimal) ? ((Decimal) value).toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert STRING to arrow VarCharVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toVarCharObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableVarCharHolder nullVarChar = new NullableVarCharHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toVarCharObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<VarCharVector>cast(vector).setSafe(row, ArrowConversion.nullVarChar);
        } else {
            ArrowConversion.<VarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert STRING to arrow LargeVarCharVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toLargeVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toLargeVarCharObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableLargeVarCharHolder nullLargeVarChar = new NullableLargeVarCharHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toLargeVarCharObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<LargeVarCharVector>cast(vector).setSafe(row, ArrowConversion.nullLargeVarChar);
        } else {
            ArrowConversion.<LargeVarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert BOOLEAN to arrow BitVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toBit = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.BooleanType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toBitObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Bit.");
        }
    };
    private static final NullableBitHolder nullBit = new NullableBitHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toBitObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<BitVector>cast(vector).setSafe(row, ArrowConversion.nullBit);
        } else {
            ArrowConversion.<BitVector>cast(vector).setSafe(row, (Boolean) value ? 1 : 0);
        }
    };
    //convert DATE to arrow DateDayVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDateDay = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDateDayObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow DateDay.");
        }
    };
    private static final NullableDateDayHolder nullDateDay = new NullableDateDayHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDateDayObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<DateDayVector>cast(vector).setSafe(row, ArrowConversion.nullDateDay);
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<DateDayVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<DateDayVector>cast(vector).setSafe(row, DateTimeUtils.microsToDays((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert DATE to arrow DateMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDateMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDateMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow DateMilli.");
        }
    };
    private static final NullableDateMilliHolder nullDateMilli = new NullableDateMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDateMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<DateMilliVector>cast(vector).setSafe(row, ArrowConversion.nullDateMilli);
        } else {
            LocalDateTime ldt = (type == DataTypes.DateType) ? LocalDateTime.of(DateTimeUtils.daysToLocalDate((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString())), LocalTime.of(0, 0))
                : (type == DataTypes.TimestampType) ? DateTimeUtils.microsToLocalDateTime((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) : LocalDateTime.now();
            ArrowConversion.<DateMilliVector>cast(vector).setSafe(row, Timestamp.valueOf(ldt).getTime());
        }
    };
    //convert TIME to arrow TimeSecVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeSecObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeSec.");
        }
    };
    private static final NullableTimeSecHolder nullTimeSec = new NullableTimeSecHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeSecObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeSecVector>cast(vector).setSafe(row, ArrowConversion.nullTimeSec);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeSecVector>cast(vector).setSafe(row, (int) (ArrowConversion.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000000000L));
        } else if (type == DataTypes.StringType) {
            ArrowConversion.<TimeSecVector>cast(vector).setSafe(row, (int) (ArrowConversion.timestrToNanos.apply(value.toString()) / 1000000000L));
        }
    };
    //convert TIME to arrow TimeMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeMilli.");
        }
    };
    private static final NullableTimeMilliHolder nullTimeMilli = new NullableTimeMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeMilliVector>cast(vector).setSafe(row, ArrowConversion.nullTimeMilli);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeMilliVector>cast(vector).setSafe(row, (int) (ArrowConversion.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000000L));
        } else if (type == DataTypes.StringType) {
            ArrowConversion.<TimeMilliVector>cast(vector).setSafe(row, (int) (ArrowConversion.timestrToNanos.apply(value.toString()) / 1000000L));
        }
    };
    //convert TIME arrow TimeMicroVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeMicroObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeMicro.");
        }
    };
    private static final NullableTimeMicroHolder nullTimeMicro = new NullableTimeMicroHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeMicroObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeMicroVector>cast(vector).setSafe(row, ArrowConversion.nullTimeMicro);
        } else if (type == DataTypes.TimestampType) {
           ArrowConversion.<TimeMicroVector>cast(vector).setSafe(row, ArrowConversion.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())) / 1000L);
        } else if (type == DataTypes.StringType) {
            ArrowConversion.<TimeMicroVector>cast(vector).setSafe(row, ArrowConversion.timestrToNanos.apply(value.toString()) / 1000L);
        }
    };
    //convert TIME to arrow TimeNanoVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeNanoObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeNano.");
        }
    };
    private static final NullableTimeNanoHolder nullTimeNano = new NullableTimeNanoHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeNanoObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeNanoVector>cast(vector).setSafe(row, ArrowConversion.nullTimeNano);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeNanoVector>cast(vector).setSafe(row, ArrowConversion.microsToEpochNanos.apply((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.StringType) {
            ArrowConversion.<TimeNanoVector>cast(vector).setSafe(row, ArrowConversion.timestrToNanos.apply(value.toString()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMicroObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMicro.");
        }
    };
    private static final NullableTimeStampMicroHolder nullTimeStampMicro = new NullableTimeStampMicroHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMicroObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampMicroVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMicro);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampMicroVector>cast(vector).setSafe(row, ((value instanceof Number) ? (Long) value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampMicroVector>cast(vector).setSafe(row, ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMicroTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMicroTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMicroTZ.");
        }
    };
    private static final NullableTimeStampMicroTZHolder nullTimeStampMicroTZ = new NullableTimeStampMicroTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMicroTZObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMicroTZ);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(ArrowConversion.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(ArrowConversion.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMilliObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMilli.");
        }
    };
    private static final NullableTimeStampMilliHolder nullTimeStampMilli = new NullableTimeStampMilliHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMilliObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampMilliVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMilli);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampMilliVector>cast(vector).setSafe(row, ArrowConversion.microsToMillis.apply(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampMilliVector>cast(vector).setSafe(row, ArrowConversion.microsToMillis.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampMilliTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampMilliTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMilliTZ.");
        }
    };
    private static final NullableTimeStampMilliTZHolder nullTimeStampMilliTZ = new NullableTimeStampMilliTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampMilliTZObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampMilliTZ);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, ArrowConversion.microsToMillis.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(ArrowConversion.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, ArrowConversion.microsToMillis.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(ArrowConversion.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampSecObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampSec.");
        }
    };
    private static final NullableTimeStampSecHolder nullTimeStampSec = new NullableTimeStampSecHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampSecObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampSecVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampSec);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampSecVector>cast(vector).setSafe(row, ArrowConversion.microsToSecs.apply(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampSecVector>cast(vector).setSafe(row, ArrowConversion.microsToSecs.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampSecTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampSecTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampSecTZ.");
        }
    };
    private static final NullableTimeStampSecTZHolder nullTimeStampSecTZ = new NullableTimeStampSecTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampSecTZObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampSecTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampSecTZ);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampSecTZVector>cast(vector).setSafe(row, ArrowConversion.microsToSecs.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(ArrowConversion.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampSecTZVector>cast(vector).setSafe(row, ArrowConversion.microsToSecs.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(ArrowConversion.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampNanoObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampNano.");
        }
    };
    private static final NullableTimeStampNanoHolder nullTimeStampNano = new NullableTimeStampNanoHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampNanoObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampNanoVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampNano);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampNanoVector>cast(vector).setSafe(row, ArrowConversion.microsToNanos.apply(((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            ArrowConversion.<TimeStampNanoVector>cast(vector).setSafe(row, ArrowConversion.microsToNanos.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoTZVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toTimeStampNanoTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toTimeStampNanoTZObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampNanoTZ.");
        }
    };
    private static final NullableTimeStampNanoTZHolder nullTimeStampNanoTZ = new NullableTimeStampNanoTZHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toTimeStampNanoTZObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, ArrowConversion.nullTimeStampNanoTZ);
        } else if (type == DataTypes.TimestampType) {
            ArrowConversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, ArrowConversion.microsToNanos.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long) value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(ArrowConversion.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        } else {
            ArrowConversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, ArrowConversion.microsToNanos.apply(ArrowConversion.daysToMicros.apply((value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()), ZoneId.of(ArrowConversion.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert DURATION_DAY_TIME to arrow DurationVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toDuration = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toDurationObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Duration.");
        }
    };
    private static final NullableDurationHolder nullDuration = new NullableDurationHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toDurationObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<DurationVector>cast(vector).setSafe(row, ArrowConversion.nullDuration);
        } else {
            ArrowConversion.<DurationVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long) value : Long.parseLong(value.toString()));
        }
    };
    //convert PERIOD_YEAR_MONTH to arrow IntervalYearVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toIntervalYear = (vector, rows, idxColumn, type) -> {
        if (type instanceof YearMonthIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntervalYearObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow IntervalYear.");
        }
    };
    private static final NullableIntervalYearHolder nullIntervalYear = new NullableIntervalYearHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntervalYearObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<IntervalYearVector>cast(vector).setSafe(row, ArrowConversion.nullIntervalYear);
        } else {
            ArrowConversion.<IntervalYearVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer) value : Integer.parseInt(value.toString()));
        }
    };
    //convert DURATION_DAY_TIME to arrow IntervalDayVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toIntervalDay = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toIntervalDayObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow IntervalDay.");
        }
    };
    private static final NullableIntervalDayHolder nullIntervalDay = new NullableIntervalDayHolder();
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toIntervalDayObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<IntervalDayVector>cast(vector).setSafe(row, ArrowConversion.nullIntervalDay);
        } else {
            long micros = (value instanceof Number) ? (Long) value : Long.parseLong(value.toString());
            ArrowConversion.<IntervalDayVector>cast(vector).setSafe(row, IntervalUtils.getDays(micros), (int) (micros % 1000L));
        }
    };
    //convert StructType to arrow StructVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toStruct = (vector, rows, idxColumn, type) -> {
        if (vector instanceof StructVector && type instanceof StructType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toStructObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else if (vector instanceof StructVector && type instanceof MapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toStructMap.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Struct.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toStructObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<StructVector>cast(vector).setNull(row);
        } else if (type instanceof StructType) {
            org.apache.arrow.vector.FieldVector[] vectorChildren = ArrowConversion.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            DataType[] dataTypes = Arrays.stream(((StructType) type).fields()).map(StructField::dataType).collect(Collectors.toList()).toArray(new DataType[0]);
            if (vectorChildren.length == dataTypes.length) {
                UnsafeRow rowsChildren = (UnsafeRow) value;
                IntStream.range(0, vectorChildren.length).forEach(idx -> ArrowConversion.getOrCreate().populateObject(vectorChildren[idx], 0, rowsChildren.get(idx, dataTypes[idx]), dataTypes[idx]));
            } else {
                throw new RuntimeException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toStructMap = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<StructVector>cast(vector).setNull(row);
        } else {
            boolean populated = false;
            if (type instanceof MapType) {
                org.apache.arrow.vector.FieldVector[] vectorChildren = ArrowConversion.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                if (vectorChildren.length == 1 && vectorChildren[0] instanceof ListVector && vectorChildren[0].getName().equals("map")) {
                    org.apache.arrow.vector.FieldVector dataVector = ArrowConversion.<ListVector>cast(vectorChildren[0]).getDataVector();
                    if (dataVector instanceof StructVector) {
                        org.apache.arrow.vector.FieldVector[] valueChildren = ArrowConversion.<StructVector>cast(dataVector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                        if (valueChildren.length == 2 && valueChildren[0].getName().equals("key") && valueChildren[1].getName().equals("value")) {
                            DataType keyType = ((MapType) type).keyType();
                            DataType valueType = ((MapType) type).valueType();
                            UnsafeMapData data = (UnsafeMapData) value;
                            UnsafeArrayData keyData = data.keyArray();
                            UnsafeArrayData valueData = data.valueArray();
                            IntStream.range(0, data.numElements()).forEach(idx -> {
                                ArrowConversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                                ArrowConversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                            });
                            populated = true;
                        }
                    }
                }
            }
            if (!populated) {
                throw new RuntimeException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    //convert ArrayType to arrow ListVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toList = (vector, rows, idxColumn, type) -> {
        if (vector instanceof ListVector && type instanceof ArrayType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toListObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow List.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toListObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<ListVector>cast(vector).setNull(row);
        } else if (type instanceof ArrayType) {
            org.apache.arrow.vector.FieldVector dataVector = ArrowConversion.<ListVector>cast(vector).getDataVector();
            DataType dataType = ((ArrayType) type).elementType();
            UnsafeArrayData data = (UnsafeArrayData) value;
            IntStream.range(0, data.numElements()).forEach(idx -> ArrowConversion.getOrCreate().populateObject(dataVector, idx, data.get(idx, dataType), dataType));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow List.");
        }
    };
    //convert MapType to arrow MapVector
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> toMap = (vector, rows, idxColumn, type) -> {
        if (vector instanceof MapVector && type instanceof MapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> ArrowConversion.toMapObject.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Map.");
        }
    };
    private static final ConvertTo<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> toMapObject = (vector, row, value, type) -> {
        if (value == null) {
            ArrowConversion.<MapVector>cast(vector).setNull(row);
        } else if (type instanceof MapType) {
            org.apache.arrow.vector.FieldVector[] valueChildren = ArrowConversion.<MapVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            if (valueChildren.length == 2) {
                DataType keyType = ((MapType) type).keyType();
                DataType valueType = ((MapType) type).valueType();
                UnsafeMapData data = (UnsafeMapData) value;
                UnsafeArrayData keyData = data.keyArray();
                UnsafeArrayData valueData = data.valueArray();
                IntStream.range(0, data.numElements()).forEach(idx -> {
                    ArrowConversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                    ArrowConversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                });
            } else {
                throw new RuntimeException("The data cannot be converted to arrow Map.");
            }
        }
    };

    //base converters
    private static final Function<BigDecimal, Decimal> bigDecimalToDecimal = Decimal::apply;
    private static final Function<String, UTF8String> stringToUtf8String = UTF8String::fromString;
    private static final Function<Integer, Integer> dateDayToInt = (dd) -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(LocalDate.ofEpochDay(dd)));
    private static final Function<LocalDateTime, Integer> localDateTimeToInt = (ldt) -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(ldt.toLocalDate()));
    private static final Function<LocalDateTime, Long> localDateTimeToLong = (ldt) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(ldt));
    private static final BiFunction<Long, String, Long> timestampSecTZToLong = (ss, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochSecond(ss).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> timestampMilliTZToLong = (mss, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(mss).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> timestampNanoTZToLong = (ns, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ns / 1000000L).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> timestampMicroTZToLong = (ms, zone) -> DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ms / 1000L).atZone(ZoneId.of(zone))).plusNanos(ms % 1_000 * 1000L)));
    private static final Function<Timestamp, Long> timestampToLong = DateTimeUtils::fromJavaTimestamp;
    private static final Function<Integer, UTF8String> timeSecToString = (ts) -> {
        int hours = ts / 3600;
        int minutes = (ts - hours * 3600) / 60;
        int seconds = (ts - hours * 3600 - minutes * 60);
        return ArrowConversion.stringToUtf8String.apply(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    };
    private static final Function<LocalDateTime, UTF8String> timeMilliToString = (ldt) -> ArrowConversion.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%03d", ldt.getHour(), ldt.getMinute(), ldt.getSecond(), ldt.getNano() / 1000000L));
    private static final Function<Long, UTF8String> timeMicroToString = (ms) -> {
        int totalSeconds = (int) (ms / 1000000L);
        long microSeconds = ms - totalSeconds * 1000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return ArrowConversion.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, microSeconds));
    };
    private static final Function<Long, UTF8String> timeNanoToString = (ns) -> {
        int totalSeconds = (int) (ns / 1000000000L);
        long nanoSeconds = totalSeconds * 1000000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return ArrowConversion.stringToUtf8String.apply(String.format("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanoSeconds));
    };
    private static final Function<Duration, Long> durationToLong = IntervalUtils::durationToMicros;
    private static final Function<Period, Integer> periodToInt = IntervalUtils::periodToMonths;
    private static final Function<PeriodDuration, InternalRow> translatePeriodDuration = (pd) -> InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(new Object[] { ArrowConversion.periodToInt.apply(pd.getPeriod()), ArrowConversion.durationToLong.apply(pd.getDuration()) })));
    @SuppressWarnings("unchecked")
    private static final ConvertFrom<FieldType, Object, ValueVector, Object> translate = (t, o, v) -> {
        switch (t.getTypeID()) {
            case VARCHAR:
            case CHAR:
                return ArrowConversion.stringToUtf8String.apply(o.toString());
            case TIMESTAMP:
                return (v instanceof TimeStampMilliVector) ? ArrowConversion.timestampToLong.apply((Timestamp) o)
                    : (v instanceof TimeStampMicroTZVector) ? ArrowConversion.timestampMicroTZToLong.apply((Long) o, ((TimeStampMicroTZVector) v).getTimeZone())
                    : (v instanceof TimeStampSecTZVector) ? ArrowConversion.timestampSecTZToLong.apply((Long) o, ((TimeStampSecTZVector) v).getTimeZone())
                    : (v instanceof TimeStampMilliTZVector) ? ArrowConversion.timestampMilliTZToLong.apply((Long) o, ((TimeStampMilliTZVector) v).getTimeZone())
                    : (v instanceof TimeStampNanoTZVector) ? ArrowConversion.timestampNanoTZToLong.apply((Long) o, ((TimeStampNanoTZVector) v).getTimeZone())
                    : (v instanceof TimeStampMicroVector || v instanceof TimeStampSecVector || v instanceof TimeStampNanoVector) ? ArrowConversion.localDateTimeToLong.apply((LocalDateTime) o) : o;
            case TIME:
                return (v instanceof TimeSecVector) ? ArrowConversion.timeSecToString.apply((Integer) o)
                    : (v instanceof TimeMilliVector) ? ArrowConversion.timeMilliToString.apply((LocalDateTime) o)
                    : (v instanceof TimeMicroVector) ? ArrowConversion.timeMicroToString.apply((Long) o)
                    : (v instanceof TimeNanoVector) ? ArrowConversion.timeNanoToString.apply((Long) o) : o;
            case DATE:
                return (v instanceof DateDayVector) ? ArrowConversion.dateDayToInt.apply((Integer) o) : (v instanceof DateMilliVector) ? ArrowConversion.localDateTimeToInt.apply((LocalDateTime) o) : o;
            case DECIMAL:
                return ArrowConversion.bigDecimalToDecimal.apply((BigDecimal) o);
            case DURATION_DAY_TIME:
                return (v instanceof IntervalDayVector || v instanceof DurationVector) ? ArrowConversion.durationToLong.apply((Duration) o) : o;
            case PERIOD_YEAR_MONTH:
                return (v instanceof IntervalYearVector) ? ArrowConversion.periodToInt.apply((Period) o) : o;
            case PERIOD_DURATION_MONTH_DAY_TIME:
                return (v instanceof IntervalMonthDayNanoVector) ? ArrowConversion.translatePeriodDuration.apply((PeriodDuration) o) : o;
            case LIST:
                return (v instanceof ListVector) ? ArrowConversion.translateList.apply((List<?>) o, (ListVector) v, (FieldType.ListType) t) : o;
            case MAP:
                return (v instanceof MapVector) ? ArrowConversion.translateMap.apply((List<?>) o, (MapVector) v, (FieldType.MapType) t) : o;
            case STRUCT:
                return (v instanceof StructVector) ? ArrowConversion.mapElseStruct.apply((Map<String, ?>) o, (StructVector) v, (FieldType.StructType) t) : o;
            case NULL:
                return null;
            default:
                break;
        }
        return o;
    };
    @SuppressWarnings("UstableApiUsage")
    private static final ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, InternalRow> translateStruct = (m, sv, t) -> {
        Map<String, FieldType> mt = t.getChildrenType();
        BiFunction<Map.Entry<String, ?>, Long, Object> setV = (e, i) -> {
            String k = e.getKey();
            Object v = e.getValue();
            return (v != null && mt.containsKey(k)) ? ArrowConversion.translate.apply(mt.get(k), v, sv.getVectorById(i.intValue())) : null;
        };
        List<Object> nm = new java.util.ArrayList<>();
        Streams.mapWithIndex(m.entrySet().stream(), setV::apply).forEach(nm::add);
        return InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(nm.toArray())).toSeq());
    };
    private static final ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, ArrayBasedMapData> structToMap = (m, sv, t) -> {
        Map<String, FieldType> mt = t.getChildrenType();
        if (mt.size() == 1 && mt.containsKey("map") && mt.get("map").getTypeID() == FieldType.IDs.LIST && sv.getVectorById(0) instanceof ListVector) {
            ListVector lv = (ListVector) sv.getVectorById(0);
            FieldType.ListType lt = (FieldType.ListType) mt.get("map");
            if (lt.getChildType().getTypeID() == FieldType.IDs.STRUCT && lv.getDataVector() instanceof StructVector) {
                StructVector dv = (StructVector) lv.getDataVector();
                FieldType.StructType st = (FieldType.StructType) lt.getChildType();
                String[] childKeys = st.getChildrenType().keySet().toArray(new String[0]);
                if (childKeys.length == 2 && childKeys[0].equals("key") && childKeys[1].equals("value")) {
                    List<Map.Entry<String, ?>> kvs = new java.util.ArrayList<>(m.entrySet());
                    if (kvs.size() == 1 && kvs.get(0).getKey().equals("map") && kvs.get(0).getValue() instanceof List<?>) {
                        FieldType kt = st.getChildrenType().get("key");
                        FieldType vt = st.getChildrenType().get("value");
                        List<?> list = (List<?>) kvs.get(0).getValue();
                        List<Object> keys = new java.util.ArrayList<>();
                        List<Object> values = new java.util.ArrayList<>();
                        list.forEach(e -> {
                            Map<?, ?> mes = (Map<?, ?>) e;
                            mes.forEach((key, value) -> {
                                keys.add(ArrowConversion.translate.apply(kt, key, dv.getVectorById(0)));
                                values.add(ArrowConversion.translate.apply(vt, value, dv.getVectorById(1)));
                            });
                        });
                        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
                    }
                }
            }
        }
        return null;
    };
    private static final ConvertFrom<Map<String, ?>, StructVector, FieldType.StructType, Object> mapElseStruct = (m, sv, t) -> ArrowConversion.o1ElseO2.apply(ArrowConversion.structToMap.apply(m, sv, t), ArrowConversion.translateStruct.apply(m, sv, t));
    private static final ConvertFrom<List<?>, MapVector, FieldType.MapType, ArrayBasedMapData> translateMap = (l, mv, mt) -> {
        List<Object> keys = new java.util.ArrayList<>();
        List<Object> values = new java.util.ArrayList<>();
        Function<ValueVector[], Boolean> probe = (vs) -> (vs.length == 2 && vs[0].getField().getName().equalsIgnoreCase("key") && vs[1].getField().getName().equalsIgnoreCase("value"));
        Consumer<ValueVector[]> populate = (vs) -> {
            if (probe.apply(vs)) {
                l.forEach(e -> {
                    JsonStringHashMap<?, ?> entry = (JsonStringHashMap<?, ?>) e;
                    keys.add(ArrowConversion.translate.apply(mt.getKeyType(), entry.get("key"), vs[0]));
                    values.add(ArrowConversion.translate.apply(mt.getValueType(), entry.get("value"), vs[1]));
                });
            }
        };
        ValueVector[] fields = mv.getChildrenFromFields().toArray(new ValueVector[0]);
        populate.accept((fields.length == 1 && fields[0] instanceof StructVector) ? ((StructVector) fields[0]).getChildrenFromFields().toArray(new ValueVector[0]) : fields);
        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
    };
    private static final ConvertFrom<List<?>, ListVector, FieldType.ListType, ArrayData> translateList = (l, v, t) -> ArrayData.toArrayData(l.stream().map(e -> ArrowConversion.translate.apply(t.getChildType(), e, v.getDataVector())).toArray());
    private static final Function<Long, Long> microsToNanos = (micros) -> ArrowConversion.microsToMillis.apply(micros) * 1000L;
    private static final Function<Long, Long> microsToMillis = DateTimeUtils::microsToMillis;
    private static final Function<Long, Long> microsToSecs = (micros) -> ArrowConversion.microsToMillis.apply(micros) / 1000L;
    private static final BiFunction<Integer, ZoneId, Long> daysToMicros = (days, zone) -> DateTimeUtils.daysToMicros(days, ZoneId.systemDefault());
    private static final Function<Long, Long> microsToEpochNanos = (micros) -> {
        Instant t = DateTimeUtils.microsToLocalDateTime(micros).withYear(1970).withMonth(1).withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant();
        return t.toEpochMilli() * 1000000L + (long) t.getNano();
    };
    private static final Function<String, Long> timestrToNanos = (ts) -> {
        Instant t = LocalDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.parse(ts)).atZone(ZoneId.systemDefault()).toInstant();
        return t.toEpochMilli() * 1000000L + (long) t.getNano();
    };
    private static final BiFunction<Object, Object, Object> o1ElseO2 = (o1, o2) -> (o1 != null) ? o1 : o2;

    //the singleton instance
    private static ArrowConversion inst = null;
    /**
     * Get or create an instance of Vector
     * @return - the singleton instance of Vector
     */
    public static synchronized ArrowConversion getOrCreate() {
        if (ArrowConversion.inst == null) {
            ArrowConversion.inst = new ArrowConversion();
        }
        return ArrowConversion.inst;
    }

    private Object readResolve() {
        return getOrCreate();
    }
}
