package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.*;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming partition reader that reads data from remote Flight service
 * without accumulating all rows in memory.
 *
 * Each batch from FlightStream is consumed row-by-row and converted to Spark InternalRow
 * on the fly. Only one batch is held in memory at a time.
 */
public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReader.class);

    private final Client client;
    private final Configuration configuration;
    private final InputPartition inputPartition;

    // current FlightStream and its VectorSchemaRoot
    private FlightStream stream;
    private VectorSchemaRoot root;
    private Field[] fields;

    // Schema from FlightInfo (what Spark's codegen was compiled against).
    // Kept separately because `fields` is overwritten per-batch with Acero's stream schema,
    // which may differ in integer width (e.g. Acero returns Int8, FlightInfo said Int32).
    // getValue() uses sparkFields to return the exact Java type Spark's getInt()/getByte() expects.
    private Field[] sparkFields;

    // current row position within the current batch
    private int rowIdx = 0;
    private int batchRowCount = 0;

    // whether we have a valid current row
    private boolean hasCurrent = false;

    /**
     * Construct a streaming partition reader
     */
    public FlightPartitionReader(Configuration configuration, InputPartition inputPartition) {
        this.configuration = configuration;
        this.inputPartition = inputPartition;
        this.client = Client.getOrCreate(configuration);
    }

    @Override
    public boolean next() throws IOException {
        LOGGER.info("FlightPartitionReader.next()");
        try {
            if (stream == null) {
                if (!openStream()) {
                    LOGGER.info("FlightPartitionReader.next(): openStream() returned false");
                    return false;
                }
            }

            LOGGER.info("FlightPartitionReader.next(): rowIdx = {}, batchRowCount = {}", rowIdx, batchRowCount);
            // move to next row in current batch
            rowIdx++;
            if (rowIdx < batchRowCount) {
                hasCurrent = true;
                return true;
            }

            LOGGER.info("FlightPartitionReader.next(): try to read next batch");
            // current batch exhausted, try next batch
            while (stream.next()) {
                root = stream.getRoot();
                batchRowCount = root.getRowCount();
                fields = Field.from(stream.getSchema());
                rowIdx = 0;
                LOGGER.info("FlightPartitionReader.next(): started new batch rowIdx = {}, batchRowCount = {}", rowIdx, batchRowCount);

                if (batchRowCount > 0) {
                    hasCurrent = true;
                    return true;
                }
            }

            LOGGER.info("FlightPartitionReader.next(): no more batches");
            // no more batches
            hasCurrent = false;
            closeStream();
            return false;

        } catch (Exception e) {
            LOGGER.error("Error reading from Flight stream: " + e.getMessage(), e);
            closeStream();
            throw new IOException(e);
        }
    }

    @Override
    public InternalRow get() {
        LOGGER.info("FlightPartitionReader.get():");

        if (!hasCurrent || root == null) {
            throw new IllegalStateException("No current row. Call next() first.");
        }

        List<org.apache.arrow.vector.FieldVector> fieldVectors = root.getFieldVectors();
        int numCols = fieldVectors.size();
        Object[] values = new Object[numCols];

        Field[] expectedFields = (sparkFields != null && sparkFields.length == numCols) ? sparkFields : fields;
        for (int col = 0; col < numCols; col++) {
            values[col] = getValue(fieldVectors.get(col), rowIdx, expectedFields[col]);
        }

        return new GenericInternalRow(values);
    }

    @Override
    public void close() {
        closeStream();
    }

    private boolean openStream() {
        try {
            if (this.inputPartition instanceof FlightInputPartition.FlightEndpointInputPartition) {
                return openEndpointStream((FlightInputPartition.FlightEndpointInputPartition) this.inputPartition);
            } else if (this.inputPartition instanceof FlightInputPartition.FlightQueryInputPartition) {
                return openQueryStream((FlightInputPartition.FlightQueryInputPartition) this.inputPartition);
            } else {
                LOGGER.error("FlightPartitionReader.openStream(): Unsupported partition, no data read: {}", this.inputPartition);
            }
            return false;
        } catch (Exception e) {
            LOGGER.error("Failed to open Flight stream: " + e.getMessage(), e);
            return false;
        }
    }

    private boolean openEndpointStream(FlightInputPartition.FlightEndpointInputPartition dePartition) throws Exception {
        LOGGER.info("FlightPartitionReader.openEndpointStream(): partition: {}", dePartition);

        Endpoint endpoint = dePartition.getEndpoint();
        Schema schema = dePartition.getSchema();
        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        LOGGER.info("FlightPartitionReader.openEndpointStream(): endpoint: {}", fep);
        this.stream = this.client.getFlightClient().getStream(fep.getTicket(), this.client.getBearerToken());
        LOGGER.info("FlightPartitionReader.openEndpointStream(): stream: {}", stream);
        this.root = stream.getRoot();
        LOGGER.info("FlightPartitionReader.openEndpointStream(): vector schema root: {}", root);
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;  // FlightInfo schema = what Spark's codegen expects
        LOGGER.info("FlightPartitionReader.openEndpointStream(): fields: {}", fields);
        this.rowIdx = -1;
        this.batchRowCount = 0;

        // try first batch
        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }

        LOGGER.info("FlightPartitionReader.openEndpointStream(): first batch row count: {}", batchRowCount);
        return true;
    }

    private boolean openQueryStream(FlightInputPartition.FlightQueryInputPartition dqPartition) throws Exception {
        LOGGER.error("FlightPartitionReader.openQueryStream(): partition: {}", dqPartition);
        QueryEndpoints qeps = this.client.getQueryEndpoints(dqPartition.getQuery());
        LOGGER.info("FlightPartitionReader.openQueryStream(): endpoints: {}", qeps);
        if (qeps.getEndpoints().length == 0) {
            return false;
        }

        // Use first endpoint for now (multi-endpoint streaming is handled by Spark partitioning)
        Endpoint endpoint = qeps.getEndpoints()[0];
        Schema schema = qeps.getSchema();
        org.apache.arrow.flight.FlightEndpoint fep = new org.apache.arrow.flight.FlightEndpoint(
                new org.apache.arrow.flight.Ticket(endpoint.getTicket()),
                Arrays.stream(endpoint.getURIs())
                        .map(org.apache.arrow.flight.Location::new)
                        .toArray(org.apache.arrow.flight.Location[]::new)
        );

        this.stream = this.client.getFlightClient().getStream(fep.getTicket(), this.client.getBearerToken());
        this.root = stream.getRoot();
        this.fields = Field.from(schema);
        this.sparkFields = this.fields;  // FlightInfo schema = what Spark's codegen expects
        this.rowIdx = -1;
        this.batchRowCount = 0;

        if (stream.next()) {
            batchRowCount = root.getRowCount();
        }
        return batchRowCount > 0;
    }

    private void closeStream() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).warn("Error closing Flight stream: " + e.getMessage());
        } finally {
            stream = null;
            root = null;
            fields = null;
            sparkFields = null;
            batchRowCount = 0;
            rowIdx = 0;
            hasCurrent = false;
        }
    }

    /**
     * Extract a single value from a FieldVector at the given row index,
     * converting it to a Spark-compatible object.
     */
    private static Object getValue(org.apache.arrow.vector.FieldVector vector, int rowIndex, Field field) {
        if (vector.isNull(rowIndex)) {
            return null;
        }

        // Int-based vectors — coerce to the Java type Spark's codegen expects.
        // The `field` parameter carries the FlightInfo schema type (what Spark compiled against).
        // Acero may return a narrower vector (e.g. TinyIntVector for a column the old server
        // reported as Int32); without coercion Spark's getInt() would ClassCastException on Byte.
        if (vector instanceof TinyIntVector) {
            byte b = ((TinyIntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) b;
                    case SHORT: return (short) b;
                    case LONG: case BIGINT: return (long) b;
                    default: break;
                }
            }
            return b;
        }
        if (vector instanceof SmallIntVector) {
            short s = ((SmallIntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case INT:   return (int) s;
                    case LONG: case BIGINT: return (long) s;
                    default: break;
                }
            }
            return s;
        }
        if (vector instanceof IntVector) {
            int i = ((IntVector) vector).get(rowIndex);
            if (field != null) {
                switch (field.getType().getTypeID()) {
                    case LONG: case BIGINT: return (long) i;
                    case SHORT: return (short) i;
                    case BYTE:  return (byte) i;
                    default: break;
                }
            }
            return i;
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(rowIndex);
        }

        // Float-based
        if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(rowIndex);
        }
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(rowIndex);
        }

        // Decimal
        if (vector instanceof DecimalVector) {
            BigDecimal bd = ((DecimalVector) vector).getObject(rowIndex);
            return bd != null ? bd.toPlainString() : null; // Spark expects string for decimal
        }
        if (vector instanceof Decimal256Vector) {
            BigDecimal bd = ((Decimal256Vector) vector).getObject(rowIndex);
            return bd != null ? bd.toPlainString() : null;
        }

        // String
        if (vector instanceof VarCharVector) {
            return UTF8String.fromString(
                    ((VarCharVector) vector).getObject(rowIndex).toString()
            );
        }
        if (vector instanceof LargeVarCharVector) {
            return UTF8String.fromString(
                    ((LargeVarCharVector) vector).getObject(rowIndex).toString()
            );
        }

        // Boolean
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(rowIndex) != 0;
        }

        // Date
        if (vector instanceof DateDayVector) {
            // Epoch days
            return ((DateDayVector) vector).get(rowIndex);
        }
        if (vector instanceof DateMilliVector) {
            return ((DateMilliVector) vector).get(rowIndex);
        }

        // Time
        if (vector instanceof TimeSecVector) {
            return ((TimeSecVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMilliVector) {
            return ((TimeMilliVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeMicroVector) {
            return ((TimeMicroVector) vector).getObject(rowIndex).toString();
        }
        if (vector instanceof TimeNanoVector) {
            return ((TimeNanoVector) vector).getObject(rowIndex).toString();
        }

        // Timestamp
        if (vector instanceof TimeStampMilliVector || vector instanceof TimeStampMilliTZVector
                || vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector
                || vector instanceof TimeStampNanoVector || vector instanceof TimeStampNanoTZVector
                || vector instanceof TimeStampSecVector || vector instanceof TimeStampSecTZVector) {
            Object someTimestamp = vector.getObject(rowIndex);
            if(someTimestamp instanceof Timestamp) {
                return ((Timestamp) vector.getObject(rowIndex)).getTime() / 1000; // Spark expects seconds
            } else if(someTimestamp instanceof LocalDateTime) {
                return ((LocalDateTime) vector.getObject(rowIndex)).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000; // Spark expects seconds
            } else {
                throw new UnsupportedOperationException("Unsupported timestamp format: " + someTimestamp.getClass());
            }

        }

        // Binary
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).getObject(rowIndex);
        }
        if (vector instanceof LargeVarBinaryVector) {
            return ((LargeVarBinaryVector) vector).getObject(rowIndex);
        }
        if (vector instanceof FixedSizeBinaryVector) {
            return ((FixedSizeBinaryVector) vector).getObject(rowIndex);
        }

        // UInt
        if (vector instanceof UInt1Vector) {
            return (int) ((UInt1Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt2Vector) {
            return (int) ((UInt2Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt4Vector) {
            return (int) ((UInt4Vector) vector).get(rowIndex);
        }
        if (vector instanceof UInt8Vector) {
            return ((UInt8Vector) vector).get(rowIndex);
        }

        // Duration, Interval
        if (vector instanceof DurationVector) {
            return ((DurationVector) vector).getObject(rowIndex);
        }
        if (vector instanceof IntervalYearVector) {
            return ((IntervalYearVector) vector).get(rowIndex);
        }
        if (vector instanceof IntervalDayVector) {
            return ((IntervalDayVector) vector).getObject(rowIndex);
        }

        // Null
        if (vector instanceof NullVector) {
            return null;
        }

        // Default fallback — getObject
        return vector.getObject(rowIndex);
    }
}
