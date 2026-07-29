package net.surpin.data.arrowflight.client.spark.write;

import net.surpin.data.arrowflight.client.ArrowConversion;
import net.surpin.data.arrowflight.client.Client;
import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.WriteProtocol;
import net.surpin.data.arrowflight.client.WriteStatement;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Flight DataWriter writes rows to the target flight table
 */
public final class FlightDataWriter implements DataWriter<InternalRow> {
    private int partitionId;
    private long taskId;
    private String epochId;

    private final StructType dataSchema;
    private final Schema arrowSchema;

    private final WriteStatement stmt;
    private final int batchSize;

    private final Client client;
    private Field[] fields = null;
    private FlightSqlClient.PreparedStatement preparedStmt = null;
    private VectorSchemaRoot root = null;
    private ArrowConversion conversion = null;

    private final java.util.List<InternalRow> rows;
    private long count = 0;

    /**
     * Construct a DataWriter for batch write
     * @param partitionId - the partition id of the data block to be written
     * @param taskId - the task id of the write operation
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     */
    public FlightDataWriter(int partitionId, long taskId, Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this(configuration, stmt, protocol, batchSize);
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = "";
    }

    /**
     * Construct a DataWriter for streaming write
     * @param partitionId - the partition id of the data block to be written
     * @param taskId - the task id of the write operation
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     * @param epochId - a monotonically increasing id for streaming queries that are split into discrete periods of execution.
     */
    public FlightDataWriter(int partitionId, long taskId, long epochId, Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this(configuration, stmt, protocol, batchSize);
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = Long.toString(epochId);
    }

    /**
     * Internal Constructor
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     */
    private FlightDataWriter(Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this.stmt = stmt;
        this.batchSize = batchSize;

        this.dataSchema = this.stmt.getDataSchema();
        this.client = Client.getOrCreate(configuration);
        if (protocol == WriteProtocol.PREPARED_SQL) {
            this.preparedStmt = this.client.getPreparedStatement(this.stmt.getStatement());
            this.arrowSchema = this.preparedStmt.getParameterSchema();
            this.fields = this.arrowSchema.getFields().toArray(new Field[0]);
            this.root = VectorSchemaRoot.create(this.arrowSchema, new RootAllocator(Integer.MAX_VALUE));
            this.conversion = ArrowConversion.getOrCreate();
        } else {
            try {
                this.arrowSchema = this.stmt.getArrowSchema();
            } catch (Exception e) {
                throw new RuntimeException("The arrow schema is invalid.", e);
            }
        }
        this.rows = new java.util.ArrayList<>();
    }

    /**
     * Write one row
     * @param row - the row of data
     * @throws IOException - thrown when writing failed
     */
    @Override
    public void write(InternalRow row) throws IOException {
        this.rows.add(row.copy());
        if (this.rows.size() > this.batchSize) {
            this.write(this.rows.toArray(new InternalRow[0]));
            this.rows.clear();
        }
    }
    /**
     * Write out all rows
     * @param rows - the data rows
     */
    private void write(InternalRow[] rows) {
        if (this.conversion != null) {
            Function<String, DataType> dtFind = (name) -> this.dataSchema.find(field -> field.name().equalsIgnoreCase(name)).map(StructField::dataType).get();
            IntStream.range(0, this.fields.length).forEach(idx -> this.conversion.populate(this.root.getVector(idx), rows, idx, dtFind.apply(this.fields[idx].getName())));
            this.root.setRowCount(rows.length);
            this.preparedStmt.setParameters(this.root);
            try {
                this.client.executeUpdate(this.preparedStmt);
            } finally {
                this.preparedStmt.clearParameters();
                this.root.clear();
            }
        } else {
            this.client.execute(this.stmt.fillStatement(rows, this.arrowSchema.getFields().toArray(new Field[0])));
        }
        this.count += rows.length;
    }

    /**
     * Commit write
     * @return - a commit-message
     */
    @Override
    public WriterCommitMessage commit() {
        //write any left-over
        if (this.rows.size() > 0) {
            this.write(this.rows.toArray(new InternalRow[0]));
            this.rows.clear();
        }

        long cnt = this.count;
        this.count = 0;
        return (this.epochId.length() == 0) ? new FlightWriterCommitMessage(this.partitionId, this.taskId, cnt)
            : new FlightWriterCommitMessage(this.partitionId, this.taskId, this.epochId, cnt);
    }

    /**
     * Abort write
     * @throws IOException - the exception with the error message
     */
    @Override
    public void abort() throws IOException {
        throw (this.epochId.length() == 0) ? new FlightWriteAbortException(this.partitionId, this.taskId, this.count)
            : new FlightWriteAbortException(this.partitionId, this.taskId, this.epochId, this.count);
    }

    /**
     * Close any connections
     */
    @Override
    public void close() {
        if (this.preparedStmt != null) {
            this.preparedStmt.close();
        }
        if (this.root != null) {
            this.root.close();
        }
    }
}
