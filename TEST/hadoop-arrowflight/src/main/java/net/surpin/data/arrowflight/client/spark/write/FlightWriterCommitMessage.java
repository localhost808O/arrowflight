package net.surpin.data.arrowflight.client.spark.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;
import java.io.Serializable;

/**
 * Describes the flight writer commit message
 */
public class FlightWriterCommitMessage implements WriterCommitMessage, Serializable {
    private final int partitionId;
    private final long taskId;
    private final String epochId;
    private final long messageCount;

    /**
     * Construct a success Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param messageCount - number of rows been written
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, long messageCount) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = "";
        this.messageCount = messageCount;
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param epochId - the epoch-id for streaming write.
     * @param messageCount - number of rows been written
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, String epochId, long messageCount) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.messageCount = messageCount;
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param message - the base write commit message
     * @param epochId - the epoch-id for streaming write.
     */
    public FlightWriterCommitMessage(FlightWriterCommitMessage message, long epochId) {
        this.partitionId = message.partitionId;
        this.taskId = message.taskId;
        this.epochId = Long.toString(epochId);
        this.messageCount = message.messageCount;
    }

    /**
     * form the committed message
     * @return - the commit message
     */
    public String getMessage() {
        return (this.epochId == null || this.epochId.length() == 0) ? String.format("Streaming write for %d messages with partition (%d), task (%d) committed.", this.messageCount, this.partitionId, this.taskId)
            : String.format("Streaming write for %d messages with partition (%d), task (%d) and epoch (%s) committed.", this.messageCount, this.partitionId, this.taskId, this.epochId);
    }
}
