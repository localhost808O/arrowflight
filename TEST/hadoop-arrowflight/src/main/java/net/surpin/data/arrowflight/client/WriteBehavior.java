package net.surpin.data.arrowflight.client;

import java.io.Serializable;
import java.util.Map;

/**
 * Defines the write-behavior
 */
public class WriteBehavior implements Serializable {
    private final WriteProtocol protocol;
    private final Map<String, String> typeMapping;

    private final int batchSize;
    private final String[] mergeByColumns;

    //the truncate flag
    private Boolean truncate = false;

    /**
     * Construct a WriteBehavior
     * @param protocol - the protocol for submitting DML requests. It must be either literal-sql or prepared-sql
     * @param batchSize - the size of each batch to be written
     * @param mergeByColumn - the columns on which to merge data into the target table
     * @param typeMapping - the arrow-type to target data-type mapping
     */
    public WriteBehavior(WriteProtocol protocol, int batchSize, String[] mergeByColumn, Map<String, String> typeMapping) {
        this.protocol = protocol;
        this.batchSize = batchSize;
        this.mergeByColumns = mergeByColumn;
        this.typeMapping = typeMapping;
    }

    /**
     * Get the write-procotol
     * @return - the protocol for writing
     */
    public WriteProtocol getProtocol() {
        return this.protocol;
    }

    /**
     * Get the size of each batch
     * @return - the size of batch for writing
     */
    public int getBatchSize() {
        return this.batchSize;
    }

    /**
     * Get the merge-by columns
     * @return - the columns on which to merge data into the target table
     */
    public String[] getMergeByColumns() {
        return isTruncate() ? new String[0] : this.mergeByColumns;
    }

    /**
     * Get the type-mapping
     * @return - the mapping between arrow-type & target data-types
     */
    public Map<String, String> getTypeMapping() {
        return this.typeMapping;
    }

    /**
     * set the flag to truncate the target table
     */
    public void truncate() {
        if (this.mergeByColumns != null && this.mergeByColumns.length > 0) {
            throw new RuntimeException("The merge-by can only work with append mode.");
        }
        this.truncate = true;
    }

    /**
     * Flag to truncate the target table
     * @return - true if it is to truncate the target table
     */
    public Boolean isTruncate() {
        return this.truncate;
    }
}
