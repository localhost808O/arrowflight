package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The flight partition-reader factory for creating flight partition-readers
 */
public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightPartitionReaderFactory.class);

    private final Configuration configuration;

    /**
     * Construct a flight partition-reader factory
     * @param configuration - the configuration of remote flight service
     */
    public FlightPartitionReaderFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Create a reader
     * @param inputPartition - the input-partition for the reader
     * @return - a partition-reader
     */
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        LOGGER.info("{}.createReader()", this.getClass().getName());
        return new FlightPartitionReader(this.configuration, inputPartition);
    }
}
