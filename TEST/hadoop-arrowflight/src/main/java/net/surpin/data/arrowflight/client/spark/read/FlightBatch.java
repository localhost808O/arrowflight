package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Describes a flight-batch
 */
public class FlightBatch implements Batch, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Batch.class);
    private final Configuration configuration;
    private final Table table;

    /**
     * Construct a FligthBatch for a scan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightBatch(Configuration configuration, Table table) {
        LOGGER.info("{}()", this.getClass().getName());
        this.configuration = configuration;
        this.table = table;
    }

    /**
     * Plan the input-partitions
     * @return - the logical partitions
     */
    @Override
    public InputPartition[] planInputPartitions() {
        LOGGER.info("{}.planInputPartitions()", this.getClass().getName());
        String[] partitionQueries = this.table.getPartitionStatements();
        LOGGER.info("{}.planInputPartitions(): partitionQueries = {}", this.getClass().getName(), Arrays.asList(partitionQueries));
        if (partitionQueries.length > 0) {
            LOGGER.info("{}.planInputPartitions(): partitionQueries = {}", this.getClass().getName(), Arrays.asList(partitionQueries));
            return Arrays.stream(partitionQueries).map(q -> new FlightInputPartition.FlightQueryInputPartition(this.table.getSchema(), q)).toArray(InputPartition[]::new);
        } else {
            LOGGER.info("{}.planInputPartitions(): endpoints = {}", this.getClass().getName(), Arrays.asList(this.table.getEndpoints()));
            return Arrays.stream(this.table.getEndpoints()).map(e -> new FlightInputPartition.FlightEndpointInputPartition(this.table.getSchema(), e)).toArray(InputPartition[]::new);
        }
    }

    /**
     * Create a partition reader factory
     * @return - the partition reader factory
     */
    @Override
    public PartitionReaderFactory createReaderFactory() {
        LOGGER.info("{}.createReaderFactory()", this.getClass().getName());
        return new FlightPartitionReaderFactory(this.configuration);
    }
}
