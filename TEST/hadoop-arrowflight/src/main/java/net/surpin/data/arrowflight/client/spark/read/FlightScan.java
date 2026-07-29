package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Describes the data-structure of FlightScan
 */
public class FlightScan implements Scan, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScan.class);

    private final Configuration configuration;
    private final Table table;

    /**
     * Construct a FligthScan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightScan(Configuration configuration, Table table) {
        LOGGER.info("{}()", this.getClass().getName());

        this.configuration = configuration;
        this.table = table;
    }

    /**
     * Get the schema of the scan
     * @return - the scan for the scan
     */
    @Override
    public StructType readSchema() {
        return this.table.getSparkSchema();
    }

    /**
     * The description of the scan
     * @return - description
     */
    @Override
    public String description() {
        return this.table.getQueryStatement();
    }

    /**
     * Translate the scan to batch
     * @return - the batch desribes the scan
     */
    @Override
    public Batch toBatch() {
        LOGGER.info("{}.toBatch()", this.getClass().getName());
        return new FlightBatch(this.configuration, this.table);
    }
}
