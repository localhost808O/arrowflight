package net.surpin.data.arrowflight.client.spark.read;

import net.surpin.data.arrowflight.client.Endpoint;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Describes the data-structure of flight input-partitions
 */
public class FlightInputPartition implements InputPartition, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightInputPartition.class);

    /**
     * The input-partition with end-points
     */
    public static class FlightEndpointInputPartition extends FlightInputPartition {
        private final Endpoint ep;

        /**
         * Construct a flight end-point input-partition
         * @param schema - the schema of the partition
         * @param ep - the end-point of the partition
         */
        public FlightEndpointInputPartition(Schema schema, Endpoint ep) {
            super(schema);
            this.ep = ep;
        }

        /**
         * Get the end-point of the input-partition
         * @return - the end-point of the input partition
         */
        public Endpoint getEndpoint() {
            return this.ep;
        }
    }

    /**
     * The input-partition with query
     */
    public static class FlightQueryInputPartition extends FlightInputPartition {
        private final String query;

        /**
         * Construct a flight query input-partition
         * @param schema - the schema of the partition
         * @param query - the query for the partition
         */
        public FlightQueryInputPartition(Schema schema, String query) {
            super(schema);
            this.query = query;
        }

        /**
         * Get the query of the input-partition
         * @return - the query of the input-partition
         */
        public String getQuery() {
            return this.query;
        }
    }

    //the schema of the input partition
    private final String schema;

    /**
     * Construct a flight-input-partition
     * @param schema - the schema of the partition
     */
    protected FlightInputPartition(Schema schema) {
        this.schema = schema.toJson();
    }

    /**
     * Get the schema of the partition
     * @return - the schema of the partition
     * @throws IOException - thrown when the schema is in invalid json format.
     */
    public Schema getSchema() throws IOException {
        return Schema.fromJSON(this.schema);
    }
}
