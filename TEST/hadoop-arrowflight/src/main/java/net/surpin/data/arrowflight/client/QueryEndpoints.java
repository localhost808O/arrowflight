package net.surpin.data.arrowflight.client;

import org.apache.arrow.vector.types.pojo.Schema;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Describes the data-structure from executing a query on remote flight service
 */
public class QueryEndpoints implements Serializable {
    //the schema
    private final transient Schema schema;
    //the collection of end-points exposed for the query
    private final Endpoint[] endpoints;

    /**
     * Construct a QueryEndpoints
     * @param schema - the schema of the query result
     * @param endpoints - end end-points exposed on the remote flight-service for fetching data
     */
    public QueryEndpoints(Schema schema, Endpoint[] endpoints) {
        this.schema = schema;
        this.endpoints = endpoints;
    }

    /**
     * Get the Schema
     * @return - the schema of the QueryEndpoints
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Get the end-points
     * @return - the end-points of the QueryEndpoints
     */
    public Endpoint[] getEndpoints() {
        return this.endpoints;
    }

    @Override
    public String toString() {
        return "QueryEndpoints{" +
                "schema=" + schema +
                ", endpoints=" + Arrays.toString(endpoints) +
                '}';
    }
}
