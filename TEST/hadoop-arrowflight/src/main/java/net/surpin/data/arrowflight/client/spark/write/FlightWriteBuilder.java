package net.surpin.data.arrowflight.client.spark.write;

import net.surpin.data.arrowflight.client.Configuration;
import net.surpin.data.arrowflight.client.Table;
import net.surpin.data.arrowflight.client.WriteBehavior;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * The flight write builder to build flight writers
 */
public class FlightWriteBuilder implements WriteBuilder, SupportsTruncate {
    private final Configuration configuration;
    private final Table table;
    private final StructType dataSchema;
    private final WriteBehavior writeBehavior;

    /**
     * Construct a builder for creating flight writers
     * @param configuration - the configuraton of remote flight service
     * @param table - the table object for describing the target flight table
     * @param dataSchema - the schema of the data being written
     * @param writeBehavior - the write-behavior
     */
    public FlightWriteBuilder(Configuration configuration, Table table, StructType dataSchema, WriteBehavior writeBehavior) {
        this.configuration = configuration;
        this.table = table;
        this.dataSchema = dataSchema;
        this.writeBehavior = writeBehavior;
    }

    @Override
    public Write build() {
        return new FlightWrite(this.configuration, this.table, this.dataSchema, this.writeBehavior);
    }

    /**
     * flag to truncate the target table
     * @return - the write-build which truncates the target table
     */
    @Override
    public WriteBuilder truncate() {
        this.writeBehavior.truncate();
        return this;
    }
}
