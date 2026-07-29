package net.surpin.data.arrowflight.client.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import scala.Option;
import scala.collection.JavaConverters;

/**
 * Makes persisted Flight tables available through Spark's session catalog.
 */
public final class LegacyFlightSessionCatalog extends DelegatingCatalogExtension {

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier identifier)
            throws NoSuchTableException {
        org.apache.spark.sql.connector.catalog.Table loaded = super.loadTable(identifier);
        if (!(loaded instanceof V1Table table) || !isFlightProvider(table)) {
            return loaded;
        }

        Map<String, String> options =
                new HashMap<>(JavaConverters.mapAsJavaMap(table.options()));
        CaseInsensitiveStringMap sourceOptions = new CaseInsensitiveStringMap(options);
        FlightSource source = new FlightSource();
        StructType schema = source.inferSchema(sourceOptions);
        return source.getTable(schema, new Transform[0], options);
    }

    /**
     * Reports whether a provider name identifies the legacy Flight source.
     *
     * @param provider provider class or short name
     * @return true for the legacy Flight provider
     */
    static boolean isFlightProviderName(String provider) {
        return FlightSource.class.getName().equals(provider)
                || "flight".equalsIgnoreCase(provider);
    }

    /**
     * Reports whether a persisted table uses the legacy Flight provider.
     *
     * @param table persisted Spark table
     * @return true for a legacy Flight table
     */
    private static boolean isFlightProvider(V1Table table) {
        Option<String> provider = table.catalogTable().provider();
        return !provider.isEmpty() && isFlightProviderName(provider.get());
    }
}
