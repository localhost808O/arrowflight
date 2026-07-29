package net.surpin.data.arrowflight.client.spark;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies legacy Flight provider recognition for the Spark catalog bridge.
 */
@Tag("unit")
class LegacyFlightSessionCatalogTest {

    /**
     * Accepts the provider class and short name used by persisted Flight tables.
     */
    @Test
    void acceptsFlightProviderNames() {
        assertTrue(LegacyFlightSessionCatalog.isFlightProviderName("flight"));
        assertTrue(LegacyFlightSessionCatalog.isFlightProviderName("FLIGHT"));
        assertTrue(LegacyFlightSessionCatalog.isFlightProviderName(
                FlightSource.class.getName()));
    }

    /**
     * Rejects unrelated Spark data sources.
     */
    @Test
    void rejectsOtherProviderNames() {
        assertFalse(LegacyFlightSessionCatalog.isFlightProviderName("parquet"));
    }
}
