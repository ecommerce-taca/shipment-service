package com.taca.shipment.carrier.ghn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.common.error.ShipmentException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GhnCarrierAdapterTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void disabledAdapterFailsClosedForQuoteCreateAndCancel() throws Exception {
        GhnCarrierAdapter adapter = new GhnCarrierAdapter(false);

        ShipmentException quoteError = assertThrows(ShipmentException.class,
                () -> adapter.quote(MAPPER.readTree("{\"line1\":\"from\"}"),
                        MAPPER.readTree("{\"line1\":\"to\"}"), List.of()));
        ShipmentException createError = assertThrows(ShipmentException.class,
                () -> adapter.create("idempotency-key", "order-1", MAPPER.createObjectNode()));
        ShipmentException cancelError = assertThrows(ShipmentException.class,
                () -> adapter.cancel("external-1", "reason", "cancel-key"));

        for (ShipmentException error : List.of(quoteError, createError, cancelError)) {
            assertEquals("SHIPMENT_CARRIER_UNAVAILABLE", error.code());
            assertEquals(HttpStatus.SERVICE_UNAVAILABLE, error.status());
        }
    }

    @Test
    void enablingAdapterDoesNotClaimIntegrationBeforeContractIsImplemented() throws Exception {
        GhnCarrierAdapter adapter = new GhnCarrierAdapter(true);

        ShipmentException error = assertThrows(ShipmentException.class,
                () -> adapter.quote(MAPPER.readTree("{\"line1\":\"from\"}"),
                        MAPPER.readTree("{\"line1\":\"to\"}"), List.of()));

        assertEquals("SHIPMENT_CARRIER_UNAVAILABLE", error.code());
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, error.status());
    }
}
