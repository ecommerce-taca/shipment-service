package com.taca.shipment.quote.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.quote.api.CarrierQuotesRequest;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuoteServiceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void comparisonKeepsAvailableQuoteWithUnknownFeeAsNull() throws Exception {
        CarrierAdapter ghn = new CarrierAdapter() {
            @Override
            public Carrier carrier() {
                return Carrier.GHN;
            }

            @Override
            public ShippingQuote quote(com.fasterxml.jackson.databind.JsonNode fromAddress,
                    com.fasterxml.jackson.databind.JsonNode toAddress,
                    List<com.fasterxml.jackson.databind.JsonNode> items) {
                return new ShippingQuote("GHN", true, null, null, 2, 4, Instant.now().plusSeconds(300), null);
            }

            @Override
            public CarrierCreateResult create(String idempotencyKey, String orderId,
                    com.fasterxml.jackson.databind.JsonNode request) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void cancel(String externalShipmentId, String reason, String idempotencyKey) {
                throw new UnsupportedOperationException();
            }
        };
        QuoteService service = new QuoteService(new CarrierRegistry(List.of(ghn)), 30, "test", true);
        CarrierQuotesRequest request = new CarrierQuotesRequest(
                "order-1",
                "shop-1",
                MAPPER.readTree("{\"line1\":\"from\"}"),
                MAPPER.readTree("{\"line1\":\"to\"}"),
                List.of(new ShipmentItemRequest("sku-1", "product-1", "item", 1, 250, null, null, null)));

        Object carriersValue = service.compare(request).get("carriers");
        assertTrue(carriersValue instanceof List<?>);
        Object firstQuote = ((List<?>) carriersValue).get(0);
        assertTrue(firstQuote instanceof Map<?, ?>);
        Map<?, ?> ghnQuote = (Map<?, ?>) firstQuote;

        assertTrue((Boolean) ghnQuote.get("available"));
        assertTrue(ghnQuote.containsKey("fee"));
        assertNull(ghnQuote.get("fee"));
    }
}
