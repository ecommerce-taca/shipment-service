package com.taca.shipment.quote.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.quote.api.CarrierQuotesRequest;
import com.taca.shipment.quote.api.QuoteRequest;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void quoteAddsConfiguredExpiryForNonMockCarrier() throws Exception {
        CarrierAdapter ghn = new CarrierAdapter() {
            @Override
            public Carrier carrier() {
                return Carrier.GHN;
            }

            @Override
            public ShippingQuote quote(com.fasterxml.jackson.databind.JsonNode fromAddress,
                    com.fasterxml.jackson.databind.JsonNode toAddress,
                    List<com.fasterxml.jackson.databind.JsonNode> items) {
                return new ShippingQuote("GHN", true, 25000L, "VND", 2, 4, null, null);
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
        Instant before = Instant.now();
        QuoteRequest request = new QuoteRequest(
                "shop-1",
                MAPPER.readTree("{\"line1\":\"from\"}"),
                MAPPER.readTree("{\"line1\":\"to\"}"),
                List.of(new ShipmentItemRequest("sku-1", "product-1", "item", 1, 250, null, null, null)),
                "GHN");

        ShippingQuote result = service.quote(request);
        Instant after = Instant.now();

        assertEquals(25000L, result.fee());
        assertEquals("VND", result.currency());
        assertTrue(result.expiresAt().isAfter(before.plusSeconds(29 * 60L)));
        assertTrue(result.expiresAt().isBefore(after.plusSeconds(31 * 60L)));
    }

    @Test
    void rejectsNonObjectOrOversizedAddressSnapshots() throws Exception {
        QuoteService service = new QuoteService(new CarrierRegistry(List.of()), 30, "test", true);
        ShipmentItemRequest item = new ShipmentItemRequest(
                "sku-1", "product-1", "item", 1, 250, null, null, null);
        JsonNode validAddress = MAPPER.readTree("{\"line1\":\"address\"}");

        ShipmentException nonObject = assertThrows(ShipmentException.class,
                () -> service.quote(new QuoteRequest(
                        "shop-1", MAPPER.readTree("[]"), validAddress, List.of(item), "GHN")));
        assertEquals("SHIPMENT_INVALID_INPUT", nonObject.code());
        assertEquals(HttpStatus.BAD_REQUEST, nonObject.status());

        JsonNode oversized = MAPPER.getNodeFactory().textNode("x".repeat(16_384));
        ShipmentException tooLarge = assertThrows(ShipmentException.class,
                () -> service.quote(new QuoteRequest(
                        "shop-1", oversized, validAddress, List.of(item), "GHN")));
        assertEquals("SHIPMENT_INVALID_INPUT", tooLarge.code());
        assertEquals(HttpStatus.BAD_REQUEST, tooLarge.status());
    }
}
