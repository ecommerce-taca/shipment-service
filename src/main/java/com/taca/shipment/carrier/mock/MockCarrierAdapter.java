package com.taca.shipment.carrier.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.shipment.domain.Carrier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "shipment.carrier.mock-enabled", havingValue = "true", matchIfMissing = true)
public class MockCarrierAdapter implements CarrierAdapter {
    private final ConcurrentHashMap<String, CarrierCreateResult> byKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> cancelled = new ConcurrentHashMap<>();

    @Override
    public Carrier carrier() {
        return Carrier.MOCK;
    }

    @Override
    public ShippingQuote quote(JsonNode from, JsonNode to, List<JsonNode> items) {
        int grams = 0;
        for (JsonNode item : items)
            grams += Math.max(0, item.path("weight_gram").asInt(item.path("weight_grams").asInt(0)))
                    * Math.max(1, item.path("quantity").asInt(1));
        long fee = 20000L + (grams / 1000L) * 3000L;
        return new ShippingQuote("MOCK", true, fee, "VND", 1, 3, Instant.now().plusSeconds(1800), null);
    }

    @Override
    public CarrierCreateResult create(String key, String orderId, JsonNode request) {
        return byKey.computeIfAbsent(key, k -> new CarrierCreateResult("mock-ext-" + safe(orderId),
                "MOCK" + Long.toUnsignedString(System.nanoTime(), 36).toUpperCase()));
    }

    @Override
    public void cancel(String externalId, String reason, String key) {
        cancelled.putIfAbsent(key, true);
    }

    @Override
    public Optional<CarrierCreateResult> findByIdempotencyKey(String key) {
        return Optional.ofNullable(byKey.get(key));
    }

    private String safe(String s) {
        return s.replaceAll("[^A-Za-z0-9]", "");
    }
}
