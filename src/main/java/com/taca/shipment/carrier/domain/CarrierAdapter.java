package com.taca.shipment.carrier.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.shipment.domain.Carrier;
import java.util.List;
import java.util.Optional;

public interface CarrierAdapter {
    Carrier carrier();

    ShippingQuote quote(JsonNode fromAddress, JsonNode toAddress, List<JsonNode> items);

    CarrierCreateResult create(String idempotencyKey, String orderId, JsonNode request);

    void cancel(String externalShipmentId, String reason, String idempotencyKey);

    default Optional<CarrierCreateResult> findByIdempotencyKey(String key) {
        return Optional.empty();
    }
}
