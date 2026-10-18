package com.taca.shipment.carrier.ghn;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.domain.Carrier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import java.util.List;

/**
 * Explicitly disabled until GHN sandbox API contract and credentials are
 * configured. Never returns fake tracking data.
 */
@Component
public class GhnCarrierAdapter implements CarrierAdapter {
    private final boolean enabled;

    public GhnCarrierAdapter(@Value("${shipment.carrier.ghn.enabled:false}") boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public Carrier carrier() {
        return Carrier.GHN;
    }

    private void requireConfigured() {
        if (!enabled)
            throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE", "GHN adapter is not configured",
                    HttpStatus.SERVICE_UNAVAILABLE);
        throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE",
                "GHN adapter contract is not implemented; configure the verified sandbox API before enabling",
                HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Override
    public ShippingQuote quote(JsonNode from, JsonNode to, List<JsonNode> items) {
        requireConfigured();
        return null;
    }

    @Override
    public CarrierCreateResult create(String key, String orderId, JsonNode request) {
        requireConfigured();
        return null;
    }

    @Override
    public void cancel(String externalId, String reason, String key) {
        requireConfigured();
    }
}
