package com.taca.shipment.carrier.domain;

public record CarrierCreateResult(String externalShipmentId, String trackingCode) {
    public CarrierCreateResult {
        if (externalShipmentId == null || externalShipmentId.isBlank() || externalShipmentId.length() > 128)
            throw new IllegalArgumentException("Carrier external shipment ID is missing or exceeds 128 characters");
        if (trackingCode == null || trackingCode.isBlank() || trackingCode.length() > 64)
            throw new IllegalArgumentException("Carrier tracking code is missing or exceeds 64 characters");
    }
}
