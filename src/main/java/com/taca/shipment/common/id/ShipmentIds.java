package com.taca.shipment.common.id;

import java.util.UUID;

public final class ShipmentIds {
    private ShipmentIds() {
    }

    public static String format(UUID id) {
        return "shp-" + id;
    }

    public static UUID parse(String value) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("shipmentId is required");
        String raw = value.startsWith("shp-") ? value.substring(4) : value;
        return UUID.fromString(raw);
    }
}
