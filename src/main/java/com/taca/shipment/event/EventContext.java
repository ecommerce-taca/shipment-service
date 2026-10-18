package com.taca.shipment.event;

public record EventContext(String correlationId, String causationId) {
    public static EventContext none() {
        return new EventContext(null, null);
    }
}
