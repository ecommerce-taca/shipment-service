package com.taca.shipment.shipment.domain;

public enum ShipmentStatus {
    CREATED, PICKED_UP, IN_TRANSIT, DELIVERED, FAILED, CANCELLED, PENDING_RECONCILIATION;

    public boolean canCancel() {
        return this == CREATED;
    }

    public boolean isTerminal() {
        return this == DELIVERED || this == CANCELLED;
    }
}
