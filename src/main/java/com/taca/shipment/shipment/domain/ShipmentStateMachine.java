package com.taca.shipment.shipment.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class ShipmentStateMachine {
    private static final Map<ShipmentStatus, Set<ShipmentStatus>> ALLOWED = new EnumMap<>(ShipmentStatus.class);
    static {
        ALLOWED.put(ShipmentStatus.PENDING_RECONCILIATION,
                EnumSet.of(ShipmentStatus.CREATED, ShipmentStatus.CANCELLED, ShipmentStatus.FAILED));
        ALLOWED.put(ShipmentStatus.CREATED, EnumSet.of(ShipmentStatus.PICKED_UP, ShipmentStatus.IN_TRANSIT,
                ShipmentStatus.CANCELLED, ShipmentStatus.FAILED));
        ALLOWED.put(ShipmentStatus.PICKED_UP,
                EnumSet.of(ShipmentStatus.IN_TRANSIT, ShipmentStatus.DELIVERED, ShipmentStatus.FAILED));
        ALLOWED.put(ShipmentStatus.IN_TRANSIT, EnumSet.of(ShipmentStatus.DELIVERED, ShipmentStatus.FAILED));
        ALLOWED.put(ShipmentStatus.FAILED, EnumSet.of(ShipmentStatus.IN_TRANSIT)); // carrier may resume a failed
                                                                                   // delivery
        ALLOWED.put(ShipmentStatus.DELIVERED, EnumSet.noneOf(ShipmentStatus.class));
        ALLOWED.put(ShipmentStatus.CANCELLED, EnumSet.noneOf(ShipmentStatus.class));
    }

    private ShipmentStateMachine() {
    }

    public static boolean canTransition(ShipmentStatus from, ShipmentStatus to) {
        return from == to || ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }
}
