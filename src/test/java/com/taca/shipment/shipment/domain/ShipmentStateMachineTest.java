package com.taca.shipment.shipment.domain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ShipmentStateMachineTest {
    @Test
    void allowsForwardProgression() {
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.CREATED, ShipmentStatus.PICKED_UP));
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.PICKED_UP, ShipmentStatus.IN_TRANSIT));
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.IN_TRANSIT, ShipmentStatus.DELIVERED));
    }

    @Test
    void rejectsRegressionAndCancellationAfterPickup() {
        assertFalse(ShipmentStateMachine.canTransition(ShipmentStatus.IN_TRANSIT, ShipmentStatus.CREATED));
        assertFalse(ShipmentStateMachine.canTransition(ShipmentStatus.PICKED_UP, ShipmentStatus.CANCELLED));
        assertFalse(ShipmentStateMachine.canTransition(ShipmentStatus.DELIVERED, ShipmentStatus.IN_TRANSIT));
    }

    @Test
    void allowsCancellationBeforePickupAndCarrierResumptionAfterFailure() {
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.CREATED, ShipmentStatus.CANCELLED));
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.FAILED, ShipmentStatus.IN_TRANSIT));
        assertFalse(ShipmentStateMachine.canTransition(ShipmentStatus.FAILED, ShipmentStatus.DELIVERED));
    }

    @Test
    void reconciliationCanResolveOnlyToKnownOutcome() {
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.PENDING_RECONCILIATION, ShipmentStatus.CREATED));
        assertTrue(ShipmentStateMachine.canTransition(ShipmentStatus.PENDING_RECONCILIATION, ShipmentStatus.CANCELLED));
    }
}
