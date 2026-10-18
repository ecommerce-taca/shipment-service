package com.taca.shipment.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutboxEventEntityTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void retriesPrimaryTopicBeforeSchedulingDeadLetterDelivery() {
        OutboxEventEntity event = newEvent();

        event.markFailed("KAFKA_PUBLISH_FAILED");
        assertEquals("PENDING", event.getStatus());
        assertEquals(1, event.getAttempts());

        event.markFailed("KAFKA_PUBLISH_FAILED");
        assertEquals("PENDING", event.getStatus());
        assertEquals(2, event.getAttempts());

        event.markFailed("KAFKA_PUBLISH_FAILED");
        assertEquals("DLQ_PENDING", event.getStatus());
        assertEquals(0, event.getAttempts());
    }

    @Test
    void retriesDeadLetterDeliveryAndBecomesTerminalAfterLimit() {
        OutboxEventEntity event = newEvent();
        event.markFailed("KAFKA_PUBLISH_FAILED");
        event.markFailed("KAFKA_PUBLISH_FAILED");
        event.markFailed("KAFKA_PUBLISH_FAILED");

        event.markDeadLetterFailed();
        assertEquals("DLQ_PENDING", event.getStatus());
        assertEquals(1, event.getAttempts());

        event.markDeadLetterFailed();
        assertEquals("DLQ_PENDING", event.getStatus());
        assertEquals(2, event.getAttempts());

        event.markDeadLetterFailed();
        assertEquals("DEAD", event.getStatus());
        assertEquals(3, event.getAttempts());
    }

    @Test
    void marksDeadLetterDeliveryAsTerminal() {
        OutboxEventEntity event = newEvent();
        event.markFailed("KAFKA_PUBLISH_FAILED");
        event.markFailed("KAFKA_PUBLISH_FAILED");
        event.markFailed("KAFKA_PUBLISH_FAILED");

        event.markDeadLetterPublished();

        assertEquals("DEAD", event.getStatus());
        assertEquals(0, event.getAttempts());
    }

    private OutboxEventEntity newEvent() {
        return OutboxEventEntity.create("SHIPMENT", "shipment-1", "shipment.created",
                "shipment.events.v1", "shipment-1", mapper.createObjectNode(), null);
    }
}
