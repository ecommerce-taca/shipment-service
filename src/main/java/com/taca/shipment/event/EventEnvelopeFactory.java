package com.taca.shipment.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taca.shipment.common.id.UuidV7;
import org.springframework.stereotype.Component;
import java.time.Instant;

@Component
public class EventEnvelopeFactory {
    private final ObjectMapper mapper;

    public EventEnvelopeFactory(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public JsonNode create(String eventType, String aggregateType, String aggregateId, JsonNode payload) {
        return create(eventType, aggregateType, aggregateId, payload, EventContext.none());
    }

    public JsonNode create(String eventType, String aggregateType, String aggregateId, JsonNode payload,
            EventContext context) {
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("event_id", UuidV7.generate().toString());
        envelope.put("schema_version", 1);
        envelope.put("event_type", eventType);
        envelope.put("occurred_at", Instant.now().toString());
        envelope.put("aggregate_type", aggregateType);
        envelope.put("aggregate_id", aggregateId);
        if (context.correlationId() != null && !context.correlationId().isBlank())
            envelope.put("correlation_id", context.correlationId());
        if (context.causationId() != null && !context.causationId().isBlank())
            envelope.put("causation_id", context.causationId());
        envelope.set("payload", payload);
        return envelope;
    }
}
