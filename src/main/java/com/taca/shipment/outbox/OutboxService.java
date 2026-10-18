package com.taca.shipment.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.event.EventEnvelopeFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OutboxService {
    private final OutboxRepository repository;
    private final EventEnvelopeFactory envelopes;
    private final ObjectMapper mapper;
    private final String topic;

    public OutboxService(OutboxRepository repository, EventEnvelopeFactory envelopes, ObjectMapper mapper,
            @Value("${shipment.kafka.shipment-events-topic}") String topic) {
        this.repository = repository;
        this.envelopes = envelopes;
        this.mapper = mapper;
        this.topic = topic;
    }

    public void append(String eventType, String aggregateType, String aggregateId, Object payload) {
        append(eventType, aggregateType, aggregateId, payload, EventContext.none());
    }

    public void append(String eventType, String aggregateType, String aggregateId, Object payload,
            EventContext context) {
        JsonNode payloadNode = mapper.valueToTree(payload);
        JsonNode envelope = envelopes.create(eventType, aggregateType, aggregateId, payloadNode, context);
        repository.save(
                OutboxEventEntity.create(aggregateType, aggregateId, eventType, topic, aggregateId, envelope, null));
    }
}
