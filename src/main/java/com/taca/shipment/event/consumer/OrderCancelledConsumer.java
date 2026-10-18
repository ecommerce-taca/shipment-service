package com.taca.shipment.event.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Component
@ConditionalOnProperty(name = "shipment.kafka.consumer-enabled", havingValue = "true")
public class OrderCancelledConsumer {
    private static final int MAX_ORDER_EVENT_BYTES = 256 * 1024;
    private static final Logger log = LoggerFactory.getLogger(OrderCancelledConsumer.class);
    private final ObjectMapper mapper;
    private final OrderCancellationInbox inbox;

    public OrderCancelledConsumer(ObjectMapper mapper, OrderCancellationInbox inbox) {
        this.mapper = mapper;
        this.inbox = inbox;
    }

    @KafkaListener(topics = "${shipment.kafka.order-events-topic:order.events.v1}", groupId = "shipment-service-order-cancelled", containerFactory = "shipmentKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        if (record.value() == null)
            throw new PermanentShipmentMessageException("Order event is missing");
        JsonNode envelope;
        try {
            envelope = mapper.readTree(record.value());
        } catch (Exception ex) {
            throw new PermanentShipmentMessageException("Order event is not valid JSON", ex);
        }
        if (envelope == null || !envelope.isObject())
            throw new PermanentShipmentMessageException("Order event envelope must be a JSON object");
        if (!"order.cancelled".equals(envelope.path("event_type").asText()))
            return;
        if (record.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_ORDER_EVENT_BYTES)
            throw new PermanentShipmentMessageException("order.cancelled exceeds the 256 KiB limit");
        JsonNode payload = envelope.path("payload");
        String orderId = payload.path("order_id").asText("");
        String eventId = envelope.path("event_id").asText("");
        String correlationId = envelope.path("correlation_id").asText("");
        if (orderId.isBlank() || orderId.length() > 80 || eventId.isBlank() || eventId.length() > 115)
            throw new PermanentShipmentMessageException("order.cancelled event requires valid event_id and order_id");
        if (correlationId.length() > 128)
            throw new PermanentShipmentMessageException("order.cancelled correlation_id exceeds 128 characters");
        String reason = payload.path("reason").asText("ORDER_CANCELLED");
        if (reason.isBlank())
            reason = "ORDER_CANCELLED";
        if (reason.length() > 255)
            throw new PermanentShipmentMessageException("order.cancelled reason exceeds 255 characters");
        inbox.accept(orderId, eventId, correlationId.isBlank() ? null : correlationId, reason, record.value(),
                record.topic(), record.partition(), record.offset());
        log.info("Persisted order cancellation intent event_id={} order_id={}", eventId, orderId);
    }
}
