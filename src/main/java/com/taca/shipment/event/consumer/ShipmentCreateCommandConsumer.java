package com.taca.shipment.event.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.shipment.api.CreateShipmentRequest;
import jakarta.validation.Validator;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

@Component
@ConditionalOnProperty(name = "shipment.kafka.create-consumer-enabled", havingValue = "true")
public class ShipmentCreateCommandConsumer {
    private static final int MAX_COMMAND_BYTES = 256 * 1024;
    private final ObjectMapper mapper;
    private final Validator validator;
    private final ShipmentCreateCommandInbox inbox;

    public ShipmentCreateCommandConsumer(ObjectMapper mapper, Validator validator, ShipmentCreateCommandInbox inbox) {
        this.mapper = mapper;
        this.validator = validator;
        this.inbox = inbox;
    }

    @KafkaListener(topics = "${shipment.kafka.create-commands-topic:shipment.commands.v1}", groupId = "${shipment.kafka.create-consumer-group-id:shipment-service-create-v1}", containerFactory = "shipmentKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        if (record.value() == null
                || record.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_COMMAND_BYTES)
            throw new PermanentShipmentMessageException("Create command is missing or exceeds the 256 KiB limit");
        JsonNode envelope;
        try {
            envelope = mapper.readTree(record.value());
        } catch (Exception ex) {
            throw new PermanentShipmentMessageException("Create command is not valid JSON", ex);
        }
        if (envelope == null || !envelope.isObject())
            throw new PermanentShipmentMessageException("Create command envelope must be a JSON object");
        if (!envelope.path("payload").isObject())
            throw new PermanentShipmentMessageException("Create command payload must be a JSON object");
        String messageId = text(envelope, "message_id");
        String messageType = text(envelope, "message_type");
        String correlationId = text(envelope, "correlation_id");
        String causationId = optionalText(envelope, "causation_id");
        JsonNode payload = envelope.path("payload");

        if (!"shipment.create".equals(messageType))
            throw new PermanentShipmentMessageException("Unsupported shipment command message_type");
        if (!messageId.matches("[A-Za-z0-9._:-]{1,128}"))
            throw new PermanentShipmentMessageException("Create command message_id is missing or invalid");
        if (!envelope.path("schema_version").isIntegralNumber()
                || !envelope.path("schema_version").canConvertToInt()
                || envelope.path("schema_version").intValue() != 1)
            throw new PermanentShipmentMessageException("Unsupported shipment create command schema_version");
        if (correlationId.isBlank() || correlationId.length() > 128)
            throw new PermanentShipmentMessageException("Create command correlation_id is missing or invalid");
        if (causationId != null && causationId.length() > 128)
            throw new PermanentShipmentMessageException("Create command causation_id exceeds 128 characters");
        try {
            Instant.parse(text(envelope, "occurred_at"));
        } catch (RuntimeException ex) {
            throw new PermanentShipmentMessageException("Create command occurred_at must be an ISO-8601 instant", ex);
        }

        String commandId = text(payload, "command_id");
        String shopOrderId = text(payload, "shop_order_id");
        String idempotencyKey = text(payload, "idempotency_key");
        if (!messageId.equals(commandId))
            throw new PermanentShipmentMessageException("message_id must equal payload.command_id");
        if (shopOrderId.isBlank() || shopOrderId.length() > 80 || !shopOrderId.equals(record.key()))
            throw new PermanentShipmentMessageException("Kafka key must equal payload.shop_order_id");
        if (!payload.path("buyer").isObject()
                || !payload.path("from_address_snapshot").isObject()
                || !payload.path("to_address_snapshot").isObject()
                || !payload.path("items").isArray())
            throw new PermanentShipmentMessageException("Create command is missing required object or array fields");
        if (idempotencyKey.isBlank() || idempotencyKey.length() > 128)
            throw new PermanentShipmentMessageException("Create command idempotency_key is missing or invalid");
        if (!payload.path("cod_amount").isIntegralNumber() || !payload.path("cod_amount").canConvertToLong()
                || payload.path("cod_amount").longValue() < 0
                || !payload.path("shipping_fee").isIntegralNumber() || !payload.path("shipping_fee").canConvertToLong()
                || payload.path("shipping_fee").longValue() < 0)
            throw new PermanentShipmentMessageException("Create command amounts must be non-negative integers");

        CreateShipmentRequest request;
        try {
            request = new CreateShipmentRequest(
                    shopOrderId,
                    optionalText(payload, "checkout_group_id"),
                    text(payload, "shop_id"),
                    text(payload.path("buyer"), "user_id"),
                    text(payload.path("buyer"), "email"),
                    text(payload, "carrier"),
                    payload.path("from_address_snapshot"),
                    payload.path("to_address_snapshot"),
                    mapper.convertValue(payload.path("items"),
                            mapper.getTypeFactory().constructCollectionType(List.class, ShipmentItemRequest.class)),
                    payload.path("cod_amount").asLong(-1),
                    payload.path("shipping_fee").asLong(-1),
                    text(payload, "payment_method"),
                    text(payload, "currency"));
        } catch (Exception ex) {
            throw new PermanentShipmentMessageException("Create command payload does not match schema version 1", ex);
        }
        var violations = validator.validate(request);
        if (!violations.isEmpty())
            throw new PermanentShipmentMessageException("Create command payload failed request validation");

        inbox.accept(new ShipmentCreateCommandInbox.Incoming(
                commandId,
                shopOrderId,
                idempotencyKey,
                correlationId,
                causationId,
                mapper.valueToTree(request),
                payload,
                record.value(),
                record.topic(),
                record.partition(),
                record.offset()));
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : "";
    }

    private String optionalText(JsonNode node, String field) {
        String value = text(node, field);
        return value.isBlank() ? null : value;
    }
}
