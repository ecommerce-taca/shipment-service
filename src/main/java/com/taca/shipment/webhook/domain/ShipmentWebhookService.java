package com.taca.shipment.webhook.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.domain.ShipmentCommandService;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import com.taca.shipment.shipment.domain.ShipmentStateMachine;
import com.taca.shipment.shipment.persistence.ShipmentEventRepository;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import com.taca.shipment.webhook.api.CarrierWebhookRequest;
import com.taca.shipment.webhook.persistence.CarrierWebhookLogEntity;
import com.taca.shipment.webhook.persistence.CarrierWebhookLogRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;

@Service
public class ShipmentWebhookService {
    private final WebhookSignatureVerifier verifier;
    private final ShipmentRepository shipments;
    private final ShipmentEventRepository events;
    private final CarrierWebhookLogRepository logs;
    private final ShipmentCommandService commands;
    private final ObjectMapper mapper;

    public ShipmentWebhookService(WebhookSignatureVerifier v, ShipmentRepository s, ShipmentEventRepository e,
            CarrierWebhookLogRepository l, ShipmentCommandService c, ObjectMapper m) {
        verifier = v;
        shipments = s;
        events = e;
        logs = l;
        commands = c;
        mapper = m;
    }

    @Transactional
    public Map<String, Object> accept(String carrierValue, String rawBody, String signature, String remoteIp) {
        Carrier carrier;
        try {
            carrier = Carrier.fromApi(carrierValue);
        } catch (Exception ex) {
            throw invalid();
        }
        if (!verifier.allowedIp(remoteIp) || !verifier.verify(carrier, rawBody, signature))
            throw invalid();
        CarrierWebhookRequest req;
        try {
            req = mapper.readValue(rawBody, CarrierWebhookRequest.class);
        } catch (Exception ex) {
            throw invalid();
        }
        if (req.external_event_id() == null || req.external_event_id().isBlank()
                || req.external_event_id().length() > 128 || req.tracking_code() == null
                || req.tracking_code().isBlank() || req.tracking_code().length() > 64 || req.status() == null
                || req.status().isBlank() || req.occurred_at() == null)
            throw invalid();
        String hash = sha256(rawBody);
        var previousLog = logs.findByCarrierAndExternalEventId(carrier, req.external_event_id());
        if (previousLog.isPresent()) {
            if (!previousLog.get().getPayloadHash().equals(hash))
                throw invalid();
            return Map.of("accepted", true, "duplicate", true);
        }
        if (events.existsByCarrierAndExternalEventId(carrier, req.external_event_id()))
            return Map.of("accepted", true, "duplicate", true);
        CarrierWebhookLogEntity webhookLog = logs
                .save(CarrierWebhookLogEntity.create(carrier, req.external_event_id(), hash, true, "RECEIVED", null));
        var shipment = shipments.findByTrackingCodeAndCarrier(req.tracking_code(), carrier).orElse(null);
        if (shipment == null) {
            webhookLog.ignored("REJECTED", "TRACKING_NOT_FOUND");
            return Map.of("accepted", true, "ignored", true);
        }
        ShipmentStatus next = mapStatus(req.status());
        if (next == null) {
            webhookLog.ignored("UNKNOWN_STATUS", "CARRIER_STATUS_UNKNOWN");
            return Map.of("accepted", true, "ignored", true);
        }
        var latest = events.findFirstByShipmentIdOrderByOccurredAtDesc(shipment.getId());
        if (latest.isPresent()
                && req.occurred_at().isBefore(latest.get().getOccurredAt().toInstant(java.time.ZoneOffset.UTC))) {
            webhookLog.ignored("IGNORED_OLD", "EVENT_OCCURRED_BEFORE_LATEST");
            return Map.of("accepted", true, "ignored", true);
        }
        if (!next.equals(shipment.getStatus()) && !ShipmentStateMachine.canTransition(shipment.getStatus(), next)) {
            webhookLog.ignored("IGNORED_OLD", "INVALID_OR_STALE_TRANSITION");
            return Map.of("accepted", true, "ignored", true);
        }
        commands.applyCarrierStatus(shipment, next, req.status(), req.external_event_id(), req.occurred_at(),
                safeNote(req.note()));
        webhookLog.processed();
        return Map.of("accepted", true, "duplicate", false);
    }

    private ShipmentStatus mapStatus(String s) {
        return switch (s.trim().toLowerCase().replace(' ', '_')) {
            case "created", "ready_to_ship", "picked" -> ShipmentStatus.CREATED;
            case "picked_up", "picked-up", "pickedup", "pickup" -> ShipmentStatus.PICKED_UP;
            case "in_transit", "transporting", "delivering", "delivery_in_progress" -> ShipmentStatus.IN_TRANSIT;
            case "delivered", "delivery_success", "success" -> ShipmentStatus.DELIVERED;
            case "failed", "delivery_failed", "exception", "returning" -> ShipmentStatus.FAILED;
            case "cancelled", "canceled" -> ShipmentStatus.CANCELLED;
            default -> null;
        };
    }

    private String safeNote(String n) {
        if (n == null)
            return null;
        return n.replaceAll("[\\r\\n\\t]", " ").replaceAll("(?i)(phone|address|token|secret)\\s*[:=].*", "[redacted]");
    }

    private String sha256(String body) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private ShipmentException invalid() {
        return new ShipmentException("SHIPMENT_WEBHOOK_INVALID", "Carrier webhook failed authentication or validation",
                HttpStatus.BAD_REQUEST);
    }
}
