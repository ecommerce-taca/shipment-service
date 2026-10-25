package com.taca.shipment.webhook.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.domain.ShipmentCommandService;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import com.taca.shipment.shipment.persistence.ShipmentEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventRepository;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import com.taca.shipment.webhook.persistence.CarrierWebhookLogEntity;
import com.taca.shipment.webhook.persistence.CarrierWebhookLogRepository;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ShipmentWebhookServiceTest {
    private static final String SECRET = "test-mock-secret";
    private static final String EVENT_ID = "carrier-event-1";
    private static final Instant OCCURRED_AT = Instant.parse("2026-10-18T10:00:00Z");
    private static final String RAW_BODY = """
            {"external_event_id":"carrier-event-1","tracking_code":"MOCK123","status":"in_transit","occurred_at":"2026-10-18T10:00:00Z"}
            """.trim();
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    @Test
    void acceptsValidWebhookAndAppliesCanonicalStatus() throws Exception {
        ShipmentRepository shipments = mock(ShipmentRepository.class);
        ShipmentEventRepository events = mock(ShipmentEventRepository.class);
        CarrierWebhookLogRepository logs = mock(CarrierWebhookLogRepository.class);
        ShipmentCommandService commands = mock(ShipmentCommandService.class);
        ShipmentEntity shipment = shipment();
        when(logs.findByCarrierAndExternalEventId(Carrier.MOCK, EVENT_ID)).thenReturn(Optional.empty());
        when(events.existsByCarrierAndExternalEventId(Carrier.MOCK, EVENT_ID)).thenReturn(false);
        when(logs.save(any(CarrierWebhookLogEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(shipments.findByTrackingCodeAndCarrier("MOCK123", Carrier.MOCK)).thenReturn(Optional.of(shipment));
        when(events.findFirstByShipmentIdOrderByOccurredAtDesc(shipment.getId())).thenReturn(Optional.empty());
        ShipmentWebhookService service = service(shipments, events, logs, commands);

        var result = service.accept("MOCK", RAW_BODY, signature(RAW_BODY), "127.0.0.1");

        assertEquals(Boolean.TRUE, result.get("accepted"));
        assertEquals(Boolean.FALSE, result.get("duplicate"));
        verify(commands).applyCarrierStatus(shipment, ShipmentStatus.IN_TRANSIT, "in_transit", EVENT_ID,
                OCCURRED_AT, null);
        verify(logs).save(any(CarrierWebhookLogEntity.class));
    }

    @Test
    void acknowledgesIdenticalDuplicateWithoutRepeatingStateChange() throws Exception {
        ShipmentRepository shipments = mock(ShipmentRepository.class);
        ShipmentEventRepository events = mock(ShipmentEventRepository.class);
        CarrierWebhookLogRepository logs = mock(CarrierWebhookLogRepository.class);
        ShipmentCommandService commands = mock(ShipmentCommandService.class);
        CarrierWebhookLogEntity prior = CarrierWebhookLogEntity.create(
                Carrier.MOCK, EVENT_ID, sha256(RAW_BODY), true, "APPLIED", null);
        when(logs.findByCarrierAndExternalEventId(Carrier.MOCK, EVENT_ID)).thenReturn(Optional.of(prior));
        ShipmentWebhookService service = service(shipments, events, logs, commands);

        var result = service.accept("MOCK", RAW_BODY, signature(RAW_BODY), "127.0.0.1");

        assertEquals(Boolean.TRUE, result.get("accepted"));
        assertEquals(Boolean.TRUE, result.get("duplicate"));
        verifyNoInteractions(shipments, events, commands);
        verify(logs, never()).save(any(CarrierWebhookLogEntity.class));
    }

    @Test
    void rejectsReusedEventIdWithChangedPayload() throws Exception {
        ShipmentRepository shipments = mock(ShipmentRepository.class);
        ShipmentEventRepository events = mock(ShipmentEventRepository.class);
        CarrierWebhookLogRepository logs = mock(CarrierWebhookLogRepository.class);
        ShipmentCommandService commands = mock(ShipmentCommandService.class);
        CarrierWebhookLogEntity prior = CarrierWebhookLogEntity.create(
                Carrier.MOCK, EVENT_ID, sha256(RAW_BODY), true, "APPLIED", null);
        when(logs.findByCarrierAndExternalEventId(Carrier.MOCK, EVENT_ID)).thenReturn(Optional.of(prior));
        String changedBody = RAW_BODY.replace("in_transit", "delivered");
        ShipmentWebhookService service = service(shipments, events, logs, commands);

        ShipmentException error = assertThrows(ShipmentException.class,
                () -> service.accept("MOCK", changedBody, signature(changedBody), "127.0.0.1"));

        assertEquals("SHIPMENT_WEBHOOK_INVALID", error.code());
        assertEquals(HttpStatus.BAD_REQUEST, error.status());
        verifyNoInteractions(shipments, events, commands);
        verify(logs, never()).save(any(CarrierWebhookLogEntity.class));
    }

    @Test
    void invalidSignatureIsRejectedBeforeRepositoryOrCommandAccess() {
        ShipmentRepository shipments = mock(ShipmentRepository.class);
        ShipmentEventRepository events = mock(ShipmentEventRepository.class);
        CarrierWebhookLogRepository logs = mock(CarrierWebhookLogRepository.class);
        ShipmentCommandService commands = mock(ShipmentCommandService.class);
        ShipmentWebhookService service = service(shipments, events, logs, commands);

        ShipmentException error = assertThrows(ShipmentException.class,
                () -> service.accept("MOCK", RAW_BODY, "bad-signature", "127.0.0.1"));

        assertEquals("SHIPMENT_WEBHOOK_INVALID", error.code());
        verifyNoInteractions(shipments, events, logs, commands);
    }

    private ShipmentWebhookService service(ShipmentRepository shipments, ShipmentEventRepository events,
            CarrierWebhookLogRepository logs, ShipmentCommandService commands) {
        WebhookSignatureVerifier verifier = new WebhookSignatureVerifier("", "", "", SECRET, "127.0.0.1");
        return new ShipmentWebhookService(verifier, shipments, events, logs, commands, MAPPER);
    }

    private ShipmentEntity shipment() throws Exception {
        ShipmentEntity shipment = ShipmentEntity.pending(
                "order-1", null, "shop-1", "buyer-1", "buyer@example.com", Carrier.MOCK,
                MAPPER.readTree("{\"line1\":\"from\"}"), MAPPER.readTree("{\"line1\":\"to\"}"),
                20000, 20000L, 20000L, "VND", 0, null, null, "create-key");
        shipment.carrierCreated("external-1", "MOCK123");
        return shipment;
    }

    private String signature(String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    private String sha256(String body) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(body.getBytes(StandardCharsets.UTF_8)));
    }
}
