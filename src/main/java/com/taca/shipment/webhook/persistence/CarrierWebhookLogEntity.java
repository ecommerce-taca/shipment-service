package com.taca.shipment.webhook.persistence;

import com.taca.shipment.common.id.UuidV7;
import com.taca.shipment.shipment.domain.Carrier;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "carrier_webhook_logs", uniqueConstraints = @UniqueConstraint(name = "uk_carrier_webhook_event", columnNames = {
        "carrier",
        "external_event_id" }), indexes = @Index(name = "idx_carrier_webhook_status_received", columnList = "status,received_at"))
public class CarrierWebhookLogEntity {
    @Id
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Carrier carrier;
    @Column(name = "external_event_id", nullable = false, length = 128)
    private String externalEventId;
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "payload_hash", nullable = false, length = 64, columnDefinition = "CHAR(64)")
    private String payloadHash;
    @Column(name = "signature_valid", nullable = false)
    private boolean signatureValid;
    @Column(nullable = false, length = 24)
    private String status;
    @Column(name = "safe_error_code", length = 80)
    private String safeErrorCode;
    @Column(name = "received_at", nullable = false)
    private LocalDateTime receivedAt;
    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    protected CarrierWebhookLogEntity() {
    }

    public static CarrierWebhookLogEntity create(Carrier c, String eventId, String hash, boolean signatureValid,
            String status, String error) {
        var e = new CarrierWebhookLogEntity();
        e.id = UuidV7.generate();
        e.carrier = c;
        e.externalEventId = eventId;
        e.payloadHash = hash;
        e.signatureValid = signatureValid;
        e.status = status;
        e.safeErrorCode = error;
        e.receivedAt = now();
        return e;
    }

    public void processed() {
        status = "APPLIED";
        processedAt = now();
        safeErrorCode = null;
    }

    public void ignored(String value, String code) {
        status = value;
        processedAt = now();
        safeErrorCode = code;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    private static LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }
}
