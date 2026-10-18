package com.taca.shipment.shipment.persistence;

import com.taca.shipment.common.id.UuidV7;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "shipment_events", uniqueConstraints = @UniqueConstraint(name = "uk_shipment_events_carrier_external", columnNames = {
        "carrier",
        "external_event_id" }), indexes = @Index(name = "idx_shipment_events_shipment_occurred", columnList = "shipment_id,occurred_at"))
public class ShipmentEventEntity {
    @Id
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(name = "shipment_id", nullable = false, columnDefinition = "BINARY(16)")
    private UUID shipmentId;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Carrier carrier;
    @Enumerated(EnumType.STRING)
    @Column(name = "canonical_status", nullable = false, length = 32)
    private ShipmentStatus canonicalStatus;
    @Column(name = "carrier_status", length = 80)
    private String carrierStatus;
    @Column(name = "description_safe", length = 500)
    private String descriptionSafe;
    @Column(nullable = false, length = 24)
    private String source;
    @Column(name = "external_event_id", length = 128)
    private String externalEventId;
    @Column(name = "occurred_at", nullable = false)
    private LocalDateTime occurredAt;
    @Column(name = "received_at", nullable = false)
    private LocalDateTime receivedAt;

    protected ShipmentEventEntity() {
    }

    public static ShipmentEventEntity create(UUID shipmentId, Carrier carrier, ShipmentStatus status,
            String carrierStatus, String source, String description, String externalEventId, LocalDateTime occurredAt) {
        var e = new ShipmentEventEntity();
        e.id = UuidV7.generate();
        e.shipmentId = shipmentId;
        e.carrier = carrier;
        e.canonicalStatus = status;
        e.carrierStatus = carrierStatus;
        e.source = source;
        e.descriptionSafe = safe(description);
        e.externalEventId = externalEventId;
        e.occurredAt = occurredAt == null ? now() : occurredAt;
        e.receivedAt = now();
        return e;
    }

    private static String safe(String s) {
        if (s == null)
            return null;
        String v = s.replaceAll("[\\r\\n\\t]", " ").trim();
        return v.length() > 500 ? v.substring(0, 500) : v;
    }

    private static LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public UUID getId() {
        return id;
    }

    public UUID getShipmentId() {
        return shipmentId;
    }

    public Carrier getCarrier() {
        return carrier;
    }

    public ShipmentStatus getCanonicalStatus() {
        return canonicalStatus;
    }

    public String getCarrierStatus() {
        return carrierStatus;
    }

    public String getDescriptionSafe() {
        return descriptionSafe;
    }

    public String getSource() {
        return source;
    }

    public String getExternalEventId() {
        return externalEventId;
    }

    public LocalDateTime getOccurredAt() {
        return occurredAt;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }
}
