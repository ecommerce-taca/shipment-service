package com.taca.shipment.shipment.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
public class IdempotencyEntity {
    @EmbeddedId
    private IdempotencyId id;
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "request_hash", nullable = false, length = 64, columnDefinition = "CHAR(64)")
    private String requestHash;
    @Column(nullable = false, length = 24)
    private String state;
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(name = "shipment_id", columnDefinition = "BINARY(16)")
    private UUID shipmentId;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_snapshot", columnDefinition = "json")
    private JsonNode responseSnapshot;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(name = "expires_at", nullable = false)
    private LocalDateTime expiresAt;

    protected IdempotencyEntity() {
    }

    public static IdempotencyEntity processing(String key, String operation, String hash, UUID shipmentId) {
        var e = new IdempotencyEntity();
        e.id = new IdempotencyId(key, operation);
        e.requestHash = hash;
        e.state = "PROCESSING";
        e.shipmentId = shipmentId;
        e.createdAt = now();
        e.expiresAt = e.createdAt.plusHours(24);
        return e;
    }

    public void complete(JsonNode response) {
        state = "COMPLETED";
        responseSnapshot = response == null ? null : response.deepCopy();
    }

    public void markPending() {
        state = "PENDING_RECONCILIATION";
    }

    public void attachShipment(UUID id) {
        shipmentId = id;
    }

    private static LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public IdempotencyId getId() {
        return id;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public String getState() {
        return state;
    }

    public UUID getShipmentId() {
        return shipmentId;
    }

    public JsonNode getResponseSnapshot() {
        return responseSnapshot;
    }

    public LocalDateTime getExpiresAt() {
        return expiresAt;
    }
}
