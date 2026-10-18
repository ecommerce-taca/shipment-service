package com.taca.shipment.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.common.id.UuidV7;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", indexes = {
        @Index(name = "idx_outbox_dispatch", columnList = "status,next_attempt_at,created_at"),
        @Index(name = "idx_outbox_lease_dispatch", columnList = "status,next_attempt_at,lease_until,created_at")
})
public class OutboxEventEntity {
    @Id
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
    @Column(name = "aggregate_type", nullable = false, length = 40)
    private String aggregateType;
    @Column(name = "aggregate_id", nullable = false, length = 80)
    private String aggregateId;
    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;
    @Column(nullable = false, length = 160)
    private String topic;
    @Column(name = "event_key", nullable = false, length = 160)
    private String eventKey;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "json")
    private JsonNode payload;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "json")
    private JsonNode headers;
    @Column(nullable = false, length = 16)
    private String status = "PENDING";
    @Column(nullable = false)
    private int attempts = 0;
    @Column(name = "next_attempt_at", nullable = false)
    private LocalDateTime nextAttemptAt;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(name = "published_at")
    private LocalDateTime publishedAt;
    @Column(name = "last_error_code", length = 80)
    private String lastErrorCode;
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "lease_token", length = 36, columnDefinition = "CHAR(36)")
    private String leaseToken;
    @Column(name = "lease_until")
    private LocalDateTime leaseUntil;

    protected OutboxEventEntity() {
    }

    public static OutboxEventEntity create(String aggregateType, String aggregateId, String eventType, String topic,
            String key, JsonNode payload, JsonNode headers) {
        var e = new OutboxEventEntity();
        e.id = UuidV7.generate();
        e.aggregateType = aggregateType;
        e.aggregateId = aggregateId;
        e.eventType = eventType;
        e.topic = topic;
        e.eventKey = key;
        e.payload = payload.deepCopy();
        e.headers = headers == null ? null : headers.deepCopy();
        e.status = "PENDING";
        e.attempts = 0;
        e.createdAt = now();
        e.nextAttemptAt = e.createdAt;
        return e;
    }

    public void markPublished() {
        status = "PUBLISHED";
        publishedAt = now();
        lastErrorCode = null;
    }

    public void markFailed(String safeCode) {
        attempts++;
        lastErrorCode = safeCode;
        if (attempts >= 3) {
            status = "DLQ_PENDING";
            attempts = 0;
            nextAttemptAt = now();
        } else {
            status = "PENDING";
            nextAttemptAt = now().plusSeconds(2L * attempts);
        }
    }

    public void markDeadLetterFailed() {
        attempts++;
        lastErrorCode = "DLQ_PUBLISH_FAILED";
        if (attempts >= 3) {
            status = "DEAD";
        } else {
            status = "DLQ_PENDING";
            nextAttemptAt = now().plusSeconds(2L * attempts);
        }
    }

    public void markDeadLetterPublished() {
        status = "DEAD";
        publishedAt = now();
    }

    private static LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventKey() {
        return eventKey;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public JsonNode getHeaders() {
        return headers;
    }

    public String getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public LocalDateTime getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLeaseToken() {
        return leaseToken;
    }

    public LocalDateTime getLeaseUntil() {
        return leaseUntil;
    }
}
