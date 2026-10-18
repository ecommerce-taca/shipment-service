package com.taca.shipment.event.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ShipmentCreateCommandInbox {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final TransactionTemplate transaction;
    private final long leaseSeconds;

    public ShipmentCreateCommandInbox(JdbcTemplate jdbc, ObjectMapper mapper,
            PlatformTransactionManager transactionManager,
            @Value("${shipment.kafka.command-lease-seconds:120}") long leaseSeconds) {
        if (leaseSeconds <= 0 || leaseSeconds > 3600)
            throw new IllegalArgumentException("shipment.kafka.command-lease-seconds must be between 1 and 3600");
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.transaction = new TransactionTemplate(transactionManager);
        this.leaseSeconds = leaseSeconds;
    }

    public void accept(Incoming incoming) {
        transaction.executeWithoutResult(status -> {
            String hash = hash(canonical(incoming.payload()));
            jdbc.update("""
                    INSERT IGNORE INTO shipment_create_commands
                        (command_id, request_hash, shop_order_id, idempotency_key, correlation_id, causation_id,
                         payload, original_message, source_topic, source_partition, source_offset, status,
                         attempts, next_attempt_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, 'ACCEPTED', 0,
                            UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                    """,
                    incoming.commandId(), hash, incoming.shopOrderId(), incoming.idempotencyKey(),
                    incoming.correlationId(), incoming.causationId(), write(incoming.request()),
                    incoming.originalMessage(), incoming.topic(), incoming.partition(), incoming.offset());
            String storedHash = jdbc.queryForObject(
                    "SELECT request_hash FROM shipment_create_commands WHERE command_id=?",
                    String.class, incoming.commandId());
            if (!hash.equals(storedHash))
                throw new PermanentShipmentMessageException(
                        "command_id was previously received with a different payload");
        });
    }

    public Optional<Claim> claimNext() {
        return transaction.execute(status -> {
            var rows = jdbc.query("""
                    SELECT command_id, idempotency_key, correlation_id, causation_id, shop_order_id, payload, attempts
                    FROM shipment_create_commands
                    WHERE status IN ('ACCEPTED', 'RETRY_WAIT', 'PROCESSING')
                      AND next_attempt_at <= UTC_TIMESTAMP(6)
                      AND (lease_until IS NULL OR lease_until <= UTC_TIMESTAMP(6))
                    ORDER BY created_at ASC
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, (rs, row) -> mapClaim(rs));
            if (rows.isEmpty())
                return Optional.empty();

            Claim selected = rows.get(0);
            String token = UUID.randomUUID().toString();
            int updated = jdbc.update("""
                    UPDATE shipment_create_commands
                    SET status='PROCESSING', lease_token=?, lease_until=TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE command_id=?
                    """, token, leaseSeconds, selected.commandId());
            if (updated != 1)
                throw new IllegalStateException("Locked shipment create command could not be claimed");
            return Optional.of(selected.withToken(token));
        });
    }

    public boolean hasCancellationIntent(String shopOrderId) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM order_cancellation_intents WHERE order_id=?",
                Integer.class, shopOrderId);
        return count != null && count > 0;
    }

    public boolean hasCreateInProgress(String shopOrderId) {
        Integer count = jdbc.queryForObject("""
                SELECT COUNT(*) FROM shipment_create_commands
                WHERE shop_order_id=? AND status IN ('ACCEPTED', 'PROCESSING', 'RETRY_WAIT',
                                                       'PENDING_RECONCILIATION')
                """, Integer.class, shopOrderId);
        return count != null && count > 0;
    }

    public boolean complete(Claim claim, String shipmentId) {
        return jdbc.update("""
                UPDATE shipment_create_commands
                SET status='COMPLETED', shipment_id=?, lease_token=NULL, lease_until=NULL,
                    error_code=NULL, error_message_safe=NULL, completed_at=UTC_TIMESTAMP(6),
                    updated_at=UTC_TIMESTAMP(6)
                WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, shipmentId, claim.commandId(), claim.leaseToken()) == 1;
    }

    public boolean ownsActiveLease(Claim claim) {
        Integer count = jdbc.queryForObject("""
                SELECT COUNT(*) FROM shipment_create_commands
                WHERE command_id=? AND lease_token=? AND status='PROCESSING'
                  AND lease_until > UTC_TIMESTAMP(6)
                """, Integer.class, claim.commandId(), claim.leaseToken());
        return count != null && count == 1;
    }

    public boolean pendingReconciliation(Claim claim, String shipmentId) {
        return jdbc.update("""
                UPDATE shipment_create_commands
                SET status='PENDING_RECONCILIATION', shipment_id=?, lease_token=NULL, lease_until=NULL,
                    error_code='CARRIER_OUTCOME_AMBIGUOUS', error_message_safe=NULL,
                    updated_at=UTC_TIMESTAMP(6)
                WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, shipmentId, claim.commandId(), claim.leaseToken()) == 1;
    }

    public boolean reject(Claim claim, String errorCode, String safeMessage) {
        return jdbc.update("""
                UPDATE shipment_create_commands
                SET status='REJECTED', error_code=?, error_message_safe=?, lease_token=NULL, lease_until=NULL,
                    completed_at=UTC_TIMESTAMP(6), updated_at=UTC_TIMESTAMP(6)
                WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, safe(errorCode, 80), safe(safeMessage, 500), claim.commandId(), claim.leaseToken()) == 1;
    }

    public boolean retry(Claim claim, String errorCode, String safeMessage, long delaySeconds) {
        return jdbc.update("""
                UPDATE shipment_create_commands
                SET status='RETRY_WAIT', attempts=attempts+1, next_attempt_at=TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                    error_code=?, error_message_safe=?, lease_token=NULL, lease_until=NULL,
                    updated_at=UTC_TIMESTAMP(6)
                WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, delaySeconds, safe(errorCode, 80), safe(safeMessage, 500), claim.commandId(),
                claim.leaseToken()) == 1;
    }

    public boolean deadLetter(Claim claim, String errorCode, String safeMessage) {
        return transaction.execute(status -> {
            int inserted = jdbc.update("""
                    INSERT IGNORE INTO shipment_command_dlq
                        (command_id, original_message, source_topic, source_partition, source_offset, attempts,
                         error_code, error_message_safe, created_at)
                    SELECT command_id, original_message, source_topic, source_partition, source_offset, attempts + 1,
                           ?, ?, UTC_TIMESTAMP(6)
                    FROM shipment_create_commands
                    WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                    """, safe(errorCode, 80), safe(safeMessage, 500), claim.commandId(), claim.leaseToken());
            if (inserted != 1)
                return false;
            return jdbc.update("""
                    UPDATE shipment_create_commands
                    SET status='DEAD', attempts=attempts+1, error_code=?, error_message_safe=?,
                        lease_token=NULL, lease_until=NULL, completed_at=UTC_TIMESTAMP(6),
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE command_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                    """, safe(errorCode, 80), safe(safeMessage, 500), claim.commandId(),
                    claim.leaseToken()) == 1;
        });
    }

    public Optional<CommandSummary> findByIdempotencyKey(String idempotencyKey) {
        return jdbc.query("""
                SELECT command_id, correlation_id, causation_id
                FROM shipment_create_commands WHERE idempotency_key=?
                ORDER BY created_at ASC LIMIT 1
                """, (rs, row) -> new CommandSummary(
                rs.getString("command_id"), rs.getString("correlation_id"), rs.getString("causation_id")),
                idempotencyKey).stream().findFirst();
    }

    public String shopId(String commandId) {
        return jdbc.queryForObject("""
                SELECT JSON_UNQUOTE(JSON_EXTRACT(payload, '$.shop_id'))
                FROM shipment_create_commands WHERE command_id=?
                """, String.class, commandId);
    }

    public boolean markReconciled(String shopOrderId, String shopId, String shipmentId) {
        return jdbc.update("""
                UPDATE shipment_create_commands
                SET status='COMPLETED', shipment_id=?, error_code=NULL, error_message_safe=NULL,
                    lease_token=NULL, lease_until=NULL,
                    completed_at=UTC_TIMESTAMP(6), updated_at=UTC_TIMESTAMP(6)
                WHERE shop_order_id=? AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.shop_id'))=?
                  AND status IN ('PROCESSING', 'PENDING_RECONCILIATION')
                """, shipmentId, shopOrderId, shopId) > 0;
    }

    public boolean replay(String commandId, String operatorId, String requestId) {
        if (operatorId == null || operatorId.isBlank() || operatorId.length() > 128)
            throw new IllegalArgumentException("A valid operator identity is required for command replay");
        if (requestId != null && requestId.length() > 128)
            throw new IllegalArgumentException("Replay request_id must be at most 128 characters");
        return transaction.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE shipment_create_commands
                    SET status='RETRY_WAIT', attempts=0, next_attempt_at=UTC_TIMESTAMP(6),
                        lease_token=NULL, lease_until=NULL, error_code=NULL, error_message_safe=NULL,
                        replay_count=replay_count+1, replayed_by=?, updated_at=UTC_TIMESTAMP(6)
                    WHERE command_id=? AND status='DEAD'
                    """, operatorId, commandId);
            if (updated != 1)
                return false;
            jdbc.update("""
                    UPDATE shipment_command_dlq
                    SET replayed_at=UTC_TIMESTAMP(6), replayed_by=?
                    WHERE command_id=? AND replayed_at IS NULL
                    """, operatorId, commandId);
            jdbc.update("""
                    INSERT INTO shipment_command_replay_audit
                        (target_type, target_id, operator_id, request_id, replayed_at)
                    VALUES ('CREATE_COMMAND', ?, ?, ?, UTC_TIMESTAMP(6))
                    """, commandId, operatorId, requestId);
            return true;
        });
    }

    private Claim mapClaim(ResultSet rs) throws SQLException {
        try {
            return new Claim(
                    rs.getString("command_id"),
                    rs.getString("idempotency_key"),
                    rs.getString("correlation_id"),
                    rs.getString("causation_id"),
                    rs.getString("shop_order_id"),
                    mapper.readTree(rs.getString("payload")),
                    rs.getInt("attempts"),
                    null);
        } catch (Exception ex) {
            throw new IllegalStateException("Persisted shipment command payload cannot be read", ex);
        }
    }

    private JsonNode canonical(JsonNode node) {
        if (node == null || node.isValueNode())
            return node;
        if (node.isArray()) {
            var array = mapper.createArrayNode();
            node.forEach(child -> array.add(canonical(child)));
            return array;
        }
        var result = mapper.createObjectNode();
        var names = new java.util.ArrayList<String>();
        node.fieldNames().forEachRemaining(names::add);
        java.util.Collections.sort(names);
        names.forEach(name -> result.set(name, canonical(node.get(name))));
        return result;
    }

    private String hash(JsonNode node) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(mapper.writeValueAsBytes(node)));
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot hash shipment command payload", ex);
        }
    }

    private String write(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot serialize shipment create command", ex);
        }
    }

    private String safe(String value, int maxLength) {
        if (value == null)
            return null;
        String sanitized = value.replaceAll("[\\r\\n\\t]", " ").replaceAll(
                "(?i)(phone|address|token|secret|email)\\s*[:=].*", "[redacted]").trim();
        return sanitized.length() > maxLength ? sanitized.substring(0, maxLength) : sanitized;
    }

    public record Incoming(String commandId, String shopOrderId, String idempotencyKey, String correlationId,
            String causationId, JsonNode request, JsonNode payload, String originalMessage, String topic,
            int partition, long offset) {
    }

    public record Claim(String commandId, String idempotencyKey, String correlationId, String causationId,
            String shopOrderId, JsonNode request, int attempts, String leaseToken) {
        Claim withToken(String token) {
            return new Claim(commandId, idempotencyKey, correlationId, causationId, shopOrderId, request, attempts,
                    token);
        }
    }

    public record CommandSummary(String commandId, String correlationId, String causationId) {
    }
}
