package com.taca.shipment.event.consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderCancellationInbox {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final long leaseSeconds;

    public OrderCancellationInbox(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
            @Value("${shipment.kafka.command-lease-seconds:120}") long leaseSeconds) {
        if (leaseSeconds <= 0 || leaseSeconds > 3600)
            throw new IllegalArgumentException("shipment.kafka.command-lease-seconds must be between 1 and 3600");
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.leaseSeconds = leaseSeconds;
    }

    public void accept(String orderId, String eventId, String correlationId, String reason, String originalMessage,
            String topic, int partition, long offset) {
        transaction.executeWithoutResult(status -> {
            jdbc.update("""
                    INSERT IGNORE INTO order_cancellation_intents
                        (order_id, event_id, correlation_id, reason, source_topic, source_partition, source_offset,
                         original_message, status, attempts, next_attempt_at, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED', 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                    """, orderId, eventId, correlationId, reason, topic, partition, offset, originalMessage);
            Integer byOrder = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM order_cancellation_intents WHERE order_id=?",
                    Integer.class, orderId);
            String eventOrder = jdbc.query(
                    "SELECT order_id FROM order_cancellation_intents WHERE event_id=?",
                    (rs, row) -> rs.getString(1), eventId).stream().findFirst().orElse(null);
            if (eventOrder != null && !eventOrder.equals(orderId))
                throw new PermanentShipmentMessageException("order.cancelled event_id was reused for another order");
            if (byOrder == null || byOrder != 1)
                throw new IllegalStateException("Order cancellation intent was not persisted");
        });
    }

    public Optional<Claim> claimNext() {
        return transaction.execute(status -> {
            var rows = jdbc.query("""
                    SELECT order_id, event_id, correlation_id, reason, attempts
                    FROM order_cancellation_intents
                    WHERE status IN ('RECEIVED', 'RETRY_WAIT', 'WAITING_RECONCILIATION', 'PROCESSING')
                      AND next_attempt_at <= UTC_TIMESTAMP(6)
                      AND (lease_until IS NULL OR lease_until <= UTC_TIMESTAMP(6))
                    ORDER BY created_at ASC
                    LIMIT 1 FOR UPDATE SKIP LOCKED
                    """, (rs, row) -> mapClaim(rs));
            if (rows.isEmpty())
                return Optional.empty();
            Claim claim = rows.get(0);
            String token = UUID.randomUUID().toString();
            int changed = jdbc.update("""
                    UPDATE order_cancellation_intents
                    SET status='PROCESSING', lease_token=?, lease_until=TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                        updated_at=UTC_TIMESTAMP(6)
                    WHERE order_id=?
                    """, token, leaseSeconds, claim.orderId());
            if (changed != 1)
                throw new IllegalStateException("Locked order cancellation intent could not be claimed");
            return Optional.of(claim.withToken(token));
        });
    }

    public boolean complete(Claim claim, String errorCode) {
        return jdbc.update("""
                UPDATE order_cancellation_intents
                SET status='APPLIED', error_code=?, lease_token=NULL, lease_until=NULL,
                    completed_at=UTC_TIMESTAMP(6), updated_at=UTC_TIMESTAMP(6)
                WHERE order_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, errorCode, claim.orderId(), claim.leaseToken()) == 1;
    }

    public boolean ownsActiveLease(Claim claim) {
        Integer count = jdbc.queryForObject("""
                SELECT COUNT(*) FROM order_cancellation_intents
                WHERE order_id=? AND lease_token=? AND status='PROCESSING'
                  AND lease_until > UTC_TIMESTAMP(6)
                """, Integer.class, claim.orderId(), claim.leaseToken());
        return count != null && count == 1;
    }

    public boolean waitForReconciliation(Claim claim) {
        return jdbc.update("""
                UPDATE order_cancellation_intents
                SET status='WAITING_RECONCILIATION',
                    next_attempt_at=TIMESTAMPADD(SECOND, 60, UTC_TIMESTAMP(6)),
                    lease_token=NULL, lease_until=NULL, updated_at=UTC_TIMESTAMP(6)
                WHERE order_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, claim.orderId(), claim.leaseToken()) == 1;
    }

    public boolean retry(Claim claim, String errorCode, long delaySeconds) {
        return jdbc.update("""
                UPDATE order_cancellation_intents
                SET status='RETRY_WAIT', attempts=attempts+1,
                    next_attempt_at=TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                    error_code=?, lease_token=NULL, lease_until=NULL, updated_at=UTC_TIMESTAMP(6)
                WHERE order_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, delaySeconds, errorCode, claim.orderId(), claim.leaseToken()) == 1;
    }

    public boolean requireOperator(Claim claim, String errorCode) {
        return jdbc.update("""
                UPDATE order_cancellation_intents
                SET status='NEEDS_OPERATOR', attempts=attempts+1, error_code=?,
                    lease_token=NULL, lease_until=NULL, updated_at=UTC_TIMESTAMP(6)
                WHERE order_id=? AND lease_token=? AND lease_until > UTC_TIMESTAMP(6)
                """, errorCode, claim.orderId(), claim.leaseToken()) == 1;
    }

    public boolean replay(String orderId, String operatorId, String requestId) {
        if (operatorId == null || operatorId.isBlank() || operatorId.length() > 128)
            throw new IllegalArgumentException("A valid operator identity is required for cancellation replay");
        if (requestId != null && requestId.length() > 128)
            throw new IllegalArgumentException("Replay request_id must be at most 128 characters");
        return transaction.execute(status -> {
            int updated = jdbc.update("""
                    UPDATE order_cancellation_intents
                    SET status='RETRY_WAIT', attempts=0, next_attempt_at=UTC_TIMESTAMP(6),
                        lease_token=NULL, lease_until=NULL, error_code=NULL, error_message_safe=NULL,
                        replay_count=replay_count+1, replayed_by=?, updated_at=UTC_TIMESTAMP(6)
                    WHERE order_id=? AND status='NEEDS_OPERATOR'
                    """, operatorId, orderId);
            if (updated != 1)
                return false;
            jdbc.update("""
                    INSERT INTO shipment_command_replay_audit
                        (target_type, target_id, operator_id, request_id, replayed_at)
                    VALUES ('ORDER_CANCELLATION', ?, ?, ?, UTC_TIMESTAMP(6))
                    """, orderId, operatorId, requestId);
            return true;
        });
    }

    private Claim mapClaim(ResultSet rs) throws SQLException {
        return new Claim(rs.getString("order_id"), rs.getString("event_id"), rs.getString("correlation_id"),
                rs.getString("reason"), rs.getInt("attempts"), null);
    }

    public record Claim(String orderId, String eventId, String correlationId, String reason, int attempts,
            String leaseToken) {
        Claim withToken(String token) {
            return new Claim(orderId, eventId, correlationId, reason, attempts, token);
        }
    }
}
