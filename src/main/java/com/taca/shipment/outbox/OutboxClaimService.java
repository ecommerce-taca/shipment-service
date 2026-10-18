package com.taca.shipment.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

@Service
public class OutboxClaimService {
    private static final int MAX_ATTEMPTS = 3;
    static final long MAX_LEASE_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final Duration leaseDuration;

    public OutboxClaimService(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
            @Value("${shipment.outbox.lease-seconds:60}") long leaseSeconds) {
        if (leaseSeconds <= 0 || leaseSeconds > MAX_LEASE_SECONDS)
            throw new IllegalArgumentException("shipment.outbox.lease-seconds must be between 1 and "
                    + MAX_LEASE_SECONDS);
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.leaseDuration = Duration.ofSeconds(leaseSeconds);
    }

    public Optional<Claim> claimNext() {
        return claim(null, leaseDuration);
    }

    public Optional<Claim> claim(UUID id) {
        return claim(id, leaseDuration);
    }

    Optional<Claim> claimNext(Duration duration) {
        return claim(null, duration);
    }

    private Optional<Claim> claim(UUID id, Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative())
            throw new IllegalArgumentException("Outbox lease duration must be positive");
        long leaseMicros;
        try {
            leaseMicros = duration.toNanos() / 1_000;
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("Outbox lease duration exceeds the supported range", ex);
        }
        return transaction.execute(status -> {
            String query = """
                    SELECT candidate.id, candidate.status, candidate.attempts
                    FROM outbox_events candidate
                    WHERE candidate.status IN ('PENDING', 'DLQ_PENDING')
                      AND candidate.next_attempt_at <= UTC_TIMESTAMP(6)
                      AND (candidate.lease_until IS NULL OR candidate.lease_until <= UTC_TIMESTAMP(6))
                      AND NOT EXISTS (
                          SELECT 1 FROM outbox_events earlier
                          WHERE earlier.aggregate_type = candidate.aggregate_type
                            AND earlier.aggregate_id = candidate.aggregate_id
                            AND earlier.status IN ('PENDING', 'DLQ_PENDING')
                            AND (earlier.created_at < candidate.created_at
                                 OR (earlier.created_at = candidate.created_at AND earlier.id < candidate.id))
                      )
                    """ + (id == null ? "" : " AND candidate.id = ?")
                    + " ORDER BY candidate.created_at ASC, candidate.id ASC LIMIT 1 FOR UPDATE SKIP LOCKED";
            var rows = id == null ? jdbc.query(query, (result, row) -> new ClaimCandidate(
                    uuid(result.getBytes("id")), result.getString("status"), result.getInt("attempts")))
                    : jdbc.query(query, (result, row) -> new ClaimCandidate(
                            uuid(result.getBytes("id")), result.getString("status"), result.getInt("attempts")),
                            uuidBytes(id));
            if (rows.isEmpty())
                return Optional.empty();

            ClaimCandidate candidate = rows.get(0);
            UUID token = UUID.randomUUID();
            int updated = jdbc.update("""
                    UPDATE outbox_events
                    SET lease_token = ?, lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))
                    WHERE id = ?
                    """, token.toString(), leaseMicros, uuidBytes(candidate.id()));
            if (updated != 1)
                throw new IllegalStateException("Locked outbox record could not be claimed");
            return Optional.of(new Claim(candidate.id(), token.toString(), candidate.status(), candidate.attempts()));
        });
    }

    public boolean markPublished(Claim claim) {
        return transaction.execute(status -> jdbc.update("""
                UPDATE outbox_events
                SET status = 'PUBLISHED', published_at = UTC_TIMESTAMP(6), last_error_code = NULL,
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND lease_token = ? AND lease_until > UTC_TIMESTAMP(6) AND status = 'PENDING'
                """, uuidBytes(claim.id()), claim.token()) == 1);
    }

    public boolean markPrimaryPublishFailed(Claim claim) {
        int attempts = claim.attempts() + 1;
        boolean moveToDeadLetter = attempts >= MAX_ATTEMPTS;
        return transaction.execute(status -> jdbc.update("""
                UPDATE outbox_events
                SET status = ?, attempts = ?, next_attempt_at = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                    last_error_code = 'KAFKA_PUBLISH_FAILED',
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND lease_token = ? AND lease_until > UTC_TIMESTAMP(6) AND status = 'PENDING'
                """, moveToDeadLetter ? "DLQ_PENDING" : "PENDING", moveToDeadLetter ? 0 : attempts,
                moveToDeadLetter ? 0 : 2L * attempts, uuidBytes(claim.id()), claim.token()) == 1);
    }

    public boolean markDeadLetterPublished(Claim claim) {
        return transaction.execute(status -> jdbc.update("""
                UPDATE outbox_events
                SET status = 'DEAD', published_at = UTC_TIMESTAMP(6), lease_token = NULL, lease_until = NULL
                WHERE id = ? AND lease_token = ? AND lease_until > UTC_TIMESTAMP(6) AND status = 'DLQ_PENDING'
                """, uuidBytes(claim.id()), claim.token()) == 1);
    }

    public boolean markDeadLetterPublishFailed(Claim claim) {
        int attempts = claim.attempts() + 1;
        boolean terminal = attempts >= MAX_ATTEMPTS;
        return transaction.execute(status -> jdbc.update("""
                UPDATE outbox_events
                SET status = ?, attempts = ?, next_attempt_at = TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)),
                    last_error_code = 'DLQ_PUBLISH_FAILED',
                    lease_token = NULL, lease_until = NULL
                WHERE id = ? AND lease_token = ? AND lease_until > UTC_TIMESTAMP(6) AND status = 'DLQ_PENDING'
                """, terminal ? "DEAD" : "DLQ_PENDING", attempts, 2L * attempts, uuidBytes(claim.id()),
                claim.token()) == 1);
    }

    public boolean ownsActiveLease(Claim claim, OutboxEventEntity event) {
        return claim.id().equals(event.getId())
                && claim.status().equals(event.getStatus())
                && jdbc.queryForObject("""
                        SELECT COUNT(*) FROM outbox_events
                        WHERE id = ? AND lease_token = ? AND status = ? AND lease_until > UTC_TIMESTAMP(6)
                        """, Integer.class, uuidBytes(claim.id()), claim.token(), claim.status()) == 1;
    }

    private static UUID uuid(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits())
                .array();
    }

    public record Claim(UUID id, String token, String status, int attempts) {
    }

    private record ClaimCandidate(UUID id, String status, int attempts) {
    }
}
