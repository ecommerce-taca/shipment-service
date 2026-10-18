package com.taca.shipment.outbox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
class OutboxClaimServiceMySqlTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shipment_test")
            .withUsername("shipment")
            .withPassword("shipment_test");

    private JdbcTemplate jdbc;
    private OutboxClaimService claims;
    private PlatformTransactionManager transactionManager;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        DataSource dataSource = new DriverManagerDataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(),
                MYSQL.getPassword());
        jdbc = new JdbcTemplate(dataSource);
        transactionManager = new DataSourceTransactionManager(dataSource);
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
        claims = new OutboxClaimService(jdbc, transactionManager, 1);
        jdbc.update("DELETE FROM outbox_events");
        eventId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO outbox_events
                    (id, aggregate_type, aggregate_id, event_type, topic, event_key, payload,
                     status, attempts, next_attempt_at, created_at)
                VALUES (?, 'shipment', ?, 'shipment.created', 'shipment.events.v1', ?, JSON_OBJECT(),
                        'PENDING', 0, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, uuidBytes(eventId), eventId.toString(), eventId.toString());
    }

    @AfterEach
    void tearDown() {
        if (jdbc != null)
            jdbc.update("DELETE FROM outbox_events");
    }

    @Test
    void runsAgainstMysql84WithLeaseMigrationApplied() {
        String mysqlVersion = jdbc.queryForObject("SELECT VERSION()", String.class);
        assertTrue(mysqlVersion.startsWith("8.4."), () -> "Expected MySQL 8.4, got " + mysqlVersion);
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM flyway_schema_history
                WHERE version = '002' AND success = 1
                """, Integer.class));
        assertEquals(2, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'outbox_events'
                  AND column_name IN ('lease_token', 'lease_until')
                """, Integer.class));
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(DISTINCT index_name)
                FROM information_schema.statistics
                WHERE table_schema = DATABASE() AND table_name = 'outbox_events'
                  AND index_name = 'idx_outbox_lease_dispatch'
                """, Integer.class));
    }

    @Test
    void currencyMigrationKeepsCharThreeAndRemovesTheLegacyDefault() {
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM flyway_schema_history
                WHERE version = '004' AND success = 1
                """, Integer.class));
        assertEquals("char(3)", jdbc.queryForObject("""
                SELECT LOWER(column_type)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'currency'
                """, String.class));
        assertNull(jdbc.queryForObject("""
                SELECT column_default
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'currency'
                """, String.class));
    }

    @Test
    void feeMigrationAddsNullableExplicitFeeColumnsAndPreservesLegacyColumn() {
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM flyway_schema_history
                WHERE version = '005' AND success = 1
                """, Integer.class));
        assertEquals("bigint", jdbc.queryForObject("""
                SELECT LOWER(data_type)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'carrier_quoted_fee'
                """, String.class));
        assertEquals("YES", jdbc.queryForObject("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'carrier_quoted_fee'
                """, String.class));
        assertEquals("bigint", jdbc.queryForObject("""
                SELECT LOWER(data_type)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'checkout_shipping_fee'
                """, String.class));
        assertEquals("YES", jdbc.queryForObject("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'checkout_shipping_fee'
                """, String.class));
        assertEquals("bigint", jdbc.queryForObject("""
                SELECT LOWER(data_type)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'shipping_fee'
                """, String.class));
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM flyway_schema_history
                WHERE version = '006' AND success = 1
                """, Integer.class));
        assertEquals("bigint", jdbc.queryForObject("""
                SELECT LOWER(data_type)
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'carrier_actual_fee'
                """, String.class));
        assertEquals("YES", jdbc.queryForObject("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name = 'carrier_actual_fee'
                """, String.class));
    }

    @Test
    void concurrentPublishersOnlyClaimOneRecord() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> claimTogether(ready, start));
            var second = executor.submit(() -> claimTogether(ready, start));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            var firstResult = first.get(10, TimeUnit.SECONDS);
            var secondResult = second.get(10, TimeUnit.SECONDS);

            assertEquals(1, (firstResult.isPresent() ? 1 : 0) + (secondResult.isPresent() ? 1 : 0));
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events WHERE lease_token IS NOT NULL",
                    Integer.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void skipsRowLockedByAnotherTransactionUntilItIsReleased() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Runnable holdRowLock = () -> {
                new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    jdbc.queryForObject(
                            "SELECT id FROM outbox_events WHERE id=? FOR UPDATE",
                            byte[].class,
                            uuidBytes(eventId));
                    locked.countDown();
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while holding test row lock", ex);
                    }
                });
            };
            var lockHolder = executor.submit(holdRowLock);
            assertTrue(locked.await(5, TimeUnit.SECONDS));

            var claimWhileLocked = executor.submit(() -> {
                return claims.claimNext();
            });
            assertTrue(claimWhileLocked.get(2, TimeUnit.SECONDS).isEmpty(),
                    "Claim should skip the row locked by the other transaction");

            release.countDown();
            lockHolder.get(5, TimeUnit.SECONDS);
            assertTrue(claims.claimNext().isPresent());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void expiredLeaseCanBeReclaimedAndOldWorkerCannotUpdate() throws Exception {
        var oldClaim = claims.claimNext(Duration.ofMillis(100)).orElseThrow();
        Thread.sleep(150);
        var newClaim = claims.claimNext(Duration.ofSeconds(1)).orElseThrow();

        assertNotEquals(oldClaim.token(), newClaim.token());
        assertFalse(claims.markPublished(oldClaim));
        assertTrue(claims.markPublished(newClaim));
        assertEquals("PUBLISHED", jdbc.queryForObject(
                "SELECT status FROM outbox_events WHERE id=?", String.class, uuidBytes(eventId)));
    }

    @Test
    void rolledBackClaimLeavesNoPersistedLease() {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            assertTrue(claims.claimNext().isPresent());
            status.setRollbackOnly();
        });

        assertNull(jdbc.queryForObject(
                "SELECT lease_token FROM outbox_events WHERE id=?", String.class, uuidBytes(eventId)));
        assertNull(jdbc.queryForObject(
                "SELECT lease_until FROM outbox_events WHERE id=?", LocalDateTime.class,
                uuidBytes(eventId)));
        assertEquals("PENDING", jdbc.queryForObject(
                "SELECT status FROM outbox_events WHERE id=?", String.class, uuidBytes(eventId)));
    }

    @Test
    void retriesPrimaryTopicAndDeadLetterUsingPersistedState() {
        for (int attempt = 0; attempt < 3; attempt++) {
            var claim = claims.claimNext(Duration.ofSeconds(5)).orElseThrow();
            assertTrue(claims.markPrimaryPublishFailed(claim));
            if (attempt < 2)
                jdbc.update("UPDATE outbox_events SET next_attempt_at=UTC_TIMESTAMP(6) WHERE id=?",
                        uuidBytes(eventId));
        }

        assertEquals("DLQ_PENDING",
                jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, uuidBytes(eventId)));
        var deadLetterClaim = claims.claimNext(Duration.ofSeconds(5)).orElseThrow();
        assertTrue(claims.markDeadLetterPublishFailed(deadLetterClaim));
        assertEquals("DLQ_PENDING",
                jdbc.queryForObject("SELECT status FROM outbox_events WHERE id=?", String.class, uuidBytes(eventId)));
        assertEquals(1, jdbc.queryForObject("SELECT attempts FROM outbox_events WHERE id=?", Integer.class,
                uuidBytes(eventId)));
    }

    private java.util.Optional<OutboxClaimService.Claim> claimTogether(CountDownLatch ready, CountDownLatch start)
            throws InterruptedException {
        ready.countDown();
        assertTrue(start.await(5, TimeUnit.SECONDS));
        return claims.claimNext(Duration.ofSeconds(5));
    }

    private byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits())
                .array();
    }
}
