package com.taca.shipment.shipment.persistence;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Testcontainers(disabledWithoutDocker = true)
class ShipmentFeeMigrationMySqlTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shipment_fee_migration_test")
            .withUsername("shipment")
            .withPassword("shipment_test");

    @Test
    void feeMigrationsPreserveLegacyRowsAndLeaveUnknownFeesNull() {
        DataSource dataSource = new DriverManagerDataSource(MYSQL.getJdbcUrl(), MYSQL.getUsername(),
                MYSQL.getPassword());
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target("004")
                .load()
                .migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        UUID shipmentId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO shipments
                    (id, order_id, shop_id, buyer_user_id, carrier, status,
                     from_address_snapshot, to_address_snapshot, shipping_fee, currency,
                     cod_amount, create_idempotency_key, version, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'MOCK', 'CREATED', JSON_OBJECT(), JSON_OBJECT(), 30000, 'VND',
                        0, ?, 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """, uuidBytes(shipmentId), "legacy-order-" + shipmentId, "legacy-shop-" + shipmentId,
                "legacy-buyer-" + shipmentId, "legacy-key-" + shipmentId);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        assertEquals(2, jdbc.queryForObject("""
                SELECT COUNT(*)
                FROM flyway_schema_history
                WHERE version IN ('005', '006') AND success = 1
                """, Integer.class));
        assertEquals(30000L, jdbc.queryForObject(
                "SELECT shipping_fee FROM shipments WHERE id=?", Long.class, uuidBytes(shipmentId)));
        assertNull(jdbc.queryForObject(
                "SELECT checkout_shipping_fee FROM shipments WHERE id=?", Long.class, uuidBytes(shipmentId)));
        assertNull(jdbc.queryForObject(
                "SELECT carrier_quoted_fee FROM shipments WHERE id=?", Long.class, uuidBytes(shipmentId)));
        assertNull(jdbc.queryForObject(
                "SELECT carrier_actual_fee FROM shipments WHERE id=?", Long.class, uuidBytes(shipmentId)));
    }

    private byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits())
                .array();
    }
}
