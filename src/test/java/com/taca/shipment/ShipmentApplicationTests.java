package com.taca.shipment;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "KAFKA_BOOTSTRAP_SERVERS=localhost:19092",
                "SHIPMENT_KAFKA_PUBLISHER_ENABLED=false",
                "SHIPMENT_KAFKA_CONSUMER_ENABLED=false",
                "SHIPMENT_CREATE_CONSUMER_ENABLED=false"
        })
@Testcontainers(disabledWithoutDocker = true)
class ShipmentApplicationTests {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shipment_context_test")
            .withUsername("shipment")
            .withPassword("shipment_test");

    @Autowired
    private ShipmentApplication application;

    @Autowired
    private JdbcTemplate jdbc;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", MYSQL::getJdbcUrl);
        registry.add("DB_USERNAME", MYSQL::getUsername);
        registry.add("DB_PASSWORD", MYSQL::getPassword);
    }

    @Test
    void startsApplicationContextAndValidatesMigratedMysqlSchema() {
        assertNotNull(application);
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history WHERE version = '006' AND success = 1",
                Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM shipments", Integer.class));
    }
}
