package com.taca.shipment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.outbox.OutboxEventEntity;
import com.taca.shipment.outbox.OutboxPublisher;
import com.taca.shipment.outbox.OutboxRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "SHIPMENT_KAFKA_PUBLISHER_ENABLED=true",
                "SHIPMENT_KAFKA_CONSUMER_ENABLED=false",
                "SHIPMENT_CREATE_CONSUMER_ENABLED=true",
                "SHIPMENT_COMMAND_POLL_DELAY_MS=100",
                "OUTBOX_POLL_DELAY_MS=600000"
        })
@EmbeddedKafka(
        kraft = true,
        partitions = 1,
        topics = {
                "shipment.commands.v1",
                "shipment.events.v1",
                "shipment.consumer.dlq.v1"
        },
        bootstrapServersProperty = "KAFKA_BOOTSTRAP_SERVERS")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
class ShipmentKafkaIntegrationTest {
    private static final String COMMAND_TOPIC = "shipment.commands.v1";
    private static final String EVENT_TOPIC = "shipment.events.v1";
    private static final String CONSUMER_DLQ_TOPIC = "shipment.consumer.dlq.v1";

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shipment_kafka_test")
            .withUsername("shipment")
            .withPassword("shipment_test");

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxPublisher publisher;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", MYSQL::getJdbcUrl);
        registry.add("DB_USERNAME", MYSQL::getUsername);
        registry.add("DB_PASSWORD", MYSQL::getPassword);
    }

    @Test
    void consumesCreateCommandPersistsShipmentAndPublishesOutboxEventToKafka() throws Exception {
        String commandId = "cmd-" + UUID.randomUUID();
        String orderId = "kafka-order-" + UUID.randomUUID();
        String shopId = "kafka-shop-" + UUID.randomUUID();

        try (KafkaConsumer<String, String> consumer = consumer("shipment-events-test-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(EVENT_TOPIC));
            consumer.poll(Duration.ofMillis(250));

            kafkaTemplate.send(COMMAND_TOPIC, orderId, createCommand(commandId, orderId, shopId))
                    .get(10, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                List<String> statuses = jdbc.query(
                        "SELECT status FROM shipment_create_commands WHERE command_id=?",
                        (resultSet, row) -> resultSet.getString(1), commandId);
                assertEquals(List.of("COMPLETED"), statuses);
                assertEquals(1, jdbc.queryForObject(
                        "SELECT COUNT(*) FROM shipments WHERE order_id=? AND shop_id=?",
                        Integer.class, orderId, shopId));
            });

            String shipmentId = jdbc.queryForObject(
                    "SELECT shipment_id FROM shipment_create_commands WHERE command_id=?",
                    String.class, commandId);
            assertNotNull(shipmentId);
            assertEquals(30000L, jdbc.queryForObject(
                    "SELECT checkout_shipping_fee FROM shipments WHERE order_id=? AND shop_id=?",
                    Long.class, orderId, shopId));

            OutboxEventEntity event = outboxRepository.findAll().stream()
                    .filter(candidate -> "shipment.created".equals(candidate.getEventType())
                            && shipmentId.equals(candidate.getAggregateId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("shipment.created outbox event was not persisted"));
            assertEquals(EVENT_TOPIC, event.getTopic());
            assertEquals(shipmentId, event.getEventKey());

            publisher.publishOne(event.getId());

            ConsumerRecord<String, String> record = nextRecord(consumer, Duration.ofSeconds(15));
            assertEquals(shipmentId, record.key());
            JsonNode envelope = mapper.readTree(record.value());
            assertEquals("shipment.created", envelope.path("event_type").asText());
            assertEquals(1, envelope.path("schema_version").asInt());
            assertEquals(shipmentId, envelope.path("aggregate_id").asText());
            assertTrue(envelope.path("event_id").isTextual());
            assertEquals(shipmentId, envelope.path("payload").path("shipment_id").asText());
            assertEquals(30000L, envelope.path("payload").path("checkout_shipping_fee").asLong());
            assertEquals("PUBLISHED", jdbc.queryForObject(
                    "SELECT status FROM outbox_events WHERE aggregate_id=? AND event_type='shipment.created'",
                    String.class, shipmentId));
        }
    }

    @Test
    void routesMalformedCreateCommandToConfiguredConsumerDlq() throws Exception {
        String orderId = "invalid-kafka-order-" + UUID.randomUUID();

        try (KafkaConsumer<String, String> consumer = consumer("shipment-dlq-test-" + UUID.randomUUID())) {
            consumer.subscribe(List.of(CONSUMER_DLQ_TOPIC));
            consumer.poll(Duration.ofMillis(250));

            kafkaTemplate.send(COMMAND_TOPIC, orderId, "{invalid-json")
                    .get(10, TimeUnit.SECONDS);

            ConsumerRecord<String, String> deadLetter = nextRecord(consumer, Duration.ofSeconds(15));
            assertEquals(orderId, deadLetter.key());
            assertEquals("{invalid-json", deadLetter.value());
            Header reason = deadLetter.headers().lastHeader("shipment_dlq_reason_code");
            assertNotNull(reason);
            assertTrue(reason.value().length > 0);
            assertEquals(0, jdbc.queryForObject(
                    "SELECT COUNT(*) FROM shipment_create_commands WHERE shop_order_id=?",
                    Integer.class, orderId));
        }
    }

    private KafkaConsumer<String, String> consumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    private ConsumerRecord<String, String> nextRecord(
            KafkaConsumer<String, String> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(200));
            if (!records.isEmpty())
                return records.iterator().next();
        }
        throw new AssertionError("Expected Kafka record was not received before timeout");
    }

    private String createCommand(String commandId, String orderId, String shopId) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("command_id", commandId);
        payload.put("idempotency_key", "kafka-idempotency-" + UUID.randomUUID());
        payload.put("shop_order_id", orderId);
        payload.put("checkout_group_id", "kafka-checkout-" + UUID.randomUUID());
        payload.put("shop_id", shopId);
        payload.put("buyer", Map.of("user_id", "kafka-buyer", "email", "buyer@example.test"));
        payload.put("carrier", "MOCK");
        payload.put("from_address_snapshot", Map.of("line1", "Warehouse"));
        payload.put("to_address_snapshot", Map.of("line1", "Destination"));
        payload.put("items", List.of(Map.of(
                "sku_id", "sku-kafka",
                "title", "Kafka integration test item",
                "quantity", 1,
                "weight_gram", 500)));
        payload.put("cod_amount", 0);
        payload.put("shipping_fee", 30000);
        payload.put("payment_method", "PREPAID");
        payload.put("currency", "VND");

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("message_id", commandId);
        envelope.put("message_type", "shipment.create");
        envelope.put("schema_version", 1);
        envelope.put("occurred_at", Instant.now().toString());
        envelope.put("correlation_id", "kafka-correlation-" + UUID.randomUUID());
        envelope.put("causation_id", "kafka-causation-" + UUID.randomUUID());
        envelope.put("payload", payload);
        return mapper.writeValueAsString(envelope);
    }
}
