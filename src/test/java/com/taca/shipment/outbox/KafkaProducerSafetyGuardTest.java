package com.taca.shipment.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.PropertySource;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerSafetyGuardTest {

    @Test
    void acceptsReliableProducerConfiguration() {
        assertDoesNotThrow(() -> guard(safeEnvironment(), 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsAcksThatDoNotWaitForAllReplicas() {
        MockEnvironment environment = safeEnvironment()
                .withProperty("spring.kafka.producer.acks", "1");

        assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsDisabledProducerIdempotence() {
        MockEnvironment environment = safeEnvironment()
                .withProperty("spring.kafka.producer.properties.enable.idempotence", "false");

        assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsProducerWithoutRetries() {
        MockEnvironment environment = safeEnvironment()
                .withProperty("spring.kafka.producer.retries", "0");

        assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsUnsafeMaxInFlightRequests() {
        MockEnvironment environment = safeEnvironment()
                .withProperty(
                        "spring.kafka.producer.properties.max.in.flight.requests.per.connection",
                        "6");

        assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsDeliveryTimeoutNotGreaterThanRequestTimeout() {
        MockEnvironment environment = safeEnvironment()
                .withProperty("spring.kafka.producer.properties.delivery.timeout.ms", "5000");

        assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsOutboxWaitTimeoutNotGreaterThanKafkaDeliveryTimeout() {
        assertThrows(IllegalStateException.class,
                () -> guard(safeEnvironment(), 10_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsOutboxWaitTimeoutThatCanOutliveTheLease() {
        assertThrows(IllegalStateException.class,
                () -> guard(safeEnvironment(), 60_000).afterSingletonsInstantiated());
    }

    @Test
    void rejectsMalformedProducerTimeoutWithPropertyName() {
        MockEnvironment environment = safeEnvironment()
                .withProperty("spring.kafka.producer.properties.delivery.timeout.ms", "not-a-number");

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> guard(environment, 15_000).afterSingletonsInstantiated());

        assertTrue(exception.getMessage().contains("spring.kafka.producer.properties.delivery.timeout.ms"));
    }

    @Test
    void rejectsNonPositiveSendTimeoutAndLeaseOverflow() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducerSafetyGuard(safeEnvironment(), 0, 60));
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducerSafetyGuard(
                        safeEnvironment(),
                        15_000,
                        OutboxClaimService.MAX_LEASE_SECONDS + 1));
    }

    @Test
    void acceptsProducerSettingsLoadedFromApplicationYaml() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        for (PropertySource<?> propertySource :
                new YamlPropertySourceLoader().load("shipment", new ClassPathResource("application.yml"))) {
            environment.getPropertySources().addFirst(propertySource);
        }

        long sendTimeoutMs = Long.parseLong(environment.resolveRequiredPlaceholders(
                "${shipment.kafka.send-timeout-ms}"));
        long leaseSeconds = Long.parseLong(environment.resolveRequiredPlaceholders(
                "${shipment.outbox.lease-seconds}"));

        assertEquals("all", environment.getProperty("spring.kafka.producer.acks"));
        assertEquals("true", environment.getProperty("spring.kafka.producer.properties.enable.idempotence"));
        Map<String, String> kafkaProperties = Binder.get(environment)
                .bind("spring.kafka.producer.properties", Bindable.mapOf(String.class, String.class))
                .orElseThrow(() -> new AssertionError("Kafka producer properties were not bound"));
        assertEquals("true", kafkaProperties.get("enable.idempotence"));
        assertEquals("5", kafkaProperties.get("max.in.flight.requests.per.connection"));
        assertEquals("10000", kafkaProperties.get("delivery.timeout.ms"));
        assertEquals("5000", kafkaProperties.get("request.timeout.ms"));
        assertEquals("3", environment.getProperty("spring.kafka.producer.retries"));
        assertDoesNotThrow(() -> new KafkaProducerSafetyGuard(environment, sendTimeoutMs, leaseSeconds)
                .afterSingletonsInstantiated());
    }

    private static KafkaProducerSafetyGuard guard(MockEnvironment environment, long sendTimeoutMs) {
        return new KafkaProducerSafetyGuard(environment, sendTimeoutMs, 60);
    }

    private static MockEnvironment safeEnvironment() {
        return new MockEnvironment()
                .withProperty("spring.kafka.producer.acks", "all")
                .withProperty("spring.kafka.producer.retries", "3")
                .withProperty("spring.kafka.producer.properties.enable.idempotence", "true")
                .withProperty(
                        "spring.kafka.producer.properties.max.in.flight.requests.per.connection",
                        "5")
                .withProperty("spring.kafka.producer.properties.delivery.timeout.ms", "10000")
                .withProperty("spring.kafka.producer.properties.request.timeout.ms", "5000");
    }
}
