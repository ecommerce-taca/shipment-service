package com.taca.shipment.outbox;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "shipment.kafka.publisher-enabled", havingValue = "true")
public class KafkaProducerSafetyGuard implements SmartInitializingSingleton {

    private final Environment environment;
    private final long sendTimeoutMs;
    private final long leaseDurationMs;

    public KafkaProducerSafetyGuard(
            Environment environment,
            @org.springframework.beans.factory.annotation.Value("${shipment.kafka.send-timeout-ms:15000}") long sendTimeoutMs,
            @org.springframework.beans.factory.annotation.Value("${shipment.outbox.lease-seconds:60}") long leaseSeconds) {
        this.environment = environment;
        if (sendTimeoutMs <= 0) {
            throw new IllegalArgumentException("shipment.kafka.send-timeout-ms must be positive");
        }
        if (leaseSeconds <= 0 || leaseSeconds > OutboxClaimService.MAX_LEASE_SECONDS) {
            throw new IllegalArgumentException(
                    "shipment.outbox.lease-seconds must be between 1 and "
                            + OutboxClaimService.MAX_LEASE_SECONDS);
        }
        this.sendTimeoutMs = sendTimeoutMs;
        this.leaseDurationMs = Math.multiplyExact(leaseSeconds, 1_000L);
    }

    @Override
    public void afterSingletonsInstantiated() {
        String acks = required("spring.kafka.producer.acks");
        boolean idempotenceEnabled = Boolean.parseBoolean(required(
                "spring.kafka.producer.properties.enable.idempotence",
                "spring.kafka.producer.properties[enable.idempotence]"));
        int retries = intValue("spring.kafka.producer.retries");
        int maxInFlight = intValue(
                "spring.kafka.producer.properties.max.in.flight.requests.per.connection",
                "spring.kafka.producer.properties[max.in.flight.requests.per.connection]");
        long deliveryTimeoutMs = longValue(
                "spring.kafka.producer.properties.delivery.timeout.ms",
                "spring.kafka.producer.properties[delivery.timeout.ms]");
        long requestTimeoutMs = longValue(
                "spring.kafka.producer.properties.request.timeout.ms",
                "spring.kafka.producer.properties[request.timeout.ms]");

        if (!"all".equalsIgnoreCase(acks)) {
            throw new IllegalStateException("Kafka producer must use acks=all for outbox publishing");
        }
        if (!idempotenceEnabled) {
            throw new IllegalStateException("Kafka producer must enable idempotence for outbox publishing");
        }
        if (retries < 1) {
            throw new IllegalStateException("Kafka producer retries must be >= 1 for outbox publishing");
        }
        if (maxInFlight < 1 || maxInFlight > 5) {
            throw new IllegalStateException(
                    "Kafka producer max.in.flight.requests.per.connection must be between 1 and 5");
        }
        if (requestTimeoutMs <= 0 || deliveryTimeoutMs <= 0) {
            throw new IllegalStateException("Kafka producer timeouts must be positive");
        }
        if (deliveryTimeoutMs <= requestTimeoutMs) {
            throw new IllegalStateException(
                    "Kafka producer delivery.timeout.ms must be greater than request.timeout.ms");
        }
        if (sendTimeoutMs <= deliveryTimeoutMs) {
            throw new IllegalStateException(
                    "Outbox send-timeout-ms must be greater than Kafka delivery.timeout.ms");
        }
        if (sendTimeoutMs >= leaseDurationMs) {
            throw new IllegalStateException(
                    "Outbox send-timeout-ms must be less than the outbox lease duration");
        }
    }

    private int intValue(String... keys) {
        String key = keys[0];
        try {
            return Integer.parseInt(required(keys));
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid integer Kafka producer property: " + key, ex);
        }
    }

    private long longValue(String... keys) {
        String key = keys[0];
        try {
            return Long.parseLong(required(keys));
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Invalid long Kafka producer property: " + key, ex);
        }
    }

    private String required(String... keys) {
        for (String key : keys) {
            String value = environment.getProperty(key);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        throw new IllegalStateException("Missing required Kafka producer property: " + keys[0]);
    }
}
