package com.taca.shipment.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "shipment.kafka.publisher-enabled", havingValue = "true")
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository repository;
    private final OutboxClaimService claims;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final String dlqTopic;
    private final long sendTimeoutMs;

    public OutboxPublisher(OutboxRepository repository, OutboxClaimService claims, KafkaTemplate<String, String> kafka,
            ObjectMapper mapper,
            @org.springframework.beans.factory.annotation.Value("${shipment.kafka.dlq-topic}") String dlqTopic,
            @org.springframework.beans.factory.annotation.Value("${shipment.kafka.send-timeout-ms:15000}") long sendTimeoutMs) {
        this.repository = repository;
        this.claims = claims;
        this.kafka = kafka;
        this.mapper = mapper;
        this.dlqTopic = dlqTopic;
        if (sendTimeoutMs <= 0) {
            throw new IllegalArgumentException("shipment.kafka.send-timeout-ms must be positive");
        }
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${OUTBOX_POLL_DELAY_MS:1000}")
    public void publishBatch() {
        for (int i = 0; i < 50; i++) {
            if (Thread.currentThread().isInterrupted())
                return;
            var claim = claims.claimNext();
            if (claim.isEmpty())
                return;
            if (!publish(claim.get()))
                return;
        }
    }

    public void publishOne(UUID id) {
        claims.claim(id).ifPresent(this::publish);
    }

    private boolean publish(OutboxClaimService.Claim claim) {
        var maybe = repository.findById(claim.id());
        if (maybe.isEmpty() || !claims.ownsActiveLease(claim, maybe.get()))
            return true;
        var event = maybe.get();
        if ("DLQ_PENDING".equals(event.getStatus())) {
            return publishDeadLetter(claim, event);
        }
        try {
            String payload = mapper.writeValueAsString(event.getPayload());
            kafka.send(event.getTopic(), event.getEventKey(), payload)
                    .get(sendTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Outbox publish interrupted event_id={} lease_retained=true", event.getId());
            return false;
        } catch (Exception ex) {
            boolean updated = claims.markPrimaryPublishFailed(claim);
            log.warn("Outbox publish failed event_id={} event_type={} claim_updated={}", event.getId(),
                    event.getEventType(), updated);
            return true;
        }
        if (!claims.markPublished(claim))
            log.warn("Outbox publish succeeded but claim was no longer owned event_id={}", event.getId());
        return true;
    }

    private boolean publishDeadLetter(OutboxClaimService.Claim claim, OutboxEventEntity event) {
        try {
            String payload = mapper.writeValueAsString(event.getPayload());
            kafka.send(dlqTopic, event.getEventKey(), payload)
                    .get(sendTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Outbox DLQ publish interrupted event_id={} lease_retained=true", event.getId());
            return false;
        } catch (Exception ex) {
            boolean updated = claims.markDeadLetterPublishFailed(claim);
            log.error("Outbox DLQ publish failed event_id={} claim_updated={}", event.getId(), updated);
            return true;
        }
        if (!claims.markDeadLetterPublished(claim))
            log.warn("DLQ publish succeeded but claim was no longer owned event_id={}", event.getId());
        return true;
    }
}
