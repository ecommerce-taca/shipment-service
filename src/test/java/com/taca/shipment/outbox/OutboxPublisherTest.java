package com.taca.shipment.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.outbox.OutboxClaimService.Claim;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.support.SendResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {
    private final OutboxRepository repository = mock(OutboxRepository.class);
    private final OutboxClaimService claims = mock(OutboxClaimService.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final OutboxPublisher publisher = new OutboxPublisher(repository, claims, kafka, new ObjectMapper(),
            "shipment.events.dlq.v1", 15_000);

    @Test
    void waitsLongerThanTheConfiguredKafkaDeliveryBudget() throws Exception {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "token-1", "PENDING", 0);
        OutboxEventEntity event = event(id, "PENDING", "token-1");
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, String>> future = mock(CompletableFuture.class);
        when(claims.claim(id)).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(true);
        when(kafka.send(eq("shipment.events.v1"), eq("shipment-1"), anyString())).thenReturn(future);
        when(future.get(15_000, TimeUnit.MILLISECONDS)).thenReturn(null);

        publisher.publishOne(id);

        verify(future).get(15_000, TimeUnit.MILLISECONDS);
        verify(claims).markPublished(claim);
    }

    @Test
    void interruptedKafkaWaitKeepsLeaseAndStopsTheBatch() throws Exception {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "token-1", "PENDING", 0);
        OutboxEventEntity event = event(id, "PENDING", "token-1");
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, String>> future = mock(CompletableFuture.class);
        when(claims.claimNext()).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(true);
        when(kafka.send(eq("shipment.events.v1"), eq("shipment-1"), anyString())).thenReturn(future);
        when(future.get(15_000, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("shutdown"));

        try {
            publisher.publishBatch();

            assertTrue(Thread.currentThread().isInterrupted());
            verify(claims).claimNext();
            verify(claims, never()).markPrimaryPublishFailed(claim);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedDeadLetterWaitKeepsLeaseForExpiryBasedRecovery() throws Exception {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "token-1", "DLQ_PENDING", 0);
        OutboxEventEntity event = event(id, "DLQ_PENDING", "token-1");
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, String>> future = mock(CompletableFuture.class);
        when(claims.claim(id)).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(true);
        when(kafka.send(eq("shipment.events.dlq.v1"), eq("shipment-1"), anyString())).thenReturn(future);
        when(future.get(15_000, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("shutdown"));

        try {
            publisher.publishOne(id);

            assertTrue(Thread.currentThread().isInterrupted());
            verify(claims, never()).markDeadLetterPublishFailed(claim);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void kafkaWaitTimeoutSchedulesRetryWithoutMarkingPublished() throws Exception {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "token-1", "PENDING", 0);
        OutboxEventEntity event = event(id, "PENDING", "token-1");
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, String>> future = mock(CompletableFuture.class);
        when(claims.claim(id)).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(true);
        when(kafka.send(eq("shipment.events.v1"), eq("shipment-1"), anyString())).thenReturn(future);
        when(future.get(15_000, TimeUnit.MILLISECONDS)).thenThrow(new TimeoutException("delivery timeout"));

        publisher.publishOne(id);

        verify(claims).markPrimaryPublishFailed(claim);
        verify(claims, never()).markPublished(claim);
    }

    @Test
    void schedulesDeadLetterDeliveryAfterThirdPrimaryTopicFailure() {
        UUID id = UUID.randomUUID();
        when(claims.claimNext()).thenReturn(
                Optional.of(new Claim(id, "token-1", "PENDING", 0)),
                Optional.of(new Claim(id, "token-2", "PENDING", 1)),
                Optional.of(new Claim(id, "token-3", "PENDING", 2)),
                Optional.empty());
        OutboxEventEntity event = event(id, "PENDING", "token-1");
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(any(), eq(event))).thenReturn(true);
        when(claims.markPrimaryPublishFailed(any())).thenReturn(true);
        when(kafka.send(eq("shipment.events.v1"), eq("shipment-1"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        publisher.publishBatch();

        verify(claims).markPrimaryPublishFailed(new Claim(id, "token-1", "PENDING", 0));
        verify(claims).markPrimaryPublishFailed(new Claim(id, "token-2", "PENDING", 1));
        verify(claims).markPrimaryPublishFailed(new Claim(id, "token-3", "PENDING", 2));
    }

    @Test
    void doesNotPublishWhenClaimIsNoLongerOwned() {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "stale-token", "PENDING", 0);
        OutboxEventEntity event = event(id, "PENDING", "new-token");
        when(claims.claim(id)).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(false);

        publisher.publishOne(id);

        verifyNoInteractions(kafka);
        verify(claims, never()).markPublished(claim);
    }

    @Test
    void retriesDeadLetterPublishFailure() {
        UUID id = UUID.randomUUID();
        Claim claim = new Claim(id, "token-1", "DLQ_PENDING", 0);
        OutboxEventEntity event = event(id, "DLQ_PENDING", "token-1");
        when(claims.claim(id)).thenReturn(Optional.of(claim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(claim, event)).thenReturn(true);
        when(claims.markDeadLetterPublishFailed(claim)).thenReturn(true);
        when(kafka.send(eq("shipment.events.dlq.v1"), eq("shipment-1"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        publisher.publishOne(id);

        verify(claims).markDeadLetterPublishFailed(claim);
    }

    @Test
    void databaseFailureAfterKafkaSuccessIsVisibleAndAllowsAtLeastOnceRetry() {
        UUID id = UUID.randomUUID();
        Claim firstClaim = new Claim(id, "token-1", "PENDING", 0);
        Claim retryClaim = new Claim(id, "token-2", "PENDING", 0);
        OutboxEventEntity event = event(id, "PENDING", "token-1");
        when(claims.claim(id)).thenReturn(Optional.of(firstClaim), Optional.of(retryClaim));
        when(repository.findById(id)).thenReturn(Optional.of(event));
        when(claims.ownsActiveLease(any(), eq(event))).thenReturn(true);
        when(kafka.send(eq("shipment.events.v1"), eq("shipment-1"), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(claims.markPublished(firstClaim))
                .thenThrow(new DataAccessResourceFailureException("database unavailable"));
        when(claims.markPublished(retryClaim)).thenReturn(true);

        assertThrows(DataAccessResourceFailureException.class, () -> publisher.publishOne(id));
        publisher.publishOne(id);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq("shipment.events.v1"), eq("shipment-1"), payloadCaptor.capture());
        assertEquals(payloadCaptor.getAllValues().get(0), payloadCaptor.getAllValues().get(1));
        verify(claims).markPublished(retryClaim);
    }

    private OutboxEventEntity event(UUID id, String status, String token) {
        OutboxEventEntity event = mock(OutboxEventEntity.class);
        when(event.getId()).thenReturn(id);
        when(event.getStatus()).thenReturn(status);
        when(event.getLeaseToken()).thenReturn(token);
        when(event.getLeaseUntil()).thenReturn(LocalDateTime.now(ZoneOffset.UTC).plusSeconds(30));
        when(event.getPayload()).thenReturn(new ObjectMapper().createObjectNode());
        when(event.getTopic()).thenReturn("shipment.events.v1");
        when(event.getEventKey()).thenReturn("shipment-1");
        when(event.getEventType()).thenReturn("shipment.created");
        return event;
    }
}
