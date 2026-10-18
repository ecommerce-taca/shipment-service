package com.taca.shipment.event.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.common.id.ShipmentIds;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.outbox.OutboxService;
import com.taca.shipment.shipment.api.CreateShipmentRequest;
import com.taca.shipment.shipment.domain.ShipmentCommandService;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "shipment.kafka.create-consumer-enabled", havingValue = "true")
public class ShipmentCreateCommandProcessor {
    private static final Logger log = LoggerFactory.getLogger(ShipmentCreateCommandProcessor.class);
    private static final int MAX_RETRIES = 5;
    private final ShipmentCreateCommandInbox inbox;
    private final ShipmentCommandService shipments;
    private final OutboxService outbox;
    private final ObjectMapper mapper;
    private final TransactionTemplate transaction;

    public ShipmentCreateCommandProcessor(ShipmentCreateCommandInbox inbox, ShipmentCommandService shipments,
            OutboxService outbox, ObjectMapper mapper, PlatformTransactionManager transactionManager) {
        this.inbox = inbox;
        this.shipments = shipments;
        this.outbox = outbox;
        this.mapper = mapper;
        this.transaction = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${shipment.kafka.command-poll-delay-ms:250}")
    public void processBatch() {
        for (int i = 0; i < 50; i++) {
            var claim = inbox.claimNext();
            if (claim.isEmpty())
                return;
            process(claim.get());
        }
    }

    private void process(ShipmentCreateCommandInbox.Claim claim) {
        EventContext context = new EventContext(claim.correlationId(), claim.commandId());
        try {
            if (!inbox.ownsActiveLease(claim))
                return;
            CreateShipmentRequest request = mapper.treeToValue(claim.request(), CreateShipmentRequest.class);
            if (inbox.hasCancellationIntent(request.order_id())) {
                reject(claim, request, "ORDER_CANCELLED");
                return;
            }
            var result = shipments.create(claim.idempotencyKey(), request, context);
            String shipmentId = ShipmentIds.format(result.shipment().getId());
            if (result.shipment().getStatus() == ShipmentStatus.PENDING_RECONCILIATION) {
                pendingReconciliation(claim, request, shipmentId);
            } else {
                transaction.executeWithoutResult(status -> {
                    if (!inbox.complete(claim, shipmentId))
                        throw new IllegalStateException("Shipment create command lease was lost before completion");
                });
            }
        } catch (ShipmentException ex) {
            if (ex.status().is4xxClientError()) {
                reject(claim, null, ex.code());
            } else {
                retryOrDeadLetter(claim, ex.code());
            }
        } catch (RuntimeException ex) {
            retryOrDeadLetter(claim, "PROCESSING_ERROR");
        } catch (Exception ex) {
            retryOrDeadLetter(claim, "PROCESSING_ERROR");
        }
    }

    private void pendingReconciliation(ShipmentCreateCommandInbox.Claim claim, CreateShipmentRequest request,
            String shipmentId) {
        transaction.executeWithoutResult(status -> {
            if (!inbox.pendingReconciliation(claim, shipmentId))
                throw new IllegalStateException("Shipment create command lease was lost before pending update");
            outbox.append("shipment.creation_pending", "SHIPMENT", shipmentId,
                    Map.of("command_id", claim.commandId(), "shop_order_id", request.order_id(),
                            "shop_id", request.shop_id(), "shipment_id", shipmentId,
                            "status", "PENDING_RECONCILIATION", "reason_code", "CARRIER_OUTCOME_AMBIGUOUS"),
                    new EventContext(claim.correlationId(), claim.commandId()));
        });
    }

    private void reject(ShipmentCreateCommandInbox.Claim claim, CreateShipmentRequest request, String errorCode) {
        transaction.executeWithoutResult(status -> {
            String orderId = request == null ? claim.shopOrderId() : request.order_id();
            String shopId = request == null ? inbox.shopId(claim.commandId()) : request.shop_id();
            if (!inbox.reject(claim, errorCode, "Shipment create command was permanently rejected"))
                throw new IllegalStateException("Shipment create command lease was lost before rejection");
            ObjectNode payload = mapper.createObjectNode();
            payload.put("command_id", claim.commandId());
            payload.put("shop_order_id", orderId);
            if (shopId != null && !shopId.isBlank())
                payload.put("shop_id", shopId);
            payload.put("error_code", errorCode);
            payload.put("retryable", false);
            outbox.append("shipment.creation_failed", "SHIPMENT_COMMAND", claim.commandId(), payload,
                    new EventContext(claim.correlationId(), claim.commandId()));
        });
    }

    private void retryOrDeadLetter(ShipmentCreateCommandInbox.Claim claim, String errorCode) {
        if (claim.attempts() >= MAX_RETRIES) {
            boolean stored = inbox.deadLetter(claim, errorCode, "Retry limit reached; operator review required");
            if (stored)
                log.error("Shipment create command moved to durable DLQ command_id={} attempts={}",
                        claim.commandId(), claim.attempts() + 1);
            return;
        }
        long baseSeconds = Math.min(300, 1L << Math.min(claim.attempts() + 1, 8));
        long delaySeconds = Math.max(1, (long) (baseSeconds
                * ThreadLocalRandom.current().nextDouble(0.5, 1.5)));
        boolean updated = inbox.retry(claim, errorCode, "Transient processing error", delaySeconds);
        if (!updated)
            log.warn("Could not schedule retry because command lease is no longer owned command_id={}",
                    claim.commandId());
    }
}
