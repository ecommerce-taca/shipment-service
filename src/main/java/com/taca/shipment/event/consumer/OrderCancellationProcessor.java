package com.taca.shipment.event.consumer;

import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.shipment.api.CancelShipmentRequest;
import com.taca.shipment.shipment.domain.ShipmentCommandService;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "shipment.kafka.consumer-enabled", havingValue = "true")
public class OrderCancellationProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderCancellationProcessor.class);
    private static final int MAX_RETRIES = 5;
    private final OrderCancellationInbox inbox;
    private final ShipmentCreateCommandInbox createCommands;
    private final ShipmentRepository shipments;
    private final ShipmentCommandService commands;

    public OrderCancellationProcessor(OrderCancellationInbox inbox, ShipmentCreateCommandInbox createCommands,
            ShipmentRepository shipments, ShipmentCommandService commands) {
        this.inbox = inbox;
        this.createCommands = createCommands;
        this.shipments = shipments;
        this.commands = commands;
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

    private void process(OrderCancellationInbox.Claim claim) {
        try {
            if (!inbox.ownsActiveLease(claim))
                return;
            var shipment = shipments.findByOrderId(claim.orderId());
            if (shipment.isEmpty()) {
                if (createCommands.hasCreateInProgress(claim.orderId())) {
                    inbox.waitForReconciliation(claim);
                    return;
                }
                complete(claim, "CANCELLED_BEFORE_SHIPMENT");
                return;
            }
            var current = shipment.get();
            if (current.getStatus() == ShipmentStatus.PENDING_RECONCILIATION) {
                inbox.waitForReconciliation(claim);
                return;
            }
            if (!current.getStatus().canCancel()) {
                complete(claim, "NOT_CANCELLABLE_AT_RECEIPT");
                return;
            }
            commands.cancel("order-cancel:" + claim.eventId(), current.getId(),
                    new CancelShipmentRequest(claim.reason(), current.getVersion()),
                    new EventContext(claim.correlationId(), claim.eventId()));
            complete(claim, null);
        } catch (ShipmentException ex) {
            var latest = shipments.findByOrderId(claim.orderId());
            if (latest.isPresent() && latest.get().getStatus() == ShipmentStatus.PENDING_RECONCILIATION) {
                inbox.waitForReconciliation(claim);
            } else if (latest.isPresent() && !latest.get().getStatus().canCancel()) {
                complete(claim, "NOT_CANCELLABLE_AT_RECEIPT");
            } else if (ex.status().is4xxClientError()) {
                boolean parked = inbox.requireOperator(claim, ex.code());
                if (parked)
                    log.error("Order cancellation requires operator review order_id={} event_id={} error_code={}",
                            claim.orderId(), claim.eventId(), ex.code());
            } else {
                retry(claim, ex.code());
            }
        } catch (RuntimeException ex) {
            retry(claim, "CANCELLATION_PROCESSING_ERROR");
        }
    }

    private void complete(OrderCancellationInbox.Claim claim, String errorCode) {
        if (!inbox.complete(claim, errorCode))
            log.warn("Order cancellation lease was lost before completion order_id={} event_id={}",
                    claim.orderId(), claim.eventId());
    }

    private void retry(OrderCancellationInbox.Claim claim, String errorCode) {
        if (claim.attempts() >= MAX_RETRIES) {
            if (inbox.requireOperator(claim, errorCode))
                log.error("Order cancellation requires operator review order_id={} event_id={} error_code={}",
                        claim.orderId(), claim.eventId(), errorCode);
            return;
        }
        long baseSeconds = Math.min(300, 1L << Math.min(claim.attempts() + 1, 8));
        long delaySeconds = Math.max(1, (long) (baseSeconds
                * ThreadLocalRandom.current().nextDouble(0.5, 1.5)));
        if (!inbox.retry(claim, errorCode, delaySeconds))
            log.warn("Could not schedule order cancellation retry order_id={} event_id={}",
                    claim.orderId(), claim.eventId());
    }
}
