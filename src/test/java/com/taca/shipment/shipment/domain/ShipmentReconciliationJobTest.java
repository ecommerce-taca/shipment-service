package com.taca.shipment.shipment.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.common.id.ShipmentIds;
import com.taca.shipment.event.consumer.ShipmentCreateCommandInbox;
import com.taca.shipment.outbox.OutboxService;
import com.taca.shipment.shipment.persistence.IdempotencyRepository;
import com.taca.shipment.shipment.persistence.ShipmentEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventRepository;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ShipmentReconciliationJobTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void positiveReconciliationEmitsCurrentReducedCreatedPayloadAndDoesNotRepeatAfterRecovery() throws Exception {
        ShipmentRepository shipments = mock(ShipmentRepository.class);
        ShipmentEventRepository events = mock(ShipmentEventRepository.class);
        OutboxService outbox = mock(OutboxService.class);
        IdempotencyRepository idempotencies = mock(IdempotencyRepository.class);
        ShipmentCommandService commands = mock(ShipmentCommandService.class);
        ShipmentCreateCommandInbox createCommands = mock(ShipmentCreateCommandInbox.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
                .thenReturn(new SimpleTransactionStatus());

        CarrierAdapter adapter = mock(CarrierAdapter.class);
        when(adapter.findByIdempotencyKey("create-key"))
                .thenReturn(Optional.of(new CarrierCreateResult("external-1", "MOCK123")));
        ShipmentEntity shipment = pendingShipment();
        when(shipments.findTop100ByStatusOrderByUpdatedAtAsc(ShipmentStatus.PENDING_RECONCILIATION))
                .thenReturn(List.of(shipment), List.of());
        when(shipments.findById(shipment.getId())).thenReturn(Optional.of(shipment));
        when(shipments.saveAndFlush(any(ShipmentEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(events.save(any(ShipmentEventEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(createCommands.findByIdempotencyKey("create-key")).thenReturn(Optional.empty());
        CarrierRegistry carriers = mock(CarrierRegistry.class);
        when(carriers.require(Carrier.MOCK)).thenReturn(adapter);

        AtomicReference<String> eventType = new AtomicReference<>();
        AtomicReference<Object> eventPayload = new AtomicReference<>();
        doAnswer(invocation -> {
            eventType.set(invocation.getArgument(0));
            eventPayload.set(invocation.getArgument(3));
            return null;
        }).when(outbox).append(anyString(), anyString(), anyString(), any(), any());

        ShipmentReconciliationJob job = new ShipmentReconciliationJob(
                shipments,
                carriers,
                events,
                outbox,
                new TransactionTemplate(transactionManager),
                idempotencies,
                MAPPER,
                commands,
                createCommands);

        job.reconcile();
        job.reconcile();

        assertEquals(ShipmentStatus.CREATED, shipment.getStatus());
        assertEquals("shipment.created", eventType.get());
        assertTrue(eventPayload.get() instanceof Map<?, ?>);
        Map<?, ?> payload = (Map<?, ?>) eventPayload.get();
        assertEquals(ShipmentIds.format(shipment.getId()), payload.get("shipment_id"));
        assertEquals("order-1", payload.get("order_id"));
        assertEquals("shop-1", payload.get("shop_id"));
        assertEquals("MOCK", payload.get("carrier"));
        assertEquals("MOCK123", payload.get("tracking_code"));
        assertEquals("CREATED", payload.get("status"));
        assertEquals("CREATED", payload.get("new_status"));
        assertEquals(Boolean.TRUE, payload.get("reconciled"));

        // This payload omission is documented as a current source discrepancy, not as
        // the desired fee contract. Do not replace unknown fee data with a default.
        assertFalse(payload.containsKey("shipping_fee"));
        assertFalse(payload.containsKey("checkout_shipping_fee"));
        assertFalse(payload.containsKey("carrier_quoted_fee"));
        assertFalse(payload.containsKey("carrier_quote_delta"));
        assertFalse(payload.containsKey("carrier_actual_fee"));

        verify(outbox, times(1)).append(anyString(), anyString(), anyString(), any(), any());
        verify(adapter, times(1)).findByIdempotencyKey("create-key");
        verify(createCommands, times(1)).markReconciled(
                "order-1", "shop-1", ShipmentIds.format(shipment.getId()));
    }

    private ShipmentEntity pendingShipment() throws Exception {
        return ShipmentEntity.pending(
                "order-1",
                null,
                "shop-1",
                "buyer-1",
                "buyer@example.com",
                Carrier.MOCK,
                MAPPER.readTree("{\"line1\":\"from\"}"),
                MAPPER.readTree("{\"line1\":\"to\"}"),
                20000,
                30000L,
                20000L,
                "VND",
                0,
                null,
                null,
                "create-key");
    }
}
