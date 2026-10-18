package com.taca.shipment.shipment.domain;

import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.common.id.ShipmentIds;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.event.consumer.ShipmentCreateCommandInbox;
import com.taca.shipment.outbox.OutboxService;
import com.taca.shipment.shipment.persistence.ShipmentEventEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventRepository;
import com.taca.shipment.shipment.persistence.ShipmentEntity;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import com.taca.shipment.shipment.persistence.IdempotencyRepository;
import com.taca.shipment.shipment.persistence.IdempotencyId;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class ShipmentReconciliationJob {
    private static final Logger log = LoggerFactory.getLogger(ShipmentReconciliationJob.class);
    private final ShipmentRepository shipments;
    private final CarrierRegistry carriers;
    private final ShipmentEventRepository events;
    private final OutboxService outbox;
    private final TransactionTemplate tx;
    private final IdempotencyRepository idempotencies;
    private final ObjectMapper mapper;
    private final ShipmentCommandService commands;
    private final ShipmentCreateCommandInbox createCommands;

    public ShipmentReconciliationJob(ShipmentRepository shipments, CarrierRegistry carriers,
            ShipmentEventRepository events, OutboxService outbox, TransactionTemplate tx,
            IdempotencyRepository idempotencies, ObjectMapper mapper, ShipmentCommandService commands,
            ShipmentCreateCommandInbox createCommands) {
        this.shipments = shipments;
        this.carriers = carriers;
        this.events = events;
        this.outbox = outbox;
        this.tx = tx;
        this.idempotencies = idempotencies;
        this.mapper = mapper;
        this.commands = commands;
        this.createCommands = createCommands;
    }

    @Scheduled(fixedDelayString = "${SHIPMENT_RECONCILIATION_DELAY_MS:60000}")
    public void reconcile() {
        for (ShipmentEntity s : shipments
                .findTop100ByStatusOrderByUpdatedAtAsc(ShipmentStatus.PENDING_RECONCILIATION)) {
            try {
                var result = carriers.require(s.getCarrier()).findByIdempotencyKey(s.getCreateIdempotencyKey());
                if (result.isPresent())
                    recover(s, result.get());
            } catch (Exception ex) {
                log.warn("Shipment reconciliation lookup failed shipment_id={} carrier={}", s.getId(),
                        s.getCarrier().apiValue());
            }
        }
    }

    private void recover(ShipmentEntity candidate, CarrierCreateResult result) {
        tx.execute(status -> {
            ShipmentEntity s = shipments.findById(candidate.getId()).orElse(null);
            if (s == null || s.getStatus() != ShipmentStatus.PENDING_RECONCILIATION)
                return null;
            s.carrierCreated(result.externalShipmentId(), result.trackingCode());
            shipments.saveAndFlush(s);
            events.save(ShipmentEventEntity.create(s.getId(), s.getCarrier(), ShipmentStatus.CREATED, null,
                    "RECONCILIATION", "Đã xác nhận vận đơn tồn tại sau đối soát", null,
                    LocalDateTime.now(ZoneOffset.UTC)));
            var command = createCommands.findByIdempotencyKey(s.getCreateIdempotencyKey()).orElse(null);
            EventContext context = command == null ? EventContext.none()
                    : new EventContext(command.correlationId(), command.commandId());
            outbox.append("shipment.created", "SHIPMENT", ShipmentIds.format(s.getId()),
                    java.util.Map.of("shipment_id", ShipmentIds.format(s.getId()), "order_id", s.getOrderId(),
                            "shop_id", s.getShopId(), "carrier", s.getCarrier().apiValue(), "tracking_code",
                            s.getTrackingCode(), "status", "CREATED", "new_status", "CREATED", "reconciled", true),
                    context);
            createCommands.markReconciled(s.getOrderId(), s.getShopId(), ShipmentIds.format(s.getId()));
            idempotencies.findById(new IdempotencyId(s.getCreateIdempotencyKey(), "CREATE")).ifPresent(row -> {
                row.complete(mapper.valueToTree(commands.view(s)));
                idempotencies.save(row);
            });
            return null;
        });
    }
}
