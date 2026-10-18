package com.taca.shipment.shipment.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface ShipmentEventRepository extends JpaRepository<ShipmentEventEntity, UUID> {
    List<ShipmentEventEntity> findByShipmentIdOrderByOccurredAtAsc(UUID shipmentId);

    java.util.Optional<ShipmentEventEntity> findFirstByShipmentIdOrderByOccurredAtDesc(java.util.UUID shipmentId);

    boolean existsByCarrierAndExternalEventId(com.taca.shipment.shipment.domain.Carrier carrier,
            String externalEventId);
}
