package com.taca.shipment.shipment.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface ShipmentItemRepository extends JpaRepository<ShipmentItemEntity, UUID> {
    List<ShipmentItemEntity> findByShipmentId(UUID shipmentId);

    void deleteByShipmentId(UUID shipmentId);
}
