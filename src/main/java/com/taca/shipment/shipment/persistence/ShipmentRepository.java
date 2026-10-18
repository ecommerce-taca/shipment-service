package com.taca.shipment.shipment.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

public interface ShipmentRepository extends JpaRepository<ShipmentEntity, UUID> {
    Optional<ShipmentEntity> findByOrderIdAndShopId(String orderId, String shopId);

    Optional<ShipmentEntity> findByOrderId(String orderId);

    Optional<ShipmentEntity> findByTrackingCodeAndCarrier(String trackingCode,
            com.taca.shipment.shipment.domain.Carrier carrier);

    boolean existsByOrderIdAndShopId(String orderId, String shopId);

    java.util.List<ShipmentEntity> findTop100ByStatusOrderByUpdatedAtAsc(
            com.taca.shipment.shipment.domain.ShipmentStatus status);
}
