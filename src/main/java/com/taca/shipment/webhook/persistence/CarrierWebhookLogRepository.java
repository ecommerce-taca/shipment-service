package com.taca.shipment.webhook.persistence;
import com.taca.shipment.shipment.domain.Carrier;import org.springframework.data.jpa.repository.JpaRepository;import java.util.UUID;
public interface CarrierWebhookLogRepository extends JpaRepository<CarrierWebhookLogEntity, UUID> { boolean existsByCarrierAndExternalEventId(Carrier carrier,String externalEventId); java.util.Optional<CarrierWebhookLogEntity> findByCarrierAndExternalEventId(Carrier carrier,String externalEventId); }
