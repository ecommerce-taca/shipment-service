package com.taca.shipment.shipment.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IdempotencyRepository extends JpaRepository<IdempotencyEntity, IdempotencyId> {
}
