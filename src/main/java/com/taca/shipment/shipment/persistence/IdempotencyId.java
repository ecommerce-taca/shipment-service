package com.taca.shipment.shipment.persistence;

import jakarta.persistence.Embeddable;
import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class IdempotencyId implements Serializable {
    @jakarta.persistence.Column(name = "idempotency_key", length = 128)
    private String idempotencyKey;
    @jakarta.persistence.Column(name = "operation", length = 32)
    private String operation;

    public IdempotencyId() {
    }

    public IdempotencyId(String idempotencyKey, String operation) {
        this.idempotencyKey = idempotencyKey;
        this.operation = operation;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getOperation() {
        return operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof IdempotencyId that))
            return false;
        return Objects.equals(idempotencyKey, that.idempotencyKey) && Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idempotencyKey, operation);
    }
}
