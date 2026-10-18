package com.taca.shipment.carrier.domain;

/**
 * Use only when the provider outcome is ambiguous (e.g. read timeout after
 * request submission).
 */
public class CarrierTimeoutException extends RuntimeException {
    public CarrierTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public CarrierTimeoutException(String message) {
        super(message);
    }
}
