package com.taca.shipment.event.consumer;

public class PermanentShipmentMessageException extends RuntimeException {
    public PermanentShipmentMessageException(String message) {
        super(message);
    }

    public PermanentShipmentMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
