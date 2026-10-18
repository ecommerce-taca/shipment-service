package com.taca.shipment.common.error;

import org.springframework.http.HttpStatus;

public class ShipmentException extends RuntimeException {
    private final String code;
    private final HttpStatus status;
    private final Object details;

    public ShipmentException(String code, String message, HttpStatus status) {
        this(code, message, status, null);
    }

    public ShipmentException(String code, String message, HttpStatus status, Object details) {
        super(message);
        this.code = code;
        this.status = status;
        this.details = details;
    }

    public String code() {
        return code;
    }

    public HttpStatus status() {
        return status;
    }

    public Object details() {
        return details;
    }
}
