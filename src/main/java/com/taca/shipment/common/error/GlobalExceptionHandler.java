package com.taca.shipment.common.error;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(ShipmentException.class)
    ResponseEntity<?> shipment(ShipmentException ex, HttpServletRequest req) {
        return error(ex.status(), ex.code(), ex.getMessage(), ex.details(), req);
    }

    @ExceptionHandler({ MethodArgumentNotValidException.class, ConstraintViolationException.class,
            HttpMessageNotReadableException.class, IllegalArgumentException.class })
    ResponseEntity<?> invalid(Exception ex, HttpServletRequest req) {
        return error(HttpStatus.BAD_REQUEST, "SHIPMENT_INVALID_INPUT", "Request input is invalid", null, req);
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ResponseEntity<?> conflict(DataIntegrityViolationException ex, HttpServletRequest req) {
        String cause = ex.getMostSpecificCause() == null ? "" : String.valueOf(ex.getMostSpecificCause().getMessage());
        if (req.getRequestURI().contains("/webhooks/shipping/") && (cause.contains("uk_carrier_webhook_event")
                || cause.contains("uk_shipment_events_carrier_external"))) {
            return ResponseEntity.ok(Map.of("data", Map.of("accepted", true, "duplicate", true), "meta",
                    Map.of("request_id", traceId(req))));
        }
        if (cause.contains("uk_shipments_carrier_tracking"))
            return error(HttpStatus.CONFLICT, "SHIPMENT_TRACKING_CONFLICT",
                    "Carrier tracking code conflicts with an existing shipment", null, req);
        if (cause.contains("uk_shipments_order_shop"))
            return error(HttpStatus.CONFLICT, "SHIPMENT_ALREADY_EXISTS",
                    "A shipment already exists for this order and shop", null, req);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "SHIPMENT_INTERNAL_ERROR",
                "Database constraint rejected the operation", null, req);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<?> unexpected(Exception ex, HttpServletRequest req) {
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "SHIPMENT_INTERNAL_ERROR", "Unexpected service error", null,
                req);
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String message, Object details,
            HttpServletRequest req) {
        Map<String, Object> body = new LinkedHashMap<>();
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("code", code);
        error.put("message", message);
        error.put("details", details);
        error.put("trace_id", traceId(req));
        body.put("error", error);
        return ResponseEntity.status(status).body(body);
    }

    private String traceId(HttpServletRequest req) {
        String trace = req.getHeader("X-Trace-ID");
        if (trace != null && trace.matches("[A-Za-z0-9._-]{1,128}"))
            return trace;
        String rid = req.getHeader("X-Request-ID");
        return rid != null && rid.matches("[A-Za-z0-9._-]{1,64}") ? rid : UUID.randomUUID().toString();
    }
}
