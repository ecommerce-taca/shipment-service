package com.taca.shipment.shipment.api;

import com.taca.shipment.common.api.ApiResponse;
import com.taca.shipment.common.id.ShipmentIds;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.domain.ShipmentCommandService;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import com.taca.shipment.shipment.persistence.ShipmentEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventEntity;
import com.taca.shipment.shipment.persistence.ShipmentEventRepository;
import com.taca.shipment.shipment.persistence.ShipmentRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/v1")
public class ShipmentController {
    private final ShipmentRepository shipments;
    private final ShipmentEventRepository events;
    private final ShipmentCommandService commands;

    public ShipmentController(ShipmentRepository shipments, ShipmentEventRepository events,
            ShipmentCommandService commands) {
        this.shipments = shipments;
        this.events = events;
        this.commands = commands;
    }

    @PostMapping("/internal/shipments")
    public ResponseEntity<ApiResponse<Map<String, Object>>> create(@RequestHeader("Idempotency-Key") String key,
            @Valid @RequestBody CreateShipmentRequest req,
            @RequestHeader(value = "X-Request-ID", required = false) String rid) {
        var result = commands.create(key, req);
        Map<String, Object> view = commands.view(result.shipment());
        return ResponseEntity.status(result.status()).body(ApiResponse.of(view, requestId(rid)));
    }

    @GetMapping("/orders/{orderId}/shipment")
    public ApiResponse<Map<String, Object>> buyer(@PathVariable String orderId,
            @RequestHeader("X-User-ID") String userId,
            @RequestHeader(value = "X-Request-ID", required = false) String rid) {
        ShipmentEntity shipment = shipments.findByOrderId(orderId).orElseThrow(this::notFound);
        if (!shipment.getBuyerUserId().equals(userId))
            throw forbidden();
        return ApiResponse.of(fullView(shipment), requestId(rid));
    }

    @GetMapping("/seller/orders/{orderId}/shipment")
    public ApiResponse<Map<String, Object>> seller(@PathVariable String orderId,
            @RequestHeader(value = "X-User-Shop-Scope", required = false) String scope,
            @RequestHeader(value = "X-User-Permissions", required = false) String permissions,
            @RequestHeader(value = "X-Request-ID", required = false) String rid) {
        ShipmentEntity shipment = shipments.findByOrderId(orderId).orElseThrow(this::notFound);
        if (!containsShop(scope, shipment.getShopId())
                && (permissions == null || !permissions.contains("shipment:read:any")))
            throw forbidden();
        return ApiResponse.of(fullView(shipment), requestId(rid));
    }

    @PostMapping("/internal/shipments/{shipmentId}/cancel")
    public ApiResponse<Map<String, Object>> cancel(@PathVariable String shipmentId,
            @RequestHeader("Idempotency-Key") String key, @Valid @RequestBody CancelShipmentRequest req,
            @RequestHeader(value = "X-Request-ID", required = false) String rid) {
        UUID parsedId;
        try {
            parsedId = ShipmentIds.parse(shipmentId);
        } catch (IllegalArgumentException ex) {
            throw notFound();
        }
        ShipmentEntity shipment = commands.cancel(key, parsedId, req);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("shipment_id", ShipmentIds.format(shipment.getId()));
        result.put("status", shipment.getStatus().name());
        result.put("reason", req.reason());
        result.put("version", shipment.getVersion());
        return ApiResponse.of(result, requestId(rid));
    }

    private Map<String, Object> fullView(ShipmentEntity s) {
        Map<String, Object> view = new LinkedHashMap<>(commands.view(s));
        List<Map<String, Object>> timeline = events.findByShipmentIdOrderByOccurredAtAsc(s.getId()).stream()
                .map(this::eventView).toList();
        view.put("timeline", timeline);
        return view;
    }

    private Map<String, Object> eventView(ShipmentEventEntity e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", e.getCanonicalStatus().name());
        m.put("occurred_at", e.getOccurredAt().toInstant(java.time.ZoneOffset.UTC).toString());
        m.put("source", e.getSource());
        m.put("note", e.getDescriptionSafe());
        return m;
    }

    private boolean containsShop(String raw, String shopId) {
        if (raw == null || raw.isBlank())
            return false;
        String normalized = raw.replace('[', ' ').replace(']', ' ').replace('"', ' ').replace('\'', ' ');
        return Arrays.stream(normalized.split("[,;\\s]+")).anyMatch(shopId::equals);
    }

    private String requestId(String value) {
        return value == null || value.isBlank() ? UUID.randomUUID().toString() : value;
    }

    private ShipmentException notFound() {
        return new ShipmentException("SHIPMENT_NOT_FOUND", "Shipment not found", HttpStatus.NOT_FOUND);
    }

    private ShipmentException forbidden() {
        return new ShipmentException("SHIPMENT_FORBIDDEN", "Shipment is outside the current user's scope",
                HttpStatus.FORBIDDEN);
    }
}
