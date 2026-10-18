package com.taca.shipment.shipment.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.CarrierTimeoutException;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.common.id.ShipmentIds;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.outbox.OutboxService;
import com.taca.shipment.shipment.api.CancelShipmentRequest;
import com.taca.shipment.shipment.api.CreateShipmentRequest;
import com.taca.shipment.shipment.persistence.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

@Service
public class ShipmentCommandService {
    private final ShipmentRepository shipments;
    private final ShipmentItemRepository items;
    private final ShipmentEventRepository events;
    private final IdempotencyRepository idempotencies;
    private final CarrierRegistry carriers;
    private final ObjectMapper mapper;
    private final OutboxService outbox;
    private final TransactionTemplate tx;
    private final String environment;
    private final boolean mockEnabled;

    public ShipmentCommandService(ShipmentRepository shipments, ShipmentItemRepository items,
            ShipmentEventRepository events,
            IdempotencyRepository idempotencies, CarrierRegistry carriers, ObjectMapper mapper,
            OutboxService outbox, TransactionTemplate tx,
            @org.springframework.beans.factory.annotation.Value("${shipment.env:local}") String environment,
            @org.springframework.beans.factory.annotation.Value("${shipment.carrier.mock-enabled:true}") boolean mockEnabled) {
        this.shipments = shipments;
        this.items = items;
        this.events = events;
        this.idempotencies = idempotencies;
        this.carriers = carriers;
        this.mapper = mapper;
        this.outbox = outbox;
        this.tx = tx;
        this.environment = environment;
        this.mockEnabled = mockEnabled;
    }

    public CreateResult create(String key, CreateShipmentRequest req) {
        return create(key, req, EventContext.none());
    }

    public CreateResult create(String key, CreateShipmentRequest req, EventContext eventContext) {
        EventContext context = eventContext == null ? EventContext.none() : eventContext;
        if (key == null || key.isBlank() || key.length() > 128)
            throw invalid("Idempotency-Key is required and must be at most 128 characters");
        validateCreate(req);
        String hash = hash(req);
        IdempotencyId idemId = new IdempotencyId(key, "CREATE");
        Optional<IdempotencyEntity> idem = idempotencies.findById(idemId);
        if (idem.isPresent() && idem.get().getExpiresAt().isBefore(now())) {
            idempotencies.delete(idem.get());
            idem = Optional.empty();
        }
        if (idem.isPresent()) {
            IdempotencyEntity prior = idem.get();
            if (!prior.getRequestHash().equals(hash))
                throw new ShipmentException("SHIPMENT_IDEMPOTENCY_CONFLICT",
                        "Idempotency-Key was already used with a different request", HttpStatus.CONFLICT);
            if (prior.getShipmentId() != null) {
                ShipmentEntity existing = shipments.findById(prior.getShipmentId()).orElseThrow(() -> notFound());
                return new CreateResult(existing,
                        "COMPLETED".equals(prior.getState()) ? HttpStatus.OK : HttpStatus.ACCEPTED, true);
            }
        }
        Optional<ShipmentEntity> sameOrder = shipments.findByOrderIdAndShopId(req.order_id(), req.shop_id());
        if (sameOrder.isPresent()) {
            ShipmentEntity existing = sameOrder.get();
            return new CreateResult(existing,
                    existing.getStatus() == ShipmentStatus.PENDING_RECONCILIATION ? HttpStatus.ACCEPTED : HttpStatus.OK,
                    true);
        }

        Carrier carrier;
        try {
            carrier = Carrier.fromApi(req.carrier());
        } catch (IllegalArgumentException ex) {
            throw invalid("Unsupported carrier");
        }
        if (carrier == Carrier.MOCK
                && (!mockEnabled || !java.util.Set.of("local", "test", "staging").contains(environment.toLowerCase())))
            throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE_FOR_ORDER",
                    "MOCK carrier is disabled outside local/test/staging", HttpStatus.BAD_REQUEST);
        CarrierAdapter adapter = carriers.require(carrier);
        List<JsonNode> itemNodes = req.items().stream().map(item -> (JsonNode) mapper.valueToTree(item)).toList();
        ShippingQuote currentQuote = adapter.quote(req.from_address_snapshot(), req.to_address_snapshot(), itemNodes);
        if (currentQuote == null)
            throw new ShipmentException("SHIPMENT_CARRIER_INVALID_QUOTE",
                    "Carrier returned no quote response", HttpStatus.BAD_GATEWAY);
        try {
            if (Carrier.fromApi(currentQuote.carrier()) != carrier)
                throw new ShipmentException("SHIPMENT_CARRIER_MISMATCH",
                        "Carrier quote does not match the selected carrier", HttpStatus.BAD_GATEWAY);
        } catch (IllegalArgumentException ex) {
            throw new ShipmentException("SHIPMENT_CARRIER_INVALID_QUOTE",
                    "Carrier quote identifies an unsupported carrier", HttpStatus.BAD_GATEWAY);
        }
        if (!currentQuote.available())
            throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE_FOR_ORDER",
                    "Selected carrier is unavailable for this shipment", HttpStatus.BAD_REQUEST);
        if (currentQuote.expiresAt() != null && !currentQuote.expiresAt().isAfter(Instant.now()))
            throw new ShipmentException("SHIPMENT_CARRIER_QUOTE_EXPIRED",
                    "Carrier returned an expired quote", HttpStatus.BAD_GATEWAY);
        if (currentQuote.fee() != null && currentQuote.fee() < 0)
            throw new ShipmentException("SHIPMENT_CARRIER_INVALID_QUOTE",
                    "Carrier returned a negative quote fee", HttpStatus.BAD_GATEWAY);
        if (currentQuote.fee() != null && !req.currency().equals(currentQuote.currency()))
            throw new ShipmentException("SHIPMENT_CURRENCY_MISMATCH",
                    "Carrier quote currency does not match requested shipment currency", HttpStatus.BAD_REQUEST);
        long legacyEffectiveFee = currentQuote.fee() == null ? req.shipping_fee() : currentQuote.fee();

        // Persist the idempotency reservation before calling the provider. If the
        // process crashes after the
        // provider accepts the request, the row remains PENDING_RECONCILIATION instead
        // of creating blindly again.
        ShipmentEntity reserved = reservePending(key, hash, req, carrier, legacyEffectiveFee, currentQuote.fee(),
                req.currency(), currentQuote.minDays(), currentQuote.maxDays());
        if (!key.equals(reserved.getCreateIdempotencyKey())
                || reserved.getStatus() != ShipmentStatus.PENDING_RECONCILIATION)
            return new CreateResult(reserved,
                    reserved.getStatus() == ShipmentStatus.PENDING_RECONCILIATION ? HttpStatus.ACCEPTED : HttpStatus.OK,
                    true);
        CarrierCreateResult external;
        try {
            external = adapter.create(key, req.order_id(), carrierRequest(req, currentQuote.fee()));
        } catch (ShipmentException ex) {
            if (isAmbiguousCarrierFailure(ex)) {
                markPendingReconciliation(reserved.getId(), key, context);
                return new CreateResult(shipments.findById(reserved.getId()).orElse(reserved), HttpStatus.ACCEPTED,
                        false);
            }
            cleanupReservation(reserved.getId(), key);
            throw ex;
        } catch (CarrierTimeoutException ex) {
            markPendingReconciliation(reserved.getId(), key, context);
            return new CreateResult(shipments.findById(reserved.getId()).orElse(reserved), HttpStatus.ACCEPTED, false);
        } catch (RuntimeException ex) {
            // Unknown provider outcome is ambiguous. Preserve reservation for
            // reconciliation; never blind retry.
            markPendingReconciliation(reserved.getId(), key, context);
            return new CreateResult(shipments.findById(reserved.getId()).orElse(reserved), HttpStatus.ACCEPTED, false);
        }
        ShipmentEntity saved = tx.execute(status -> {
            ShipmentEntity shipment = shipments.findById(reserved.getId()).orElseThrow(() -> notFound());
            if (shipment.getStatus() != ShipmentStatus.PENDING_RECONCILIATION || shipment.getTrackingCode() != null)
                return shipment;
            shipment.carrierCreated(external.externalShipmentId(), external.trackingCode());
            shipments.saveAndFlush(shipment);
            events.save(ShipmentEventEntity.create(shipment.getId(), carrier, ShipmentStatus.CREATED, null, "SYSTEM",
                    "Đã tạo vận đơn", null, now()));
            outbox.append("shipment.created", "SHIPMENT", ShipmentIds.format(shipment.getId()),
                    eventPayload(shipment, null, ShipmentStatus.CREATED, "SYSTEM", null,
                            now().toInstant(ZoneOffset.UTC)),
                    context);
            IdempotencyEntity row = idempotencies.findById(idemId).orElseThrow();
            row.complete(mapper.valueToTree(view(shipment)));
            idempotencies.save(row);
            return shipment;
        });
        return new CreateResult(saved, HttpStatus.CREATED, false);
    }

    private ShipmentEntity reservePending(String key, String hash, CreateShipmentRequest req, Carrier carrier,
            long legacyEffectiveFee, Long carrierQuotedFee, String currency, Integer minDays, Integer maxDays) {
        return tx.execute(status -> {
            Optional<ShipmentEntity> existing = shipments.findByOrderIdAndShopId(req.order_id(), req.shop_id());
            if (existing.isPresent())
                return existing.get();
            ShipmentEntity shipment = ShipmentEntity.pending(req.order_id(), req.checkout_group_id(), req.shop_id(),
                    req.buyer_user_id(), req.buyer_email(), carrier, req.from_address_snapshot(),
                    req.to_address_snapshot(), legacyEffectiveFee, req.shipping_fee(), carrierQuotedFee, currency,
                    req.cod_amount(), minDays, maxDays, key);
            shipments.saveAndFlush(shipment);
            persistItems(shipment, req);
            idempotencies.save(IdempotencyEntity.processing(key, "CREATE", hash, shipment.getId()));
            return shipment;
        });
    }

    private void markPendingReconciliation(UUID shipmentId, String key, EventContext context) {
        tx.execute(status -> {
            ShipmentEntity shipment = shipments.findById(shipmentId).orElseThrow(() -> notFound());
            boolean alreadyRecorded = events.findByShipmentIdOrderByOccurredAtAsc(shipmentId).stream()
                    .anyMatch(e -> e.getCanonicalStatus() == ShipmentStatus.PENDING_RECONCILIATION);
            if (!alreadyRecorded) {
                events.save(ShipmentEventEntity.create(shipment.getId(), shipment.getCarrier(),
                        ShipmentStatus.PENDING_RECONCILIATION, null, "SYSTEM", "Cần đối soát kết quả tạo vận đơn", null,
                        now()));
                outbox.append("shipment.status_changed", "SHIPMENT", ShipmentIds.format(shipment.getId()),
                        eventPayload(shipment, null, ShipmentStatus.PENDING_RECONCILIATION, "SYSTEM",
                                "CARRIER_OUTCOME_AMBIGUOUS", now().toInstant(ZoneOffset.UTC)),
                        context);
            }
            IdempotencyEntity row = idempotencies.findById(new IdempotencyId(key, "CREATE")).orElseThrow();
            row.markPending();
            idempotencies.save(row);
            return null;
        });
    }

    private void cleanupReservation(UUID shipmentId, String key) {
        tx.execute(status -> {
            items.deleteByShipmentId(shipmentId);
            shipments.deleteById(shipmentId);
            idempotencies.deleteById(new IdempotencyId(key, "CREATE"));
            return null;
        });
    }

    public ShipmentEntity cancel(String key, UUID shipmentId, CancelShipmentRequest req) {
        return cancel(key, shipmentId, req, EventContext.none());
    }

    public ShipmentEntity cancel(String key, UUID shipmentId, CancelShipmentRequest req, EventContext eventContext) {
        EventContext context = eventContext == null ? EventContext.none() : eventContext;
        if (key == null || key.isBlank() || key.length() > 128)
            throw invalid("Idempotency-Key is required");
        if (req == null || req.reason() == null || req.reason().isBlank() || req.reason().length() > 255
                || req.version() < 0)
            throw invalid("Cancellation reason and version are invalid");
        String hash = hash(Map.of("shipment_id", ShipmentIds.format(shipmentId), "reason", req.reason(), "version",
                req.version()));
        IdempotencyId id = new IdempotencyId(key, "CANCEL");
        Optional<IdempotencyEntity> previous = idempotencies.findById(id);
        if (previous.isPresent() && previous.get().getExpiresAt().isBefore(now())) {
            idempotencies.delete(previous.get());
            previous = Optional.empty();
        }
        if (previous.isPresent()) {
            if (!previous.get().getRequestHash().equals(hash))
                throw new ShipmentException("SHIPMENT_IDEMPOTENCY_CONFLICT",
                        "Idempotency-Key was used with a different cancel request", HttpStatus.CONFLICT);
            if (previous.get().getShipmentId() != null)
                return shipments.findById(previous.get().getShipmentId()).orElseThrow(() -> notFound());
        }
        ShipmentEntity current = shipments.findById(shipmentId).orElseThrow(() -> notFound());
        if (current.getVersion() != req.version())
            throw new ShipmentException("SHIPMENT_STATE_INVALID", "Shipment version conflict", HttpStatus.CONFLICT);
        if (!current.getStatus().canCancel())
            throw new ShipmentException("SHIPMENT_STATE_INVALID", "Shipment can only be cancelled before pickup",
                    HttpStatus.CONFLICT);
        CarrierAdapter adapter = carriers.require(current.getCarrier());
        adapter.cancel(current.getExternalShipmentId(), req.reason(), key);
        return tx.execute(status -> {
            ShipmentEntity shipment = shipments.findById(shipmentId).orElseThrow(() -> notFound());
            if (shipment.getVersion() != req.version())
                throw new ShipmentException("SHIPMENT_STATE_INVALID", "Shipment version conflict", HttpStatus.CONFLICT);
            ShipmentStatus old = shipment.getStatus();
            shipment.changeStatus(ShipmentStatus.CANCELLED);
            shipments.saveAndFlush(shipment);
            events.save(ShipmentEventEntity.create(shipment.getId(), shipment.getCarrier(), ShipmentStatus.CANCELLED,
                    null, "SYSTEM", "Vận đơn đã hủy", null, now()));
            outbox.append("shipment.cancelled", "SHIPMENT", ShipmentIds.format(shipment.getId()), eventPayload(shipment,
                    old, ShipmentStatus.CANCELLED, "SYSTEM", req.reason(), now().toInstant(ZoneOffset.UTC)), context);
            IdempotencyEntity row = IdempotencyEntity.processing(key, "CANCEL", hash, shipment.getId());
            row.complete(mapper.valueToTree(view(shipment)));
            idempotencies.save(row);
            return shipment;
        });
    }

    public ShipmentEntity applyCarrierStatus(ShipmentEntity shipment, ShipmentStatus next, String carrierStatus,
            String externalEventId, Instant occurredAt, String note) {
        ShipmentStatus old = shipment.getStatus();
        if (!ShipmentStateMachine.canTransition(old, next))
            return shipment;
        shipment.changeStatus(next);
        shipments.saveAndFlush(shipment);
        events.save(ShipmentEventEntity.create(shipment.getId(), shipment.getCarrier(), next, carrierStatus, "CARRIER",
                note, externalEventId,
                occurredAt == null ? now() : LocalDateTime.ofInstant(occurredAt, ZoneOffset.UTC)));
        if (old != next) {
            String eventType = switch (next) {
                case DELIVERED -> "shipment.delivered";
                case FAILED -> "shipment.failed";
                case CANCELLED -> "shipment.cancelled";
                default -> "shipment.status_changed";
            };
            outbox.append(eventType, "SHIPMENT", ShipmentIds.format(shipment.getId()),
                    eventPayload(shipment, old, next, "CARRIER",
                            note, occurredAt == null ? Instant.now() : occurredAt));
        }
        return shipment;
    }

    private void validateCreate(CreateShipmentRequest req) {
        if (req == null || req.currency() == null || !req.currency().matches("[A-Z]{3}"))
            throw invalid("currency must be a three-letter uppercase currency code");
        validateAddress(req.from_address_snapshot());
        validateAddress(req.to_address_snapshot());
        if (req.items() == null || req.items().isEmpty() || req.items().size() > 100)
            throw invalid("Shipment must contain 1 to 100 item rows");
        if (req.cod_amount() > 0 && (req.payment_method() == null || !req.payment_method().equalsIgnoreCase("COD")))
            throw invalid("cod_amount must be zero unless payment_method is COD");
        if (req.shipping_fee() < 0 || req.cod_amount() < 0)
            throw invalid("Amounts cannot be negative");
    }

    private boolean isAmbiguousCarrierFailure(ShipmentException ex) {
        return "SHIPMENT_CARRIER_TIMEOUT".equals(ex.code())
                || ex.status().is5xxServerError()
                || ex.status() == HttpStatus.REQUEST_TIMEOUT
                || ex.status() == HttpStatus.CONFLICT
                || ex.status() == HttpStatus.TOO_EARLY
                || ex.status() == HttpStatus.TOO_MANY_REQUESTS;
    }

    private void validateAddress(JsonNode address) {
        if (address == null || !address.isObject())
            throw invalid("Address snapshot must be a JSON object");
        if (address.toString().getBytes(StandardCharsets.UTF_8).length > 16384)
            throw invalid("Address snapshot exceeds 16 KiB");
    }

    private JsonNode carrierRequest(CreateShipmentRequest req, Long carrierQuotedFee) {
        ObjectNode n = mapper.createObjectNode();
        n.put("order_id", req.order_id());
        n.put("shop_id", req.shop_id());
        n.put("idempotency_key", "shipment-create:" + req.order_id() + ":" + req.shop_id());
        n.put("currency", req.currency());
        n.put("checkout_shipping_fee", req.shipping_fee());
        if (carrierQuotedFee == null)
            n.putNull("carrier_quoted_fee");
        else
            n.put("carrier_quoted_fee", carrierQuotedFee);
        n.set("from_address_snapshot", req.from_address_snapshot());
        n.set("to_address_snapshot", req.to_address_snapshot());
        n.set("items", mapper.valueToTree(req.items()));
        n.put("cod_amount", req.cod_amount());
        return n;
    }

    private void persistItems(ShipmentEntity shipment, CreateShipmentRequest req) {
        for (var item : req.items())
            items.save(ShipmentItemEntity.create(shipment.getId(), item.sku_id(), item.product_id(), item.title(),
                    item.quantity(), item.weight_gram()));
    }

    private Map<String, Object> eventPayload(ShipmentEntity s, ShipmentStatus old, ShipmentStatus next, String source,
            String reason, Instant eventTime) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("shipment_id", ShipmentIds.format(s.getId()));
        p.put("order_id", s.getOrderId());
        p.put("shop_id", s.getShopId());
        p.put("carrier", s.getCarrier().apiValue());
        p.put("tracking_code", s.getTrackingCode());
        p.put("status", next.name());
        p.put("old_status", old == null ? null : old.name());
        p.put("new_status", next.name());
        p.put("source", source);
        p.put("occurred_at", (eventTime == null ? Instant.now() : eventTime).toString());
        p.put("shipping_fee", s.getShippingFee());
        p.put("checkout_shipping_fee", s.getCheckoutShippingFee());
        p.put("carrier_quoted_fee", s.getCarrierQuotedFee());
        if (s.getCarrierQuotedFee() != null && s.getCheckoutShippingFee() != null)
            p.put("carrier_quote_delta", s.getCarrierQuotedFee() - s.getCheckoutShippingFee());
        if (s.getCarrierActualFee() != null)
            p.put("carrier_actual_fee", s.getCarrierActualFee());
        p.put("currency", s.getCurrency());
        if (reason != null)
            p.put("reason", reason);
        if (next == ShipmentStatus.DELIVERED)
            p.put("delivered_at", (eventTime == null ? Instant.now() : eventTime).toString());
        if (next == ShipmentStatus.DELIVERED || next == ShipmentStatus.FAILED) {
            Map<String, Object> buyer = new LinkedHashMap<>();
            buyer.put("user_id", s.getBuyerUserId());
            buyer.put("email", s.getBuyerEmail());
            p.put("buyer", buyer);
        }
        return p;
    }

    public Map<String, Object> view(ShipmentEntity s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("shipment_id", ShipmentIds.format(s.getId()));
        m.put("order_id", s.getOrderId());
        m.put("shop_id", s.getShopId());
        m.put("carrier", s.getCarrier().apiValue());
        m.put("tracking_code", s.getTrackingCode());
        m.put("status", s.getStatus().name());
        m.put("shipping_fee", s.getShippingFee());
        m.put("checkout_shipping_fee", s.getCheckoutShippingFee());
        m.put("carrier_quoted_fee", s.getCarrierQuotedFee());
        if (s.getCarrierQuotedFee() != null && s.getCheckoutShippingFee() != null)
            m.put("carrier_quote_delta", s.getCarrierQuotedFee() - s.getCheckoutShippingFee());
        if (s.getCarrierActualFee() != null)
            m.put("carrier_actual_fee", s.getCarrierActualFee());
        m.put("currency", s.getCurrency());
        m.put("estimated_delivery",
                s.getEstimatedDeliveryMinDate() == null ? null
                        : Map.of("min_date", s.getEstimatedDeliveryMinDate().toString(), "max_date",
                                s.getEstimatedDeliveryMaxDate().toString()));
        m.put("version", s.getVersion());
        m.put("created_at", s.getCreatedAt().toInstant(ZoneOffset.UTC).toString());
        return m;
    }

    private String hash(Object value) {
        try {
            JsonNode node = mapper.readTree(mapper.writeValueAsBytes(value));
            byte[] bytes = mapper.writeValueAsBytes(canonical(node));
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            return HexFormat.of().formatHex(digest);
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot hash request", ex);
        }
    }

    private JsonNode canonical(JsonNode node) {
        if (node == null || node.isValueNode())
            return node;
        if (node.isArray()) {
            var array = mapper.createArrayNode();
            node.forEach(child -> array.add(canonical(child)));
            return array;
        }
        var sorted = mapper.createObjectNode();
        var names = new java.util.ArrayList<String>();
        node.fieldNames().forEachRemaining(names::add);
        java.util.Collections.sort(names);
        for (String name : names)
            sorted.set(name, canonical(node.get(name)));
        return sorted;
    }

    private ShipmentException invalid(String msg) {
        return new ShipmentException("SHIPMENT_INVALID_INPUT", msg, HttpStatus.BAD_REQUEST);
    }

    private ShipmentException notFound() {
        return new ShipmentException("SHIPMENT_NOT_FOUND", "Shipment not found", HttpStatus.NOT_FOUND);
    }

    private LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public record CreateResult(ShipmentEntity shipment, HttpStatus status, boolean replayed) {
    }
}
