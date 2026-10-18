package com.taca.shipment.quote.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.quote.api.CarrierQuotesRequest;
import com.taca.shipment.quote.api.QuoteRequest;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import com.taca.shipment.shipment.domain.Carrier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class QuoteService {
    private final CarrierRegistry registry;
    private final int ttlMinutes;
    private final String environment;
    private final boolean mockEnabled;

    public QuoteService(CarrierRegistry registry, @Value("${shipment.carrier.quote-ttl-minutes:30}") int ttlMinutes,
            @Value("${shipment.env:local}") String environment,
            @Value("${shipment.carrier.mock-enabled:true}") boolean mockEnabled) {
        this.registry = registry;
        this.ttlMinutes = ttlMinutes;
        this.environment = environment;
        this.mockEnabled = mockEnabled;
    }

    public ShippingQuote quote(QuoteRequest req) {
        validateSnapshots(req.from_address_snapshot(), req.to_address_snapshot());
        Carrier carrier = req.carrier() == null || req.carrier().isBlank() ? Carrier.GHN
                : Carrier.fromApi(req.carrier());
        if (carrier == Carrier.MOCK
                && (!mockEnabled || !java.util.Set.of("local", "test", "staging").contains(environment.toLowerCase())))
            throw new ShipmentException("SHIPMENT_CARRIER_UNAVAILABLE",
                    "MOCK carrier is disabled outside local/test/staging", HttpStatus.SERVICE_UNAVAILABLE);
        if (carrier == Carrier.MOCK)
            return registry.require(Carrier.MOCK).quote(req.from_address_snapshot(), req.to_address_snapshot(),
                    itemNodes(req.items()));
        CarrierAdapter adapter = registry.require(carrier);
        ShippingQuote quote = adapter.quote(req.from_address_snapshot(), req.to_address_snapshot(),
                itemNodes(req.items()));
        return withExpiry(quote);
    }

    public Map<String, Object> compare(CarrierQuotesRequest req) {
        validateSnapshots(req.from_address_snapshot(), req.to_address_snapshot());
        List<Map<String, Object>> results = new ArrayList<>();
        for (Carrier carrier : List.of(Carrier.GHN, Carrier.SPX, Carrier.J_AND_T)) {
            try {
                if (!registry.configured(carrier)) {
                    results.add(unavailable(carrier, "ADAPTER_NOT_CONFIGURED"));
                    continue;
                }
                ShippingQuote q = withExpiry(registry.require(carrier).quote(req.from_address_snapshot(),
                        req.to_address_snapshot(), itemNodes(req.items())));
                if (!q.available())
                    results.add(unavailable(carrier,
                            q.unavailableReason() == null ? "CARRIER_UNAVAILABLE" : q.unavailableReason()));
                else {
                    Map<String, Object> result = new java.util.LinkedHashMap<>();
                    result.put("carrier", carrier.apiValue());
                    result.put("available", true);
                    result.put("fee", q.fee());
                    result.put("currency", q.currency());
                    result.put("estimate", q.minDays() == null ? null
                            : Map.of("min_days", q.minDays(), "max_days", q.maxDays()));
                    results.add(result);
                }
            } catch (Exception ex) {
                results.add(unavailable(carrier,
                        ex instanceof ShipmentException se && "SHIPMENT_CARRIER_UNAVAILABLE".equals(se.code())
                                ? "CARRIER_UNAVAILABLE"
                                : "QUOTE_FAILED"));
            }
        }
        return Map.of("order_id", req.order_id(), "carriers", results, "quote_expires_at",
                Instant.now().plusSeconds(ttlMinutes * 60L).toString());
    }

    private ShippingQuote withExpiry(ShippingQuote q) {
        return new ShippingQuote(q.carrier(), q.available(), q.fee(), q.currency(), q.minDays(), q.maxDays(),
                Instant.now().plusSeconds(ttlMinutes * 60L), q.unavailableReason());
    }

    private Map<String, Object> unavailable(Carrier c, String reason) {
        return Map.of("carrier", c.apiValue(), "available", false, "unavailable_reason", reason);
    }

    private List<JsonNode> itemNodes(List<ShipmentItemRequest> items) {
        // Use the record fields as a stable carrier input shape; no raw request is
        // logged.
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return items.stream().map(item -> (JsonNode) mapper.valueToTree(item)).toList();
    }

    private void validateSnapshots(JsonNode from, JsonNode to) {
        if (from == null || to == null || !from.isObject() || !to.isObject())
            throw new ShipmentException("SHIPMENT_INVALID_INPUT", "Address snapshots must be JSON objects",
                    HttpStatus.BAD_REQUEST);
        if (from.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16384
                || to.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16384)
            throw new ShipmentException("SHIPMENT_INVALID_INPUT", "Address snapshot exceeds 16 KiB",
                    HttpStatus.BAD_REQUEST);
    }
}
