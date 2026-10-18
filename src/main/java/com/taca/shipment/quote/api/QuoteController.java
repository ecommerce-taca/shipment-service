package com.taca.shipment.quote.api;

import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.common.api.ApiResponse;
import com.taca.shipment.quote.domain.QuoteService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/internal/shipping")
public class QuoteController {
    private final QuoteService service;

    public QuoteController(QuoteService service) {
        this.service = service;
    }

    /**
     * Canonical internal quote contract: Order sends the address snapshots and
     * items it owns.
     */
    @PostMapping("/quote")
    public ApiResponse<Map<String, Object>> quote(@Valid @RequestBody QuoteRequest req,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId) {
        ShippingQuote q = service.quote(req);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("fee", q.fee());
        data.put("currency", q.currency());
        data.put("carrier", q.carrier());
        data.put("estimate", q.minDays() == null ? null : Map.of("min_days", q.minDays(), "max_days", q.maxDays()));
        data.put("quote_expires_at", q.expiresAt() == null ? null : q.expiresAt().toString());
        return ApiResponse.of(data, rid(requestId));
    }

    @PostMapping("/carrier-quotes")
    public ApiResponse<Map<String, Object>> carrierQuotes(@Valid @RequestBody CarrierQuotesRequest req,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId) {
        return ApiResponse.of(service.compare(req), rid(requestId));
    }

    private String rid(String value) {
        return value == null || value.isBlank() ? UUID.randomUUID().toString() : value;
    }
}
