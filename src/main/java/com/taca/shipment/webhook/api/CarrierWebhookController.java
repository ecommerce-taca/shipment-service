package com.taca.shipment.webhook.api;

import com.taca.shipment.common.api.ApiResponse;
import com.taca.shipment.webhook.domain.ShipmentWebhookService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/webhooks/shipping")
public class CarrierWebhookController {
    private final ShipmentWebhookService service;

    public CarrierWebhookController(ShipmentWebhookService service) {
        this.service = service;
    }

    @PostMapping("/{carrier}")
    public ApiResponse<Map<String, Object>> accept(@PathVariable String carrier, @RequestBody String rawBody,
            @RequestHeader(value = "X-Carrier-Signature", required = false) String signature,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId, HttpServletRequest request) {
        Map<String, Object> result = service.accept(carrier, rawBody, signature, request.getRemoteAddr());
        return ApiResponse.of(result,
                requestId == null || requestId.isBlank() ? UUID.randomUUID().toString() : requestId);
    }
}
