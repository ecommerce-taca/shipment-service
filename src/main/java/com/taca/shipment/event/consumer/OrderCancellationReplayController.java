package com.taca.shipment.event.consumer;

import com.taca.shipment.common.api.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/internal/ops/order-cancellations")
public class OrderCancellationReplayController {
    private final OrderCancellationInbox inbox;

    public OrderCancellationReplayController(OrderCancellationInbox inbox) {
        this.inbox = inbox;
    }

    @PostMapping("/{orderId}/replay")
    public ApiResponse<Map<String, String>> replay(@PathVariable String orderId,
            @RequestHeader("X-Operator-ID") String operatorId,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId) {
        String auditRequestId = requestId == null || requestId.isBlank() ? "order-cancellation-replay" : requestId;
        if (!inbox.replay(orderId, operatorId, auditRequestId))
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Only an order cancellation in NEEDS_OPERATOR state can be replayed");
        return ApiResponse.of(Map.of("order_id", orderId, "status", "RETRY_WAIT"), auditRequestId);
    }
}
