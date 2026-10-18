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
@RequestMapping("/api/v1/internal/ops/shipment-create-commands")
public class ShipmentCommandReplayController {
    private final ShipmentCreateCommandInbox inbox;

    public ShipmentCommandReplayController(ShipmentCreateCommandInbox inbox) {
        this.inbox = inbox;
    }

    @PostMapping("/{commandId}/replay")
    public ApiResponse<Map<String, String>> replay(@PathVariable String commandId,
            @RequestHeader("X-Operator-ID") String operatorId,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId) {
        String auditRequestId = requestId == null || requestId.isBlank() ? "shipment-command-replay" : requestId;
        if (!inbox.replay(commandId, operatorId, auditRequestId))
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Only a command in durable DEAD state can be replayed");
        return ApiResponse.of(Map.of("command_id", commandId, "status", "RETRY_WAIT"), auditRequestId);
    }
}
