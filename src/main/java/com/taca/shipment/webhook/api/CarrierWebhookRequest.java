package com.taca.shipment.webhook.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Instant;

public record CarrierWebhookRequest(@NotBlank @Size(max = 128) String external_event_id,
        @NotBlank @Size(max = 64) String tracking_code,
        @NotBlank @Size(max = 80) String status,
        @NotNull Instant occurred_at,
        @Size(max = 500) String note) {
}
