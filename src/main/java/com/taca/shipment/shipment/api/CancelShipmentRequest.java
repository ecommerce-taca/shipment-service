package com.taca.shipment.shipment.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

public record CancelShipmentRequest(@NotBlank String reason, @PositiveOrZero long version) {
}
