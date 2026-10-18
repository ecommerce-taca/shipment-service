package com.taca.shipment.shipment.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record ShipmentItemRequest(
                @NotBlank @Size(max = 80) String sku_id,
                @Size(max = 80) String product_id,
                @NotBlank @Size(max = 255) String title,
                @Min(1) int quantity,
                @Min(0) Integer weight_gram,
                @Min(0) Integer length_cm,
                @Min(0) Integer width_cm,
                @Min(0) Integer height_cm) {
}
