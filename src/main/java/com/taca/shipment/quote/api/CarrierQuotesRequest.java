package com.taca.shipment.quote.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * Internal REST contract called by Order-Commerce after it has checked seller
 * scope/order status.
 */
public record CarrierQuotesRequest(
                @NotBlank String order_id,
                @NotBlank String shop_id,
                @NotNull JsonNode from_address_snapshot,
                @NotNull JsonNode to_address_snapshot,
                @NotEmpty @Size(max = 100) List<@Valid ShipmentItemRequest> items) {
}
