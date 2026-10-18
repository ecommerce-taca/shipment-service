package com.taca.shipment.shipment.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

public record CreateShipmentRequest(
                @NotBlank @Size(max = 80) String order_id,
                @Size(max = 80) String checkout_group_id,
                @NotBlank @Size(max = 80) String shop_id,
                @NotBlank @Size(max = 80) String buyer_user_id,
                @NotBlank @Email @Size(max = 254) String buyer_email,
                @NotBlank String carrier,
                @NotNull JsonNode from_address_snapshot,
                @NotNull JsonNode to_address_snapshot,
                @NotEmpty @Size(max = 100) List<@Valid ShipmentItemRequest> items,
                @Min(0) long cod_amount,
                @Min(0) long shipping_fee,
                String payment_method,
                @NotBlank @Size(min = 3, max = 3) @Pattern(regexp = "[A-Z]{3}") String currency) {
}
