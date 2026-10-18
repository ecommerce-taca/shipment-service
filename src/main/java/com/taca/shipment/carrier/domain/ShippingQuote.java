package com.taca.shipment.carrier.domain;

import java.time.Instant;

public record ShippingQuote(String carrier, boolean available, Long fee, String currency, Integer minDays,
        Integer maxDays, Instant expiresAt, String unavailableReason) {
}
