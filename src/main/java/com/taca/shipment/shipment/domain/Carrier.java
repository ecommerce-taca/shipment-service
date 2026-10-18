package com.taca.shipment.shipment.domain;

public enum Carrier {
    GHN, SPX, J_AND_T, MOCK;

    public static Carrier fromApi(String value) {
        if (value == null)
            throw new IllegalArgumentException("carrier is required");
        return switch (value.trim().toUpperCase()) {
            case "GHN" -> GHN;
            case "SPX" -> SPX;
            case "J&T", "J_AND_T", "J_AND_T_EXPRESS" -> J_AND_T;
            case "MOCK" -> MOCK;
            default -> throw new IllegalArgumentException("Unsupported carrier");
        };
    }

    public String apiValue() {
        return this == J_AND_T ? "J&T" : name();
    }
}
