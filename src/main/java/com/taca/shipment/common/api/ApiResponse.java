package com.taca.shipment.common.api;

import java.util.Map;

public record ApiResponse<T>(T data, Map<String, Object> meta) {
    public static <T> ApiResponse<T> of(T data, String requestId) {
        return new ApiResponse<>(data, Map.of("request_id", requestId));
    }
}
