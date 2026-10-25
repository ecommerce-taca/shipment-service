package com.taca.shipment.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrustedContextFilterTest {
    private static final String INTERNAL_TOKEN = "test-internal-token";
    private final ObjectMapper mapper = new ObjectMapper();
    private final TrustedContextFilter filter = new TrustedContextFilter(INTERNAL_TOKEN, "test-ops-token", true,
            mapper);

    @Test
    void allowsInternalRequestWithMatchingServiceToken() throws Exception {
        MockHttpServletRequest request = request("/api/v1/internal/shipments");
        request.addHeader("X-Internal-Service-Token", INTERNAL_TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean chainInvoked = new AtomicBoolean();

        filter.doFilter(request, response, (req, res) -> chainInvoked.set(true));

        assertTrue(chainInvoked.get());
        assertEquals(200, response.getStatus());
    }

    @Test
    void rejectsMissingOrIncorrectInternalTokenWithAuthenticationError() throws Exception {
        for (String suppliedToken : new String[] { null, "wrong-token" }) {
            MockHttpServletRequest request = request("/api/v1/internal/shipments");
            if (suppliedToken != null) {
                request.addHeader("X-Internal-Service-Token", suppliedToken);
            }
            MockHttpServletResponse response = new MockHttpServletResponse();
            AtomicBoolean chainInvoked = new AtomicBoolean();

            filter.doFilter(request, response, (req, res) -> chainInvoked.set(true));

            JsonNode error = mapper.readTree(response.getContentAsByteArray()).path("error");
            assertEquals(401, response.getStatus());
            assertEquals("SHIPMENT_UNAUTHENTICATED", error.path("code").asText());
            assertFalse(chainInvoked.get());
        }
    }

    @Test
    void requiresSellerRoleForSellerRoutesAndAllowsAuthorizedSeller() throws Exception {
        MockHttpServletRequest deniedRequest = request("/api/v1/seller/orders/order-1/shipment");
        deniedRequest.addHeader("X-User-ID", "user-1");
        deniedRequest.addHeader("X-User-Roles", "BUYER");
        MockHttpServletResponse deniedResponse = new MockHttpServletResponse();
        AtomicBoolean deniedChainInvoked = new AtomicBoolean();

        filter.doFilter(deniedRequest, deniedResponse, (req, res) -> deniedChainInvoked.set(true));

        JsonNode deniedError = mapper.readTree(deniedResponse.getContentAsByteArray()).path("error");
        assertEquals(403, deniedResponse.getStatus());
        assertEquals("SHIPMENT_FORBIDDEN", deniedError.path("code").asText());
        assertFalse(deniedChainInvoked.get());

        MockHttpServletRequest allowedRequest = request("/api/v1/seller/orders/order-1/shipment");
        allowedRequest.addHeader("X-User-ID", "seller-1");
        allowedRequest.addHeader("X-User-Roles", "[\"ROLE_SELLER\"]");
        MockHttpServletResponse allowedResponse = new MockHttpServletResponse();
        AtomicBoolean allowedChainInvoked = new AtomicBoolean();

        filter.doFilter(allowedRequest, allowedResponse, (req, res) -> allowedChainInvoked.set(true));

        assertTrue(allowedChainInvoked.get());
        assertEquals(200, allowedResponse.getStatus());
    }

    @Test
    void requiresOperatorTokenIdentityAndApprovedRoleForOperationsRoutes() throws Exception {
        MockHttpServletRequest request = request("/api/v1/internal/ops/shipment-create-commands/id/replay");
        request.addHeader("X-Internal-Service-Token", INTERNAL_TOKEN);
        request.addHeader("X-Operator-Token", "test-ops-token");
        request.addHeader("X-Operator-ID", "operator-1");
        request.addHeader("X-Operator-Roles", "BUYER");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean chainInvoked = new AtomicBoolean();

        filter.doFilter(request, response, (req, res) -> chainInvoked.set(true));

        JsonNode error = mapper.readTree(response.getContentAsByteArray()).path("error");
        assertEquals(403, response.getStatus());
        assertEquals("SHIPMENT_FORBIDDEN", error.path("code").asText());
        assertFalse(chainInvoked.get());
    }

    private MockHttpServletRequest request(String path) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", path);
        request.setRequestURI(path);
        return request;
    }
}
