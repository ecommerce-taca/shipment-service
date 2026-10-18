package com.taca.shipment.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Defense-in-depth. Network policy must also restrict direct access to the
 * service.
 */
@Component
public class TrustedContextFilter extends OncePerRequestFilter {
    private final String internalToken;
    private final String opsToken;
    private final boolean enforceGatewayHeaders;
    private final ObjectMapper mapper;

    public TrustedContextFilter(@Value("${shipment.security.internal-token}") String internalToken,
            @Value("${shipment.security.ops-token:}") String opsToken,
            @Value("${shipment.security.enforce-gateway-headers:true}") boolean enforceGatewayHeaders,
            ObjectMapper mapper) {
        this.internalToken = internalToken;
        this.opsToken = opsToken;
        this.enforceGatewayHeaders = enforceGatewayHeaders;
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String p = request.getRequestURI();
        return p.endsWith("/health/live") || p.endsWith("/health/ready") || p.contains("/webhooks/shipping/")
                || !p.startsWith("/api/v1/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String path = req.getRequestURI();
        if (path.contains("/internal/")) {
            String supplied = req.getHeader("X-Internal-Service-Token");
            if (internalToken == null || internalToken.isBlank() || supplied == null
                    || !constantTimeEquals(internalToken, supplied)) {
                writeError(res, 401, "SHIPMENT_UNAUTHENTICATED", "Internal service authentication required", req);
                return;
            }
            if (path.contains("/internal/ops/")) {
                String operatorId = req.getHeader("X-Operator-ID");
                String roles = req.getHeader("X-Operator-Roles");
                String suppliedOpsToken = req.getHeader("X-Operator-Token");
                if (opsToken == null || opsToken.isBlank() || suppliedOpsToken == null
                        || !constantTimeEquals(opsToken, suppliedOpsToken)
                        || blank(operatorId) || operatorId.length() > 128
                        || (!hasRole(roles, "ADMIN") && !hasRole(roles, "SHIPMENT_COMMAND_REPLAY"))) {
                    writeError(res, 403, "SHIPMENT_FORBIDDEN", "Authorized operator context required", req);
                    return;
                }
            }
        } else if (enforceGatewayHeaders && (path.contains("/orders/") || path.contains("/seller/"))) {
            String user = req.getHeader("X-User-ID");
            String roles = req.getHeader("X-User-Roles");
            if (blank(user) || blank(roles)) {
                writeError(res, 401, "SHIPMENT_UNAUTHENTICATED", "Trusted Gateway actor context required", req);
                return;
            }
            if (path.contains("/seller/") && !hasRole(roles, "SELLER") && !hasRole(roles, "SHOP_STAFF")
                    && !hasRole(roles, "ADMIN")) {
                writeError(res, 403, "SHIPMENT_FORBIDDEN", "Seller role required", req);
                return;
            }
            if (!path.contains("/seller/") && !hasRole(roles, "BUYER") && !hasRole(roles, "ADMIN")) {
                writeError(res, 403, "SHIPMENT_FORBIDDEN", "Buyer role required", req);
                return;
            }
        }
        chain.doFilter(req, res);
    }

    private boolean hasRole(String roles, String expected) {
        if (roles == null || roles.isBlank())
            return false;
        String normalized = roles.replace('[', ' ').replace(']', ' ').replace('"', ' ').replace('\'', ' ')
                .toUpperCase();
        return java.util.Arrays.stream(normalized.split("[,;\\s]+")).filter(token -> !token.isBlank())
                .anyMatch(token -> token.equals(expected) || token.equals("ROLE_" + expected));
    }

    private boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length())
            return false;
        int result = 0;
        for (int i = 0; i < a.length(); i++)
            result |= a.charAt(i) ^ b.charAt(i);
        return result == 0;
    }

    private void writeError(HttpServletResponse res, int status, String code, String message, HttpServletRequest req)
            throws IOException {
        res.setStatus(status);
        res.setContentType("application/json");
        String trace = req.getHeader("X-Trace-ID");
        if (trace == null || trace.isBlank())
            trace = req.getHeader("X-Request-ID");
        if (trace == null || trace.isBlank())
            trace = UUID.randomUUID().toString();
        mapper.writeValue(res.getOutputStream(),
                Map.of("error", Map.of("code", code, "message", message, "details", "", "trace_id", trace)));
    }
}
