package com.taca.shipment.webhook.domain;

import com.taca.shipment.shipment.domain.Carrier;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebhookSignatureVerifierTest {
    private static final String MOCK_SECRET = "test-mock-secret";
    private static final String RAW_BODY = "{\"tracking_code\":\"MOCK123\",\"status\":\"IN_TRANSIT\"}";

    private final WebhookSignatureVerifier verifier = new WebhookSignatureVerifier(
            "ghn-secret", "spx-secret", "jt-secret", MOCK_SECRET, "127.0.0.1, ::1");

    @Test
    void verifiesHmacForExactRawBodyWithOptionalSha256Prefix() throws Exception {
        String signature = signature(MOCK_SECRET, RAW_BODY);

        assertTrue(verifier.verify(Carrier.MOCK, RAW_BODY, signature));
        assertTrue(verifier.verify(Carrier.MOCK, RAW_BODY, "sha256=" + signature));
        assertTrue(verifier.verify(Carrier.MOCK, RAW_BODY, "SHA256=" + signature));
    }

    @Test
    void rejectsChangedBodyWrongSecretAndMalformedOrMissingSignatures() throws Exception {
        String validSignature = signature(MOCK_SECRET, RAW_BODY);

        assertFalse(verifier.verify(Carrier.MOCK, RAW_BODY + " ", validSignature));
        assertFalse(verifier.verify(Carrier.GHN, RAW_BODY, validSignature));
        assertFalse(verifier.verify(Carrier.MOCK, RAW_BODY, null));
        assertFalse(verifier.verify(Carrier.MOCK, RAW_BODY, " "));
        assertFalse(verifier.verify(Carrier.MOCK, RAW_BODY, "not-hex"));
    }

    @Test
    void matchesOnlyConfiguredSourceIpAddresses() {
        assertTrue(verifier.allowedIp("127.0.0.1"));
        assertTrue(verifier.allowedIp("::1"));
        assertFalse(verifier.allowedIp("127.0.0.2"));
        assertFalse(verifier.allowedIp(null));
    }

    private String signature(String secret, String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }
}
