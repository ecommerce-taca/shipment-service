package com.taca.shipment.webhook.domain;

import com.taca.shipment.shipment.domain.Carrier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class WebhookSignatureVerifier {
    private final String ghnSecret, spxSecret, jtSecret, mockSecret;
    private final Set<String> allowedIps;

    public WebhookSignatureVerifier(@Value("${SHIPMENT_WEBHOOK_GHN_SECRET:}") String ghnSecret,
            @Value("${SHIPMENT_WEBHOOK_SPX_SECRET:}") String spxSecret,
            @Value("${SHIPMENT_WEBHOOK_JT_SECRET:}") String jtSecret,
            @Value("${SHIPMENT_WEBHOOK_MOCK_SECRET:local-mock-webhook-secret}") String mockSecret,
            @Value("${SHIPMENT_WEBHOOK_ALLOWED_IPS:127.0.0.1,::1}") String allowedIps) {
        this.ghnSecret = ghnSecret;
        this.spxSecret = spxSecret;
        this.jtSecret = jtSecret;
        this.mockSecret = mockSecret;
        this.allowedIps = Arrays.stream(allowedIps.split(",")).map(String::trim).filter(s -> !s.isBlank())
                .collect(Collectors.toSet());
    }

    public boolean allowedIp(String ip) {
        return allowedIps.contains(ip);
    }

    public boolean verify(Carrier carrier, String rawBody, String supplied) {
        String secret = switch (carrier) {
            case GHN -> ghnSecret;
            case SPX -> spxSecret;
            case J_AND_T -> jtSecret;
            case MOCK -> mockSecret;
        };
        if (secret == null || secret.isBlank() || supplied == null || supplied.isBlank())
            return false;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] expected = mac.doFinal(rawBody.getBytes(StandardCharsets.UTF_8));
            byte[] actual = HexFormat.of().parseHex(supplied.replaceFirst("(?i)^sha256=", ""));
            return MessageDigest.isEqual(expected, actual);
        } catch (Exception ex) {
            return false;
        }
    }
}
