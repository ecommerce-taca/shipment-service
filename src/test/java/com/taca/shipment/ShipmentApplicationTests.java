package com.taca.shipment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "KAFKA_BOOTSTRAP_SERVERS=localhost:19092",
                "SHIPMENT_KAFKA_PUBLISHER_ENABLED=false",
                "SHIPMENT_KAFKA_CONSUMER_ENABLED=false",
                "SHIPMENT_CREATE_CONSUMER_ENABLED=false"
        })
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class ShipmentApplicationTests {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("shipment_context_test")
            .withUsername("shipment")
            .withPassword("shipment_test");

    @Autowired
    private ShipmentApplication application;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", MYSQL::getJdbcUrl);
        registry.add("DB_USERNAME", MYSQL::getUsername);
        registry.add("DB_PASSWORD", MYSQL::getPassword);
    }

    @Test
    void startsApplicationContextAndValidatesMigratedMysqlSchema() {
        assertNotNull(application);
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history WHERE version = '006' AND success = 1",
                Integer.class));
        assertEquals(4, jdbc.queryForObject("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = 'shipments'
                  AND column_name IN ('checkout_shipping_fee', 'carrier_quoted_fee',
                                      'carrier_actual_fee', 'currency')
                """, Integer.class));
    }

    @Test
    void httpCreatePersistsShipmentAndSameKeyRetryReplaysIt() throws Exception {
        String orderId = "rest-order-" + UUID.randomUUID();
        String shopId = "rest-shop-" + UUID.randomUUID();
        String key = "rest-create-" + UUID.randomUUID();
        String request = createRequest(orderId, shopId, 30000);

        JsonNode created = mapper.readTree(mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", key)
                        .header("X-Request-ID", "rest-create-request")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.meta.request_id").value("rest-create-request"))
                .andExpect(jsonPath("$.data.status").value("CREATED"))
                .andExpect(jsonPath("$.data.order_id").value(orderId))
                .andExpect(jsonPath("$.data.shop_id").value(shopId))
                .andReturn().getResponse().getContentAsString());

        String shipmentId = created.path("data").path("shipment_id").asText();
        assertEquals(30000L, jdbc.queryForObject(
                "SELECT checkout_shipping_fee FROM shipments WHERE order_id=? AND shop_id=?",
                Long.class, orderId, shopId));
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM shipments WHERE order_id=? AND shop_id=?",
                Integer.class, orderId, shopId));
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key=? AND operation='CREATE'",
                Integer.class, key));
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=? AND event_type='shipment.created'",
                Integer.class, shipmentId));

        JsonNode replay = mapper.readTree(mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());

        assertEquals(shipmentId, replay.path("data").path("shipment_id").asText());
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM shipments WHERE order_id=? AND shop_id=?",
                Integer.class, orderId, shopId));
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id=? AND event_type='shipment.created'",
                Integer.class, shipmentId));
    }

    @Test
    void httpMapsChangedPayloadUnderSameIdempotencyKeyToConflict() throws Exception {
        String orderId = "rest-conflict-order-" + UUID.randomUUID();
        String shopId = "rest-conflict-shop-" + UUID.randomUUID();
        String key = "rest-conflict-" + UUID.randomUUID();

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(orderId, shopId, 30000)))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(orderId, shopId, 25000)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_IDEMPOTENCY_CONFLICT"));

        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM shipments WHERE order_id=? AND shop_id=?",
                Integer.class, orderId, shopId));
    }

    @Test
    void httpNewKeyForExistingOrderShopReturnsStoredShipmentWithoutApplyingChangedPayload() throws Exception {
        String orderId = "rest-existing-order-" + UUID.randomUUID();
        String shopId = "rest-existing-shop-" + UUID.randomUUID();
        String firstKey = "rest-existing-first-" + UUID.randomUUID();
        String secondKey = "rest-existing-second-" + UUID.randomUUID();

        JsonNode first = mapper.readTree(mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", firstKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(orderId, shopId, 30000)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString());

        JsonNode second = mapper.readTree(mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", secondKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(orderId, shopId, 25000)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());

        assertEquals(first.path("data").path("shipment_id").asText(),
                second.path("data").path("shipment_id").asText());
        assertEquals(30000L, second.path("data").path("checkout_shipping_fee").asLong());
        assertEquals(30000L, jdbc.queryForObject(
                "SELECT checkout_shipping_fee FROM shipments WHERE order_id=? AND shop_id=?",
                Long.class, orderId, shopId));
        assertEquals(0, jdbc.queryForObject(
                "SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key=? AND operation='CREATE'",
                Integer.class, secondKey));
    }

    @Test
    void httpRejectsMissingInternalTokenAndInvalidRequestWithContractErrorBodies() throws Exception {
        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("Idempotency-Key", "missing-auth-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_UNAUTHENTICATED"));

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", "invalid-request-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_INVALID_INPUT"))
                .andExpect(jsonPath("$.error.trace_id").isNotEmpty());

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", "invalid-json-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{invalid-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_INVALID_INPUT"));

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", "invalid-carrier-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(
                                "invalid-carrier-order-" + UUID.randomUUID(),
                                "invalid-carrier-shop-" + UUID.randomUUID(),
                                30000).replace("\"carrier\":\"MOCK\"", "\"carrier\":\"UNSUPPORTED\"")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_INVALID_INPUT"));
    }

    @Test
    void httpBuyerAuthorizationChecksTrustedRoleAndShipmentOwnership() throws Exception {
        String orderId = "rest-owner-order-" + UUID.randomUUID();
        String shopId = "rest-owner-shop-" + UUID.randomUUID();
        String buyerId = "rest-buyer-" + UUID.randomUUID();

        mockMvc.perform(post("/api/v1/internal/shipments")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .header("Idempotency-Key", "rest-owner-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createRequest(orderId, shopId, 30000, buyerId)))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/api/v1/orders/{orderId}/shipment", orderId)
                        .header("X-User-ID", buyerId)
                        .header("X-User-Roles", "BUYER"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.order_id").value(orderId));

        mockMvc.perform(get("/api/v1/orders/{orderId}/shipment", orderId)
                        .header("X-User-ID", "another-buyer")
                        .header("X-User-Roles", "BUYER"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_FORBIDDEN"));

        mockMvc.perform(get("/api/v1/orders/{orderId}/shipment", orderId)
                        .header("X-User-ID", buyerId)
                        .header("X-User-Roles", "SELLER"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_FORBIDDEN"));

        mockMvc.perform(get("/api/v1/seller/orders/{orderId}/shipment", orderId)
                        .header("X-User-ID", "seller-1")
                        .header("X-User-Roles", "SELLER")
                        .header("X-User-Shop-Scope", shopId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.shop_id").value(shopId));

        mockMvc.perform(get("/api/v1/seller/orders/{orderId}/shipment", orderId)
                        .header("X-User-ID", "seller-1")
                        .header("X-User-Roles", "SELLER")
                        .header("X-User-Shop-Scope", "another-shop"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_FORBIDDEN"));
    }

    @Test
    void httpQuoteUsesLocalMockCarrierAndValidatesRequiredSnapshots() throws Exception {
        mockMvc.perform(post("/api/v1/internal/shipping/quote")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "from_shop_id": "local-shop",
                                  "from_address_snapshot": {"line1": "Warehouse"},
                                  "to_address_snapshot": {"line1": "Destination"},
                                  "items": [{"sku_id": "sku-1", "title": "Test item", "quantity": 1, "weight_gram": 500}],
                                  "carrier": "MOCK"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.fee").value(20000))
                .andExpect(jsonPath("$.data.currency").value("VND"))
                .andExpect(jsonPath("$.data.carrier").value("MOCK"))
                .andExpect(jsonPath("$.data.quote_expires_at").isNotEmpty());

        mockMvc.perform(post("/api/v1/internal/shipping/quote")
                        .header("X-Internal-Service-Token", "local-dev-token-change-me")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"from_shop_id": "local-shop", "items": []}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("SHIPMENT_INVALID_INPUT"));
    }

    private String createRequest(String orderId, String shopId, long shippingFee) throws Exception {
        return createRequest(orderId, shopId, shippingFee, "rest-buyer");
    }

    private String createRequest(String orderId, String shopId, long shippingFee, String buyerId) throws Exception {
        var request = mapper.createObjectNode();
        request.put("order_id", orderId);
        request.put("checkout_group_id", "rest-checkout-" + UUID.randomUUID());
        request.put("shop_id", shopId);
        request.put("buyer_user_id", buyerId);
        request.put("buyer_email", "buyer@example.test");
        request.put("carrier", "MOCK");
        request.set("from_address_snapshot", mapper.createObjectNode().put("line1", "Warehouse"));
        request.set("to_address_snapshot", mapper.createObjectNode().put("line1", "Destination"));
        request.set("items", mapper.createArrayNode().add(mapper.createObjectNode()
                .put("sku_id", "sku-1")
                .put("title", "Test item")
                .put("quantity", 1)
                .put("weight_gram", 500)));
        request.put("cod_amount", 0);
        request.put("shipping_fee", shippingFee);
        request.put("payment_method", "PREPAID");
        request.put("currency", "VND");
        return request.toString();
    }
}
