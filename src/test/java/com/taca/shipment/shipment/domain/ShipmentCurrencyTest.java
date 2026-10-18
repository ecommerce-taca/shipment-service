package com.taca.shipment.shipment.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.taca.shipment.carrier.domain.CarrierAdapter;
import com.taca.shipment.carrier.domain.CarrierCreateResult;
import com.taca.shipment.carrier.domain.CarrierRegistry;
import com.taca.shipment.carrier.domain.CarrierTimeoutException;
import com.taca.shipment.carrier.domain.ShippingQuote;
import com.taca.shipment.common.error.ShipmentException;
import com.taca.shipment.carrier.mock.MockCarrierAdapter;
import com.taca.shipment.event.EventContext;
import com.taca.shipment.outbox.OutboxService;
import com.taca.shipment.shipment.api.CreateShipmentRequest;
import com.taca.shipment.shipment.api.ShipmentItemRequest;
import com.taca.shipment.shipment.api.ShipmentController;
import com.taca.shipment.shipment.persistence.*;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ShipmentCurrencyTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ValidatorFactory VALIDATOR_FACTORY = Validation.buildDefaultValidatorFactory();
    private static final Validator VALIDATOR = VALIDATOR_FACTORY.getValidator();

    private ShipmentRepository shipments;
    private ShipmentItemRepository items;
    private ShipmentEventRepository events;
    private IdempotencyRepository idempotencies;
    private OutboxService outbox;
    private PlatformTransactionManager transactionManager;
    private ShipmentCommandService commands;
    private CarrierAdapter adapter;
    private AtomicReference<ShipmentEntity> storedShipment;
    private AtomicReference<JsonNode> carrierRequest;
    private AtomicReference<Map<String, Object>> eventPayload;
    private AtomicReference<String> eventType;
    private AtomicReference<ShippingQuote> quoteResult;
    private Map<IdempotencyId, IdempotencyEntity> idemRows;

    @AfterAll
    static void closeValidatorFactory() {
        VALIDATOR_FACTORY.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        shipments = mock(ShipmentRepository.class);
        items = mock(ShipmentItemRepository.class);
        events = mock(ShipmentEventRepository.class);
        idempotencies = mock(IdempotencyRepository.class);
        outbox = mock(OutboxService.class);
        transactionManager = mock(PlatformTransactionManager.class);
        storedShipment = new AtomicReference<>();
        carrierRequest = new AtomicReference<>();
        eventPayload = new AtomicReference<>();
        eventType = new AtomicReference<>();
        quoteResult = new AtomicReference<>(
                new ShippingQuote("MOCK", true, 4200L, "EUR", 2, 4, Instant.now().plusSeconds(300), null));
        idemRows = new HashMap<>();

        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
                .thenReturn(new SimpleTransactionStatus());
        when(shipments.findByOrderIdAndShopId(anyString(), anyString())).thenReturn(Optional.empty());
        when(shipments.findById(any(UUID.class))).thenAnswer(invocation -> Optional.ofNullable(storedShipment.get()));
        when(shipments.saveAndFlush(any(ShipmentEntity.class))).thenAnswer(invocation -> {
            ShipmentEntity shipment = invocation.getArgument(0);
            storedShipment.set(shipment);
            return shipment;
        });
        when(idempotencies.findById(any(IdempotencyId.class)))
                .thenAnswer(invocation -> Optional.ofNullable(idemRows.get(invocation.getArgument(0))));
        when(idempotencies.save(any(IdempotencyEntity.class))).thenAnswer(invocation -> {
            IdempotencyEntity row = invocation.getArgument(0);
            idemRows.put(row.getId(), row);
            return row;
        });
        when(events.findByShipmentIdOrderByOccurredAtAsc(any(UUID.class))).thenReturn(List.of());
        when(events.save(any(ShipmentEventEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        doAnswer(invocation -> {
            eventType.set(invocation.getArgument(0));
            eventPayload.set(invocation.getArgument(3));
            return null;
        }).when(outbox).append(anyString(), anyString(), anyString(), any(), any(EventContext.class));

        adapter = spy(new CarrierAdapter() {
            @Override
            public Carrier carrier() {
                return Carrier.MOCK;
            }

            @Override
            public ShippingQuote quote(JsonNode from, JsonNode to, List<JsonNode> quoteItems) {
                return quoteResult.get();
            }

            @Override
            public CarrierCreateResult create(String idempotencyKey, String orderId, JsonNode request) {
                carrierRequest.set(request.deepCopy());
                return new CarrierCreateResult("external-1", "tracking-1");
            }

            @Override
            public void cancel(String externalShipmentId, String reason, String idempotencyKey) {
            }
        });
        commands = new ShipmentCommandService(shipments, items, events, idempotencies,
                new CarrierRegistry(List.of(adapter)), MAPPER, outbox,
                new TransactionTemplate(transactionManager), "test", true);
    }

    @Test
    void checkoutFeeIsPreservedAndLegacyFeeContractRemainsUnchanged() throws Exception {
        quoteResult.set(quote(20000L, "VND", Instant.now().plusSeconds(300)));
        var result = commands.create("create-vnd", request("VND", 30000));

        assertEquals(HttpStatus.CREATED, result.status());
        assertEquals(20000L, result.shipment().getShippingFee());
        assertEquals(30000L, result.shipment().getCheckoutShippingFee());
        assertEquals(20000L, result.shipment().getCarrierQuotedFee());
        assertEquals(20000L, commands.view(result.shipment()).get("shipping_fee"));
        assertEquals(30000L, commands.view(result.shipment()).get("checkout_shipping_fee"));
        assertEquals(20000L, commands.view(result.shipment()).get("carrier_quoted_fee"));
        assertEquals(-10000L, commands.view(result.shipment()).get("carrier_quote_delta"));
        assertEquals(30000L, carrierRequest.get().path("checkout_shipping_fee").asLong());
        assertEquals(20000L, carrierRequest.get().path("carrier_quoted_fee").asLong());
        assertTrue(carrierRequest.get().path("carrier_actual_fee").isMissingNode());
        assertEquals(20000L, eventPayload.get().get("shipping_fee"));
        assertEquals(30000L, eventPayload.get().get("checkout_shipping_fee"));
        assertEquals(20000L, eventPayload.get().get("carrier_quoted_fee"));
        assertEquals(-10000L, eventPayload.get().get("carrier_quote_delta"));
        assertNull(eventPayload.get().get("carrier_actual_fee"));
        assertEquals("shipment.created", eventType.get());

        ObjectMapper serializationMapper = MAPPER.copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JsonNode responseJson = serializationMapper.valueToTree(commands.view(result.shipment()));
        JsonNode eventJson = serializationMapper.valueToTree(eventPayload.get());
        assertEquals(30000L, responseJson.path("checkout_shipping_fee").asLong());
        assertEquals(20000L, responseJson.path("carrier_quoted_fee").asLong());
        assertEquals(-10000L, responseJson.path("carrier_quote_delta").asLong());
        assertEquals(30000L, eventJson.path("checkout_shipping_fee").asLong());
        assertEquals(20000L, eventJson.path("carrier_quoted_fee").asLong());
        assertEquals(-10000L, eventJson.path("carrier_quote_delta").asLong());
        assertTrue(responseJson.path("carrier_actual_fee").isMissingNode());
        assertTrue(eventJson.path("carrier_actual_fee").isMissingNode());
    }

    @Test
    void createAndLookupApiResponsesSerializeExplicitFeeFields() throws Exception {
        quoteResult.set(quote(20000L, "VND", Instant.now().plusSeconds(300)));
        ShipmentController controller = new ShipmentController(shipments, events, commands);

        var createResponse = controller.create("api-fee-key", request("VND", 30000), "request-1");
        assertEquals(HttpStatus.CREATED, createResponse.getStatusCode());
        var created = createResponse.getBody().data();
        when(shipments.findByOrderId("order-1")).thenReturn(Optional.of(storedShipment.get()));

        var lookupResponse = controller.buyer("order-1", "buyer-1", "request-2");
        ObjectMapper serializationMapper = MAPPER.copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JsonNode createJson = serializationMapper.valueToTree(created);
        JsonNode lookupJson = serializationMapper.valueToTree(lookupResponse.data());

        for (JsonNode response : List.of(createJson, lookupJson)) {
            assertEquals(30000L, response.path("checkout_shipping_fee").asLong());
            assertEquals(20000L, response.path("carrier_quoted_fee").asLong());
            assertEquals(-10000L, response.path("carrier_quote_delta").asLong());
            assertTrue(response.path("carrier_actual_fee").isMissingNode());
        }
    }

    @Test
    void higherCarrierQuoteDoesNotChangeCheckoutFee() throws Exception {
        quoteResult.set(quote(30000L, "VND", Instant.now().plusSeconds(300)));
        var result = commands.create("higher-quote-key", request("VND", 20000));

        assertEquals(30000L, result.shipment().getShippingFee());
        assertEquals(20000L, result.shipment().getCheckoutShippingFee());
        assertEquals(30000L, result.shipment().getCarrierQuotedFee());
        assertEquals(10000L, commands.view(result.shipment()).get("carrier_quote_delta"));
        assertEquals(30000L, eventPayload.get().get("shipping_fee"));
        assertEquals(20000L, eventPayload.get().get("checkout_shipping_fee"));
        assertEquals(30000L, eventPayload.get().get("carrier_quoted_fee"));
        assertEquals(10000L, eventPayload.get().get("carrier_quote_delta"));
        assertNull(eventPayload.get().get("carrier_actual_fee"));
    }

    @Test
    void missingCarrierFeeRemainsUnknownAndDoesNotBecomeZero() throws Exception {
        quoteResult.set(quote(null, null, Instant.now().plusSeconds(300)));
        var result = commands.create("no-carrier-fee-key", request("VND", 30000));

        assertEquals(30000L, result.shipment().getShippingFee());
        assertEquals(30000L, result.shipment().getCheckoutShippingFee());
        assertNull(result.shipment().getCarrierQuotedFee());
        assertNull(commands.view(result.shipment()).get("carrier_quoted_fee"));
        assertNull(commands.view(result.shipment()).get("carrier_actual_fee"));
        assertTrue(carrierRequest.get().path("carrier_quoted_fee").isNull());
        assertTrue(carrierRequest.get().path("shipping_fee").isMissingNode());
        assertEquals(30000L, eventPayload.get().get("checkout_shipping_fee"));
        assertNull(eventPayload.get().get("carrier_quoted_fee"));
        assertNull(eventPayload.get().get("carrier_quote_delta"));
        assertNull(eventPayload.get().get("carrier_actual_fee"));

        ObjectMapper serializationMapper = MAPPER.copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JsonNode responseJson = serializationMapper.valueToTree(commands.view(result.shipment()));
        JsonNode eventJson = serializationMapper.valueToTree(eventPayload.get());
        assertTrue(responseJson.path("carrier_quoted_fee").isMissingNode());
        assertTrue(responseJson.path("carrier_quote_delta").isMissingNode());
        assertTrue(responseJson.path("carrier_actual_fee").isMissingNode());
        assertTrue(eventJson.path("carrier_quoted_fee").isMissingNode());
        assertTrue(eventJson.path("carrier_quote_delta").isMissingNode());
        assertTrue(eventJson.path("carrier_actual_fee").isMissingNode());
    }

    @Test
    void expiredCarrierQuoteIsRejectedBeforeShipmentReservation() throws Exception {
        quoteResult.set(quote(20000L, "VND", Instant.now().minusSeconds(1)));
        ShipmentException error = assertThrows(ShipmentException.class,
                () -> commands.create("expired-quote-key", request("VND", 30000)));

        assertEquals("SHIPMENT_CARRIER_QUOTE_EXPIRED", error.code());
        verify(shipments, never()).saveAndFlush(any(ShipmentEntity.class));
        verify(adapter, never()).create(anyString(), anyString(), any(JsonNode.class));
    }

    @Test
    void negativeCarrierQuoteIsRejectedRatherThanPersisted() throws Exception {
        quoteResult.set(quote(-1L, "VND", Instant.now().plusSeconds(300)));
        ShipmentException error = assertThrows(ShipmentException.class,
                () -> commands.create("negative-quote-key", request("VND", 30000)));

        assertEquals("SHIPMENT_CARRIER_INVALID_QUOTE", error.code());
        verify(shipments, never()).saveAndFlush(any(ShipmentEntity.class));
    }

    @Test
    void ambiguousCarrierCreateKeepsFeeDistinctionWithoutClaimingActualCost() throws Exception {
        quoteResult.set(quote(20000L, "VND", Instant.now().plusSeconds(300)));
        doThrow(new CarrierTimeoutException("provider outcome unknown"))
                .when(adapter).create(anyString(), anyString(), any(JsonNode.class));

        var result = commands.create("ambiguous-fee-key", request("VND", 30000));

        assertEquals(HttpStatus.ACCEPTED, result.status());
        assertEquals(20000L, result.shipment().getShippingFee());
        assertEquals(30000L, result.shipment().getCheckoutShippingFee());
        assertEquals(20000L, result.shipment().getCarrierQuotedFee());
        assertEquals("shipment.status_changed", eventType.get());
        assertEquals(20000L, eventPayload.get().get("shipping_fee"));
        assertEquals(30000L, eventPayload.get().get("checkout_shipping_fee"));
        assertEquals(20000L, eventPayload.get().get("carrier_quoted_fee"));
        assertNotEquals("shipment.created", eventType.get());
    }

    @Test
    void mockQuoteForFiveHundredGramsIsTwentyThousandVnd() throws Exception {
        var mock = new MockCarrierAdapter();
        JsonNode from = MAPPER.readTree("{\"line1\":\"from\"}");
        JsonNode to = MAPPER.readTree("{\"line1\":\"to\"}");
        List<JsonNode> quoteItems = List.of(MAPPER.readTree("{\"weight_gram\":500,\"quantity\":1}"));

        ShippingQuote quote = mock.quote(from, to, quoteItems);

        assertEquals(20000L, quote.fee());
        assertEquals("VND", quote.currency());
    }

    @Test
    void sameCurrencyRetryIsIdempotentButChangingCurrencyConflicts() throws Exception {
        quoteResult.set(quote(20000L, "VND", Instant.now().plusSeconds(300)));
        var original = request("VND", 30000);
        var first = commands.create("retry-key", original);
        var repeated = commands.create("retry-key", original);

        assertTrue(repeated.replayed());
        assertEquals(first.shipment().getId(), repeated.shipment().getId());
        assertEquals(20000L, repeated.shipment().getShippingFee());
        assertEquals(30000L, repeated.shipment().getCheckoutShippingFee());
        verify(adapter, times(1)).create(anyString(), anyString(), any(JsonNode.class));

        ShipmentException error = assertThrows(ShipmentException.class,
                () -> commands.create("retry-key", request("VND", 25000)));
        assertEquals("SHIPMENT_IDEMPOTENCY_CONFLICT", error.code());
    }

    @Test
    void quoteWithDifferentCurrencyIsRejectedBeforeCreatingShipment() {
        ShipmentException error = assertThrows(ShipmentException.class,
                () -> commands.create("mismatch-key", request("USD", 30000)));

        assertEquals("SHIPMENT_CURRENCY_MISMATCH", error.code());
        verify(shipments, never()).saveAndFlush(any(ShipmentEntity.class));
        verify(adapter, never()).create(anyString(), anyString(), any(JsonNode.class));
    }

    @Test
    void quoteForDifferentCarrierIsRejectedBeforeCreatingShipment() {
        quoteResult.set(new ShippingQuote("GHN", true, 20000L, "VND", 2, 4,
                Instant.now().plusSeconds(300), null));

        ShipmentException error = assertThrows(ShipmentException.class,
                () -> commands.create("wrong-carrier-key", request("VND", 30000)));

        assertEquals("SHIPMENT_CARRIER_MISMATCH", error.code());
        verify(shipments, never()).saveAndFlush(any(ShipmentEntity.class));
        verify(adapter, never()).create(anyString(), anyString(), any(JsonNode.class));
    }

    @Test
    void requestCurrencyMustBePresentAndThreeUppercaseLetters() throws Exception {
        assertTrue(VALIDATOR.validate(request("VND", 30000)).isEmpty());
        assertFalse(VALIDATOR.validate(request(null, 30000)).isEmpty());
        assertFalse(VALIDATOR.validate(request("vnd", 30000)).isEmpty());
        assertFalse(VALIDATOR.validate(request("VNDD", 30000)).isEmpty());
    }

    private ShippingQuote quote(Long fee, String currency, Instant expiresAt) {
        return new ShippingQuote("MOCK", true, fee, currency, 2, 4, expiresAt, null);
    }

    private CreateShipmentRequest request(String currency, long shippingFee) throws Exception {
        return new CreateShipmentRequest(
                "order-1",
                null,
                "shop-1",
                "buyer-1",
                "buyer@example.com",
                "MOCK",
                MAPPER.readTree("{\"line1\":\"from\"}"),
                MAPPER.readTree("{\"line1\":\"to\"}"),
                List.of(new ShipmentItemRequest("sku-1", "product-1", "item", 1, 250, null, null, null)),
                0,
                shippingFee,
                "PREPAID",
                currency);
    }
}
