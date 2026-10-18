# Order-Commerce ↔ Shipment Service integration contract

**Document status (2026-10-18):** Shipment-side contract and source behavior reference. Order-Commerce is not implemented in this workspace, so cross-service integration is unverified. The REST quote/create flow below is the agreed integration direction for preparation. Kafka shipment creation is not the selected contract; an opt-in Shipment-owned command consumer exists in source but remains a separate, unapproved proposal.

## 1. Ownership and boundaries

| Data or decision | Owner | Contract rule |
|---|---|---|
| Order lifecycle, checkout total, and commercially confirmed shipping fee | Order-Commerce | Order-Commerce chooses and confirms the customer amount. Shipment cannot change it, collect a difference, or trigger a refund. |
| Carrier quote and shipment/tracking lifecycle | Shipment Service | Shipment records a create-time quote separately when the adapter returns one and owns carrier shipment state. |
| Carrier actual charge | Carrier evidence, recorded by Shipment only after verification | A quote or checkout amount is not actual carrier cost. No current adapter provides an authoritative actual charge. |

`checkout_shipping_fee` is the canonical cross-service name for the confirmed commercial amount in Shipment responses/events. The current REST create request and current opt-in command payload still call this input `shipping_fee`; neither currently accepts a request field named `checkout_shipping_fee`. The request spelling is retained for compatibility and is a known naming inconsistency, not permission for Shipment to replace the amount.

## 2. Contract maturity

| Classification | Items |
|---|---|
| Implemented and source/test verified | Internal quote REST routes, REST shipment create/read/cancel routes, request validation, fee persistence fields in V005/V006, create success Outbox payload, event envelope, at-least-once publication, local MOCK quote behavior, GHN fail-closed behavior. |
| Shipment-side contract decision; cross-service integration not yet verified | Order-Commerce owns checkout fee and order state; REST quote then REST create; Shipment reports checkout and carrier quote separately; Shipment does not collect or adjust customer money. |
| Implemented but proposal-only; not selected for Order-Commerce shipment creation | Opt-in `shipment.commands.v1` create consumer, worker result events `shipment.creation_pending` and `shipment.creation_failed`. The consumer is disabled by default and is not a replacement for the REST create contract here. |
| Needs decision/owner confirmation before integration | Order/Shop ID uniqueness mapping, REST create retry/reconciliation polling and order projection, allowed delta/customer policy, event consumer compatibility, auth transport and trusted identity boundary, GHN carrier behavior, handling of incomplete fee fields in historical/reconciled events. |

## 3. Internal REST API

Base path is `/api/v1`. Internal endpoints require `X-Internal-Service-Token`; Order-Commerce must call over a trusted internal network. The token filter is implemented, but TLS/mTLS/shared service-token integration and network isolation are deployment concerns and are not proven by the Shipment source.

### 3.1 Get a checkout estimate

`POST /api/v1/internal/shipping/quote`

Request type: `QuoteRequest`.

```json
{
  "from_shop_id": "shop-123",
  "from_address_snapshot": {"line1":"Warehouse"},
  "to_address_snapshot": {"line1":"Buyer address"},
  "items": [
    {"sku_id":"sku-1","product_id":"product-1","title":"Item","quantity":1,"weight_gram":500}
  ],
  "carrier": "MOCK"
}
```

`from_shop_id`, both address snapshots and a non-empty list of up to 100 valid item rows are required by the DTO. Carrier is optional in source; when omitted, source selects GHN. Invalid input returns `400 SHIPMENT_INVALID_INPUT`; unavailable/unconfigured carrier returns a Shipment error (GHN currently fails closed).

Success is `200` with `{data,meta}`:

```json
{
  "data": {
    "fee": 20000,
    "currency": "VND",
    "carrier": "MOCK",
    "estimate": {"min_days":1,"max_days":3},
    "quote_expires_at": "2026-10-18T10:00:00Z"
  },
  "meta": {"request_id":"request-id"}
}
```

This example illustrates the local MOCK result; it is not a GHN tariff. `fee` can be null (unknown, not zero). This endpoint does not create/hold a shipment or issue a reusable quote ID. For the MOCK adapter the quote supplies an expiry; the default non-MOCK quote service adds its configured TTL to the adapter result. Do not treat an estimate as a guaranteed or locked price.

### 3.2 Compare carriers

`POST /api/v1/internal/shipping/carrier-quotes`

Request type: `CarrierQuotesRequest`. Source requires `order_id`, `shop_id`, both address snapshots, and 1–100 valid item rows. The caller must supply trusted order/shop/address/item snapshots. This endpoint does not authorize seller/order state itself and does not query an Order database.

Success is `200` with a top-level data object containing `order_id`, `carriers`, and `quote_expires_at`. Each configured available carrier result includes `carrier`, `available`, `fee` (which may be null), `currency`, and optional estimate. An unavailable result includes `carrier`, `available:false`, and `unavailable_reason`. Source compares GHN, SPX, J&T; adapters not configured are reported unavailable. Values in different currencies are not converted or summed.

The seller-facing route, if needed, belongs to Order-Commerce: authenticate seller, verify order state and shop ownership, build snapshots, then call this internal endpoint. The route `GET /api/v1/seller/orders/{orderId}/shipment/carriers` is not implemented by Shipment.

### 3.3 Create shipment

`POST /api/v1/internal/shipments`

Required headers:

```http
X-Internal-Service-Token: <internal-token>
Idempotency-Key: <stable-create-key>
X-Request-ID: <request-id>   # optional
```

Request type: `CreateShipmentRequest`. Order-Commerce supplies immutable order/shop/buyer/address/item snapshots, selected carrier, COD amount/payment method, checkout-confirmed fee using the current input field `shipping_fee`, and explicit three-uppercase-letter `currency`.

```json
{
  "order_id":"order-123",
  "checkout_group_id":"checkout-123",
  "shop_id":"shop-123",
  "buyer_user_id":"buyer-123",
  "buyer_email":"buyer@example.com",
  "carrier":"MOCK",
  "from_address_snapshot":{"line1":"Warehouse"},
  "to_address_snapshot":{"line1":"Buyer address"},
  "items":[{"sku_id":"sku-1","product_id":"product-1","title":"Item","quantity":1,"weight_gram":500}],
  "cod_amount":0,
  "shipping_fee":30000,
  "currency":"VND",
  "payment_method":"PREPAID"
}
```

Required fields and DTO limits are enforced by bean validation; address snapshots must be JSON objects no larger than 16 KiB each, and the item list must contain 1–100 rows. Amounts must be non-negative. A positive `cod_amount` requires `payment_method` equal to `COD` (case-insensitive). `buyer_email` is required by the current DTO. `currency` must match `[A-Z]{3}`. The create-time quote, if it has a fee, must identify the selected carrier, not be expired, have a non-negative fee, and use the request currency. A missing quote fee is retained as unknown. Shipment does not compare quote amount to checkout amount as a rejection condition and does not change the checkout amount.

Success response uses `{data,meta}` and the same view for create and lookup. First confirmed create returns `201`; a completed idempotent replay or existing shipment returns `200`; a pending carrier outcome returns `202` with status `PENDING_RECONCILIATION`. Fee mapping and legacy behavior are in §4. Create returns an error envelope for invalid input, authentication, unavailable carrier, quote/currency errors, idempotency conflict, uniqueness conflict, or internal failure. Relevant source codes include:

| Condition | Status/code |
|---|---|
| DTO/custom validation | `400 SHIPMENT_INVALID_INPUT` |
| Missing/invalid internal token | `401 SHIPMENT_UNAUTHENTICATED` |
| Carrier unavailable for this order | `400 SHIPMENT_CARRIER_UNAVAILABLE_FOR_ORDER` |
| Carrier adapter unavailable | `503 SHIPMENT_CARRIER_UNAVAILABLE` |
| Quote expired | `502 SHIPMENT_CARRIER_QUOTE_EXPIRED` |
| Quote absent/invalid identity or negative fee | `502 SHIPMENT_CARRIER_INVALID_QUOTE` |
| Quote identifies another carrier | `502 SHIPMENT_CARRIER_MISMATCH` |
| Quote fee currency differs from request | `400 SHIPMENT_CURRENCY_MISMATCH` |
| Same idempotency key with different request hash | `409 SHIPMENT_IDEMPOTENCY_CONFLICT` |
| Existing shipment violates `(order_id,shop_id)` uniqueness | `409 SHIPMENT_ALREADY_EXISTS` |
| Ambiguous carrier create result | `202`, status `PENDING_RECONCILIATION` |

### 3.4 Idempotency and existing-shipment behavior

For the same `Idempotency-Key` and `CREATE` operation, Shipment stores a canonical request hash and a 24-hour expiry. Same key and same request replays the linked shipment (`200` when complete, `202` while pending); same key and a different body, including another checkout fee/currency, returns `409 SHIPMENT_IDEMPOTENCY_CONFLICT`. A timeout after Shipment has reserved/processed the request may mean the carrier outcome is unknown; retry only with the same key and same body.

**Source limitation to resolve:** before making a new carrier call, source also returns an existing row found by `(order_id, shop_id)`, even if it was requested with a different idempotency key and different request body. That lookup does not compare the new request to the original. Therefore consumers must not interpret `200` for an existing shipment as proof that a changed request (including changed checkout fee) was accepted. Confirm desired conflict behavior and fix it in a separately approved implementation change.

### 3.5 Read and cancel

- `GET /api/v1/orders/{orderId}/shipment`: trusted Gateway actor context; buyer reads own shipment. Returns the full view including fee fields and timeline.
- `GET /api/v1/seller/orders/{orderId}/shipment`: trusted Gateway seller context and shop scope/admin permission; returns the same full view.
- `POST /api/v1/internal/shipments/{shipmentId}/cancel`: internal token, `Idempotency-Key`, request `{reason,version}`. Shipment may cancel only while domain state permits (before pickup); status change and `shipment.cancelled` event are persisted through the current domain flow. It does not update Order state or payment/refund.

## 4. Fee field mapping

Fee values are non-negative integer amounts paired with the supplied three-letter currency code; source does not define whether the numeric scale is major or minor currency units. The integration owners must explicitly confirm the amount scale (especially for currencies with fractional units) before cross-service accounting. Current local examples use VND; there is no FX conversion. The response/event `currency` is the shipment/request currency.

| API/event field | Domain/entity | Database | Authority and when known | Unknown/compatibility rule |
|---|---|---|---|---|
| Create request `shipping_fee` | `CreateShipmentRequest.shipping_fee`; copied to `ShipmentEntity.checkoutShippingFee` | `shipments.checkout_shipping_fee` (V005, nullable for historical compatibility) | Order-Commerce is authoritative; known at checkout confirmation and create request | Integer scale must be agreed across services. Do not replace with quote. Current create request does not accept `checkout_shipping_fee`; use request `shipping_fee` until a versioned contract change. |
| `checkout_shipping_fee` | `ShipmentEntity.checkoutShippingFee` | `shipments.checkout_shipping_fee` | Same commercial amount supplied by Order-Commerce | Null for rows predating V005; do not infer from legacy field. New consumers use this for checkout totals. |
| `carrier_quoted_fee` | `ShipmentEntity.carrierQuotedFee` | `shipments.carrier_quoted_fee` (V005, nullable) | Carrier adapter's create-time quote, if returned | Null means unknown; not zero and not checkout fee. Not proof of actual charge. |
| `carrier_actual_fee` | `ShipmentEntity.carrierActualFee` | `shipments.carrier_actual_fee` (V006, nullable) | Only an authoritative, verified actual charge from carrier evidence | No current source path sets it; absent from current API/event JSON. Never copy quote or checkout amount. |
| `carrier_quote_delta` | Derived when viewing entity or creating event payload | Not stored | `carrier_quoted_fee - checkout_shipping_fee` when both are known | Omitted/null if either is unknown. Not an amount Shipment charges or settles. |
| `shipping_fee` legacy | `ShipmentEntity.shippingFee` | Existing non-null `shipments.shipping_fee` | Legacy effective fee: create-time quote when non-null, otherwise request fee | Retained in response/event for compatibility and deprecated. Never use as checkout source of truth. Historical values are preserved; they cannot reliably identify whether original value was checkout or carrier quote. |

V005 adds nullable checkout and quoted fee columns; V006 adds nullable actual fee. Neither backfills old rows. JPA maps the new columns as nullable wrappers. Source applies Flyway at application startup, but this repository review does not establish whether any local or deployed database has applied either migration. See [DB notes](../db/shipment.md).

## 5. Kafka lifecycle event contract

Shipment writes events to the configured topic, default `shipment.events.v1`, through the transactional Outbox. Event `event_key` is the shipment aggregate ID for shipment lifecycle events. The publisher sends the stored JSON envelope and is at-least-once; it does not provide exactly-once delivery.

The source envelope is:

```json
{
  "event_id":"<uuidv7>",
  "schema_version":1,
  "event_type":"shipment.created",
  "occurred_at":"<ISO-8601 instant>",
  "aggregate_type":"SHIPMENT",
  "aggregate_id":"<shipment-id>",
  "correlation_id":"<optional>",
  "causation_id":"<optional>",
  "payload":{}
}
```

`correlation_id` and `causation_id` are omitted when absent. The stored envelope, including `event_id`, remains the same on publisher retry. Consumers should persist/deduplicate by `event_id` before applying projections and make applying the event idempotent. This is consumer guidance; no Order-Commerce consumer exists here.

Shipment lifecycle event types emitted by source include `shipment.created`, `shipment.status_changed`, `shipment.delivered`, `shipment.failed`, and `shipment.cancelled`. Delivery `shipment.failed` is not a shipment-creation failure. A confirmed regular REST create appends `shipment.created` after carrier create returns valid identifiers and after the shipment row/status and event are saved in the same database transaction. The transaction commits before the REST success response; the publisher later delivers the committed Outbox envelope.

**Confirmed discrepancy:** the separate positive reconciliation path also emits `shipment.created`, but its current payload is a reduced map and omits all fee fields, including `checkout_shipping_fee`, `carrier_quoted_fee`, `carrier_quote_delta`, and legacy `shipping_fee`. The regular REST success payload has those fee fields. Consumers must tolerate their absence for now; uniform event fee mapping after reconciliation needs an implementation fix before it can be guaranteed. This document does not change source code.

For the regular create and other lifecycle events built with the shared payload builder, fees follow §4. `carrier_actual_fee` is omitted unless an entity value exists; current source has no path that sets it. The REST ambiguity path emits `shipment.status_changed` with `PENDING_RECONCILIATION`, not the create-command-specific `shipment.creation_pending`.

The opt-in `shipment.commands.v1` consumer and its proposed `shipment.creation_pending`/`shipment.creation_failed` results are described in [Kafka implementation notes](../kafka/shipment-events.md). They are not the selected Order-Commerce create contract here, are disabled by default, and require separate agreement before use. The target integration flow remains REST create plus lifecycle events.

## 6. Integration flow

<!-- mermaid-checked: every participant uses `participant Id as "Label"`, no \n in aliases/messages/notes, every alt/opt/loop closed by end, no `:` inside any alias -->
~~~mermaid
sequenceDiagram
    participant Order as "Order-Commerce"
    participant Quote as "Shipment Quote API"
    participant Create as "Shipment Create API"
    participant DB as "Shipment Database"
    participant Carrier as "Selected Carrier Adapter"
    participant Outbox as "Outbox Publisher"
    participant Kafka as "shipment.events.v1"

    Order->>Quote: POST quote with owned snapshots and items
    Quote->>Carrier: Calculate estimate
    Carrier-->>Quote: Fee currency estimate expiry
    Quote-->>Order: Quote response
    Note over Order: Order decides checkout total and confirms commercial fee
    Order->>Create: POST create with request shipping_fee and stable Idempotency-Key
    Create->>DB: Reserve idempotency and PENDING_RECONCILIATION row
    Create->>Carrier: Create shipment with idempotency key
    alt Carrier confirms create
        Carrier-->>Create: External shipment ID and tracking code
        Create->>DB: Save CREATED shipment and Outbox envelope in one transaction
        Create-->>Order: HTTP 201 plus explicit fee fields
    else Carrier result ambiguous or client times out
        Create->>DB: Preserve pending reconciliation state and Outbox status event
        Create-->>Order: HTTP 202 when response reaches caller
        Note over Order: On client timeout retry same key and identical body; do not assume failure
    end
    Outbox-)Kafka: Publish committed envelope at least once
    Kafka-->>Order: shipment.created or lifecycle event
    Note over Order: Deduplicate by event_id and update Order-owned state only by agreed policy
~~~

## 7. Failure and retry handling responsibilities

- **Quote expired before create:** create rejects an expired quote returned by its own create-time adapter call. Shipment has no quote ID and does not know whether the earlier checkout estimate expired. Order owns the policy for whether to request a fresh quote or proceed with its already-confirmed amount.
- **Quote missing a fee:** value remains unknown; Shipment stores no carrier quote amount and computes no delta. Checkout fee remains the Order-supplied amount.
- **Quote differs from checkout fee:** both values are retained separately if currency matches and quote is otherwise valid. Shipment does not adjust or collect the difference. Any customer-facing, margin, or settlement policy belongs to Order/owners and remains undecided here.
- **Carrier create timeout/ambiguous response:** source returns `202` if the request is still active and transitions to reconciliation; if the network times out before the HTTP response arrives, caller cannot infer the outcome. Retry the identical request with the same key. Do not issue a new key to force a second carrier create.
- **Kafka event duplicate:** deduplicate by stable `event_id`. Outbox delivery is at-least-once, so duplicate publication is possible.
- **Order state:** Order-Commerce alone owns its order projection. Whether the Order moves to a fulfillment-pending state on HTTP 202, polls Shipment, or waits for a particular event must be confirmed by Order owners; do not mark shipped solely from a request being accepted.
- **Cancellation:** Shipment's internal cancel route or its opt-in `order.cancelled` consumer can result in `shipment.cancelled` when domain/carrier conditions permit. Who calls which path and the Order state transition need cross-service confirmation.
- **Actual fee:** no source event/API contract currently supplies verified carrier actual charges. Remains unknown until an agreed authoritative source is implemented.

## 8. Decisions required before integration

1. Confirm Order's shop-order identity mapping and whether one order has exactly one immutable `shop_id`; Shipment uniqueness is `(order_id, shop_id)`, while buyer/seller lookup uses `order_id`.
2. Confirm whether REST create request keeps `shipping_fee` as its input field indefinitely or gets a versioned/compatible `checkout_shipping_fee` input alias. Do not silently rename it.
3. Confirm Order's handling of `200` existing-shipment response where request body may differ, and correct the source behavior noted in §3.4 before relying on conflict semantics across different keys.
4. Agree the Order state transition and recovery strategy for REST `202`/client timeout, including whether Order polls shipment lookup or consumes events.
5. Ensure all `shipment.created` producer paths include the same fee payload, including positive reconciliation; current source does not.
6. Confirm event topic/schema compatibility, consumer event deduplication storage, and treatment of old events where only `shipping_fee` exists or new fee fields are absent.
7. Define policy for a carrier quote above/below the checkout amount; Shipment does not decide customer charge, refund, or settlement.
8. Confirm internal token/mTLS, TLS, network boundaries, and request-ID/correlation propagation.
9. Implement and verify GHN API, quote, idempotency, reconciliation, and authoritative actual-fee contracts before claiming real carrier integration.
