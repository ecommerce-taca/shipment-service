# Shipment Service API — implementation candidate v0.2

For the Shipment ↔ Order-Commerce REST and event integration contract, including fee ownership, examples, idempotency caveats, event discrepancy and the sequence flow, see [the integration contract](../decisions/order-commerce-contract-patch.md).

Base path `/api/v1`; timestamps ISO-8601 UTC. Success envelope `{data,meta:{request_id}}`; error envelope `{error:{code,message,details,trace_id}}`.

## Authentication

- `/internal/**`: `X-Internal-Service-Token` in this candidate; must be aligned with shared Taca internal token/mTLS policy before integration.
- Buyer/seller tracking: actor headers supplied by trusted Gateway only. Buyer may read only own shipment; seller must have shipment shop in `X-User-Shop-Scope` or appropriate admin permission.
- `/webhooks/shipping/{carrier}`: no JWT; requires configured carrier signature verification and source-IP allowlist. Current generic HMAC-SHA256 implementation is test-only and must not be treated as GHN's real signature contract.

## 1. Buyer tracking

`GET /orders/{orderId}/shipment` requires `X-User-ID` and `X-User-Roles`. Shipment absent → `404 SHIPMENT_NOT_FOUND`; wrong buyer → `403 SHIPMENT_FORBIDDEN`.

## 2. Seller tracking

`GET /seller/orders/{orderId}/shipment` requires trusted `X-User-Shop-Scope`. Scope is checked against the stored shipment shop.

## 3. Checkout quote

`POST /internal/shipping/quote`

Order sends the address snapshots and item data it already owns. This POST body is used instead of the old GET query contract because address snapshots and item arrays should not be placed in a URL/query string.

```json
{
  "from_shop_id": "shop-01912f31",
  "from_address_snapshot": {"line1":"Warehouse A","ward":"Ben Thanh","district":"District 1","province":"Ho Chi Minh City"},
  "to_address_snapshot": {"line1":"12 Nguyen Hue","ward":"Ben Nghe","district":"District 1","province":"Ho Chi Minh City"},
  "items": [{"sku_id":"sku-1","product_id":"product-1","title":"Sample item","quantity":2,"weight_gram":300}],
  "carrier": "MOCK"
}
```

Response contains `fee`, `currency`, `carrier`, `estimate:{min_days,max_days}` and `quote_expires_at`. `fee` may be null when an adapter has no fee value; null is unknown, not zero. This endpoint returns a current estimate only; it does not reserve or issue a reusable quote ID. Create makes a fresh quote from its own snapshots and does not assume the earlier estimate remains valid. The currency is the adapter's quote denomination; comparison results retain each carrier's own currency and are not converted or summed across currencies. No shipment is created. Carrier adapter unavailable → error, not a fabricated quote.

## 4. Seller carrier comparison

`POST /internal/shipping/carrier-quotes`

Order-Commerce must validate that the order is `CONFIRMED`, has no shipment, and belongs to the seller. It then passes `order_id`, `shop_id`, origin/destination snapshots and item data to Shipment. The response includes GHN/SPX/J&T options with `available`, `fee`, `currency`, `estimate` or `unavailable_reason`.

**Ownership adjustment:** the user-facing `GET /seller/orders/{orderId}/shipment/carriers` should be an Order-Commerce façade because Order owns order state and seller authorization. It calls this internal Shipment endpoint; Shipment does not fetch order context by ID or access `orderdb`.

## 5. Create shipment

> **Current Shipment ↔ Order-Commerce create direction:** REST create is the selected contract direction documented for Order-Commerce preparation. The source also contains a separate opt-in Kafka command consumer proposal; it is not the selected create contract and must not be inferred as a required integration.

`POST /internal/shipments`; `Idempotency-Key` is required. The current request DTO names the Order-owned confirmed checkout amount `shipping_fee`; it does not accept `checkout_shipping_fee`. Shipment copies that input unchanged into `checkout_shipping_fee`. Order-Commerce must not interpret a create-time carrier quote as authority to change the confirmed total.

```json
{
  "order_id":"order-01912f91",
  "checkout_group_id":"checkout-01912f90",
  "shop_id":"shop-01912f31",
  "buyer_user_id":"usr-01912f10",
  "buyer_email":"buyer@example.com",
  "carrier":"MOCK",
  "from_address_snapshot":{"contact_name":"Shop","phone":"0900000000","line1":"Warehouse A","ward":"Ben Thanh","district":"District 1","province":"Ho Chi Minh City"},
  "to_address_snapshot":{"contact_name":"Buyer","phone":"0912345678","line1":"12 Nguyen Hue","ward":"Ben Nghe","district":"District 1","province":"Ho Chi Minh City"},
  "items":[{"sku_id":"sku-1","product_id":"product-1","title":"Sample item","quantity":2,"weight_gram":300}],
  "cod_amount":0,
  "shipping_fee":32000,
  "currency":"VND",
  "payment_method":"VNPAY"
}
```

Request `shipping_fee` is the checkout amount Order-Commerce supplies. Shipment preserves it as `checkout_shipping_fee` and does not replace that value with a carrier quote. In responses, `shipping_fee` remains the legacy effective-fee field (create-time quote fee when one is available, otherwise the request fee) for compatibility; it is deprecated for checkout totals and must not be used for payment/refund reconciliation. Consumers should use `checkout_shipping_fee` for the commercial amount and `carrier_quoted_fee` for the separate carrier estimate. `carrier_quote_delta`, when present, is `carrier_quoted_fee - checkout_shipping_fee`. `carrier_actual_fee` is optional and is emitted only after Shipment receives authoritative, verified actual-charge data; no current adapter provides this data, so it is absent. No quote or checkout amount is copied into this field. The explicit fee columns are nullable so pre-migration rows remain valid and their checkout/quote/actual amounts are not guessed. `currency` is required and must be a three-letter uppercase code. It applies to the checkout fee and, when available and matching, to `carrier_quoted_fee`; a mismatch is rejected as `SHIPMENT_CURRENCY_MISMATCH`. A quote identifying a different carrier, returned already expired, or carrying a negative fee is rejected; a zero fee is valid. If the adapter returns no fee, `carrier_quoted_fee` and delta remain unknown; Shipment does not substitute zero or copy the checkout fee into the carrier-fee field. No FX conversion is applied. No quote ID is accepted or reused: the create-time quote is generated from the create request's own address/item snapshots. The current MOCK adapter supplies a local estimate, not a GHN tariff or actual charge. See the integration contract for source-confirmed response examples and migration behavior. `buyer_email` is required to support the Notification email contract. `payment_method` is used to reject a positive COD amount for non-COD orders. The current database constraint is unique on `(order_id, shop_id)`; that represents one shipment per Shop Order only if each Shop Order has one immutable `shop_id`, which remains to be confirmed. Same idempotency key + same body replays the previous result; same key + different body, including a different checkout fee or currency, → `409 SHIPMENT_IDEMPOTENCY_CONFLICT`. If provider result is ambiguous after timeout, response is `202` with `PENDING_RECONCILIATION`; do not blind-retry create.

## 5a. Kafka create consumer status

The opt-in `shipment.commands.v1` consumer and `shipment.creation_pending` / `shipment.creation_failed` result events are implemented as a Shipment-owned proposal, disabled by default, and are **not** the selected Order-Commerce shipment-create contract. The integration direction in this document is REST `POST /internal/shipments` plus Shipment lifecycle events on Kafka. See [the integration contract](../decisions/order-commerce-contract-patch.md) for the boundary and [Kafka implementation notes](../kafka/shipment-events.md) for the proposal's actual source behavior.

## 6. Cancel shipment

`POST /internal/shipments/{shipmentId}/cancel`; `Idempotency-Key` required. Body:

```json
{"reason":"ORDER_CANCELLED","version":1}
```

Only before pickup. Version mismatch or invalid state → `409 SHIPMENT_STATE_INVALID`. Shipment never triggers payment/refund directly.

## 7. Carrier webhook

`POST /webhooks/shipping/{carrier}`. The candidate expects `X-Carrier-Signature` as hex HMAC-SHA256 of the exact raw request body. This is a test-only signature contract until carrier-specific documentation is supplied. Source IP must be in `SHIPMENT_WEBHOOK_ALLOWED_IPS`.

```json
{"external_event_id":"test-event-1","tracking_code":"MOCK123","status":"IN_TRANSIT","occurred_at":"2026-10-11T10:00:00Z","note":"Đang trung chuyển"}
```

Duplicate event is ACKed without reapplying. Unknown carrier status is ignored safely; stale/invalid transition cannot regress canonical state. Only authenticated carrier webhook can set `DELIVERED`.

## 8. Health

- `GET /health/live`: process liveness.
- `GET /health/ready`: verifies database and Kafka topic configuration. Current candidate does not actively probe Kafka broker reachability; add a broker probe before production readiness checks.

## Errors

`SHIPMENT_INVALID_INPUT` 400; `SHIPMENT_UNAUTHENTICATED` 401; `SHIPMENT_FORBIDDEN` 403; `SHIPMENT_NOT_FOUND` 404; `SHIPMENT_STATE_INVALID` 409; `SHIPMENT_ALREADY_EXISTS` 409; `SHIPMENT_CARRIER_UNAVAILABLE_FOR_ORDER` 400; `SHIPMENT_CARRIER_UNAVAILABLE` 503; `SHIPMENT_CARRIER_TIMEOUT` 504; `SHIPMENT_CARRIER_INVALID_QUOTE` 502; `SHIPMENT_CARRIER_MISMATCH` 502; `SHIPMENT_CURRENCY_MISMATCH` 400; `SHIPMENT_WEBHOOK_INVALID` 400; `SHIPMENT_IDEMPOTENCY_CONFLICT` 409; `SHIPMENT_TRACKING_CONFLICT` 409; `SHIPMENT_INTERNAL_ERROR` 500.
