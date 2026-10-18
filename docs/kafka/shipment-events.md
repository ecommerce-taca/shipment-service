# Kafka contracts — Shipment Service

> **Scope/status:** `shipment.events.v1` lifecycle event envelope and Outbox behavior below describe current Shipment source. The opt-in `shipment.commands.v1` create-command section is a separate Shipment-owned proposal, not the selected Order-Commerce create contract; REST `POST /api/v1/internal/shipments` is the create direction in [the integration contract](../decisions/order-commerce-contract-patch.md). Order-Commerce/platform have not verified integration or approved the command proposal.

## Outgoing topic

`shipment.events.v1` — shipment lifecycle events use shipment ID as the partition key. The proposed `shipment.creation_failed` event has no Shipment aggregate, so current source uses `command_id` as its key (`aggregate_type=SHIPMENT_COMMAND`, `aggregate_id=command_id`). This key exception is part of the Shipment proposal and needs Order-Commerce/platform confirmation. Event envelope follows the Taca shared pattern:

```json
{
  "event_id": "019...",
  "schema_version": 1,
  "event_type": "shipment.created",
  "occurred_at": "2026-10-11T10:00:00Z",
  "aggregate_type": "SHIPMENT",
  "aggregate_id": "019...",
  "correlation_id": "req-019...",
  "causation_id": "cmd-019...",
  "payload": {
    "shipment_id": "019...",
    "order_id": "order-...",
    "shop_id": "shop-...",
    "carrier": "MOCK",
    "tracking_code": "...",
    "status": "CREATED",
    "shipping_fee": 20000,
    "checkout_shipping_fee": 32000,
    "carrier_quoted_fee": 20000,
    "carrier_quote_delta": -12000,
    "currency": "VND"
  }
}
```

Existing event names and envelope fields (`event_id`, `event_type`) are retained for compatibility. Optional `correlation_id` and `causation_id` are added without renaming existing fields. Event payload is persisted in the Outbox once, so its `event_id` and metadata remain stable when the same event is republished. Consumers must deduplicate by `event_id`; delivery remains at-least-once.

For fee semantics, `payload.shipping_fee` retains its legacy effective-fee behavior (create-time quote when available, otherwise the request amount) for compatibility and is deprecated for checkout totals. Consumers must use `checkout_shipping_fee` for the checkout-confirmed commercial amount and `carrier_quoted_fee` for the separate carrier quote. `carrier_quote_delta` is `carrier_quoted_fee - checkout_shipping_fee` only when both values are known. If the adapter does not return a valid quote fee, the quote/delta remain absent; no zero or copied checkout amount is fabricated. `carrier_actual_fee` is optional and may be emitted only when authoritative verified actual-charge data exists; no current adapter provides it, and Shipment never copies the quote or checkout amount into that field. Existing rows created before migration may have null explicit fee fields because their original checkout/quote/actual values cannot be reconstructed reliably. The event type, topic and envelope/schema version remain unchanged; the new fields are additive, and consumers should migrate from the deprecated field before using fee data for accounting.

Existing event types: `shipment.created`, `shipment.status_changed`, `shipment.delivered`, `shipment.failed`, `shipment.cancelled`. Proposed create-command result types are `shipment.creation_pending` and `shipment.creation_failed`; these are new and require Order-Commerce/consumer approval. `shipment.failed` continues to mean delivery failure only. `shipment.delivered` and `shipment.failed` need `payload.buyer.user_id` and `payload.buyer.email` for Notification email templates. The current REST `CreateShipmentRequest` requires a valid `buyer_email`; whether the future Kafka command must carry this snapshot or use another agreed source remains part of the command contract.

`shipment.created` is emitted after the carrier confirms creation with a nonblank external shipment ID and tracking code and Shipment commits the result. Normal REST create uses the shared fee-bearing payload. Positive reconciliation also emits `shipment.created`, but source currently uses a reduced payload that omits every fee field, including `checkout_shipping_fee`, `carrier_quoted_fee`, `carrier_quote_delta`, and legacy `shipping_fee`; consumers must tolerate absence on that path until an implementation fix makes payloads uniform. `shipment.creation_pending` communicates an unresolved outcome only on the opt-in create-command worker; a REST create ambiguity emits `shipment.status_changed` with `PENDING_RECONCILIATION`. `shipment.creation_failed` is reserved for the opt-in command proposal. Neither command result event replaces the REST create contract. Exhausted command infrastructure retries are parked for operator review and are not asserted to be a business failure.
Repeated carrier notifications that map to the already-current canonical status are retained in the Shipment timeline, but do not emit another `shipment.status_changed` event because no lifecycle state changed.

Outbox publishes at-least-once. Consumers must deduplicate on `event_id`. Each publisher claims one eligible record using a short MySQL transaction with `SELECT ... FOR UPDATE SKIP LOCKED`, then releases the row lock before waiting for Kafka. Claims are ordered by aggregate `created_at` and ID, and a later record by that ordering is held while an earlier record remains `PENDING` or `DLQ_PENDING`. This prevents concurrent publishers from overtaking pending rows under that ordering; it is not a strict causal sequence because `created_at` is assigned by application clocks and there is no aggregate sequence number. It also does not provide global ordering or prevent duplicates. A 60-second lease (configurable with `SHIPMENT_OUTBOX_LEASE_SECONDS`) allows recovery after a worker stops; writes are fenced by the lease token and expiry. A stale worker cannot mark a record complete after another worker reclaims it. The publisher makes at most three main-topic send attempts total (initial attempt plus two retries, with 2s then 4s retry delays); after the third failure it moves the record to `DLQ_PENDING`. Dead-letter delivery also gets at most three attempts total (initial plus two retries, 2s then 4s). `DEAD` means processing is terminal; if all dead-letter attempts fail, the record remains in the database for operator recovery. A worker can still publish successfully and lose its lease or fail before recording the result; consumers must deduplicate on `event_id`. Topic provisioning is centralized; this service does not auto-create topics. `shipment.events.dlq.v1` is configurable and must be created by platform operations.

For rollout, apply the additive lease migration, drain or disable every pre-lease publisher instance, and only then enable publishers on the new version. Older instances do not honor lease columns and can still publish records claimed by the new version.

When the publisher is enabled, startup validates Kafka producer durability settings: `acks=all`, idempotence enabled, retries enabled, and at most five in-flight requests. The configured request timeout must be below Kafka's delivery timeout; `SHIPMENT_KAFKA_SEND_TIMEOUT_MS` must exceed that delivery timeout and remain below the configured outbox lease. Defaults are 5s, 10s, 15s, and a 60s lease respectively. If the waiting thread is interrupted, the publisher restores its interrupt flag, stops the current batch, and leaves the lease in place; recovery occurs after lease expiry rather than scheduling an early retry while the send outcome remains unknown. A timeout remains an ambiguous publish outcome: Kafka may accept a record even when the publisher times out, so outbox retry is at-least-once and downstream consumers must deduplicate by `event_id`. These producer settings do not guarantee broker replica durability unless the Kafka cluster's `min.insync.replicas` and replication policy are configured appropriately.

## Incoming order events

`order.events.v1`, event `order.cancelled`. Consumer is opt-in in local profile and enabled with `SHIPMENT_KAFKA_CONSUMER_ENABLED=true`. It attempts to cancel the shipment only while cancellation is allowed (before pickup). It does not create shipment from `order.created` and does not mutate order state.

Only `order.cancelled` records are subject to the consumer's 256 KiB UTF-8 limit; other event types on the shared topic are ignored. An accepted cancellation is first stored as a durable intent before its Kafka offset can be committed. If it arrives before create, the intent remains as a tombstone and prevents a later create command for the same Shop Order. If it arrives while a shipment is awaiting carrier reconciliation, cancellation waits for a definitive carrier result. Cancellation business processing is retried independently from create-command delivery.
If the incoming `order.cancelled` envelope contains `correlation_id`, it is persisted with the intent and copied to the resulting Shipment event; the source `event_id` is used as `causation_id`. W3C trace headers are not currently propagated.

## Proposed create-shipment command

Topic `shipment.commands.v1`; key is `shop_order_id`. Envelope:

```json
{
  "message_id": "cmd-01912f91",
  "message_type": "shipment.create",
  "schema_version": 1,
  "occurred_at": "2026-10-18T05:00:00Z",
  "correlation_id": "req-01912f90",
  "causation_id": "evt-order-processing-01912f90",
  "payload": {
    "command_id": "cmd-01912f91",
    "idempotency_key": "fulfill-order-01912f91-v1",
    "shop_order_id": "order-01912f91",
    "checkout_group_id": "checkout-01912f90",
    "shop_id": "shop-01912f31",
    "buyer": { "user_id": "usr-01912f10", "email": "buyer@example.com" },
    "carrier": "GHN",
    "from_address_snapshot": { "line1": "Warehouse A", "ward": "Ben Thanh", "district": "District 1", "province": "Ho Chi Minh City" },
    "to_address_snapshot": { "line1": "12 Nguyen Hue", "ward": "Ben Nghe", "district": "District 1", "province": "Ho Chi Minh City" },
    "items": [{ "sku_id": "sku-1", "product_id": "product-1", "title": "Sample item", "quantity": 2, "weight_gram": 300 }],
    "cod_amount": 0,
    "shipping_fee": 32000,
    "payment_method": "VNPAY",
    "currency": "VND"
  }
}
```

`message_id` must equal `payload.command_id`; the command ID is the durable inbox identity. `idempotency_key` is stable across retries and is passed to the carrier adapter. In the command payload, `shipping_fee` is the checkout-confirmed commercial amount provided by Order-Commerce. Shipment stores it explicitly as `checkout_shipping_fee`; the create-time carrier quote, when available, is saved as `carrier_quoted_fee` and does not change the order amount. For compatibility, resulting response/event `shipping_fee` retains its prior effective-fee meaning (quote when available, otherwise request amount); consumers must not use that deprecated field for checkout accounting. `currency` is required and must be a three-letter uppercase code. It is included in the durable payload hash and in the Shipment request hash; reusing an idempotency key with a different fee or currency is a conflict. No currency conversion is performed. The current MOCK quote is a local estimate denominated in VND, not evidence of GHN pricing or actual charges. GHN remains fail-closed until its API/quote contract is implemented. `shop_order_id` maps to Shipment's existing `order_id` and the Kafka key must exactly equal it. The existing database constraint is `(order_id, shop_id)`, so it guarantees one shipment per Shop Order only if the Shop Order has exactly one immutable `shop_id`; confirm this invariant with Order-Commerce before cutover. This mapping is a proposal pending Order-Commerce confirmation.

The consumer accepts command records up to 256 KiB (UTF-8) and validates schema and request constraints, then commits the raw envelope, canonical payload hash, source topic/partition/offset, IDs and `ACCEPTED` state to MySQL before returning to the listener container. Record acknowledgement is after listener return. A duplicate with the same ID/hash is harmless; same ID with a different payload is sent to the consumer DLT. A scheduled worker claims accepted work with a short MySQL transaction and lease, then calls the existing `ShipmentCommandService` outside that transaction. The 256 KiB limit is a Shipment-side proposal; platform broker/topic limits must be aligned.

### Proposed command outcome examples

Confirmed creation emits the existing `shipment.created` event only after carrier confirmation and database commit. Its payload retains `shipment_id`, `order_id` (the Shop Order ID), `shop_id`, `carrier`, `tracking_code`, and `status=CREATED`.

Unresolved carrier outcome:

```json
{
  "event_id": "019...",
  "schema_version": 1,
  "event_type": "shipment.creation_pending",
  "occurred_at": "2026-10-18T05:00:05Z",
  "aggregate_type": "SHIPMENT",
  "aggregate_id": "shipment-019...",
  "correlation_id": "req-01912f90",
  "causation_id": "cmd-01912f91",
  "payload": {
    "command_id": "cmd-01912f91",
    "shop_order_id": "order-01912f91",
    "shop_id": "shop-01912f31",
    "shipment_id": "shipment-019...",
    "status": "PENDING_RECONCILIATION",
    "reason_code": "CARRIER_OUTCOME_AMBIGUOUS"
  }
}
```

Known permanent rejection:

```json
{
  "event_id": "019...",
  "schema_version": 1,
  "event_type": "shipment.creation_failed",
  "occurred_at": "2026-10-18T05:00:05Z",
  "aggregate_type": "SHIPMENT_COMMAND",
  "aggregate_id": "cmd-01912f91",
  "correlation_id": "req-01912f90",
  "causation_id": "cmd-01912f91",
  "payload": {
    "command_id": "cmd-01912f91",
    "shop_order_id": "order-01912f91",
    "shop_id": "shop-01912f31",
    "error_code": "SHIPMENT_INVALID_INPUT",
    "retryable": false
  }
}
```

These event names/payloads are proposal-only until downstream consumers approve them. Schema-invalid or unparseable records go to the configured Kafka consumer DLT with original Kafka metadata. A valid command with transient infrastructure failure gets up to five scheduled retries after the initial attempt using exponential delay with jitter; after exhaustion it is retained in `shipment_command_dlq` as `DEAD` for privileged replay. No `shipment.creation_failed` is emitted for unresolved infrastructure outcomes.

### Retry, DLQ and replay ownership

- Kafka listener delivery retries (default 3 retries, exponential backoff) handle failure to durably accept an inbound record. Exhausted/permanent records are published unchanged to `shipment.consumer.dlq.v1` by default; original topic/partition/offset and exception metadata are added by Spring Kafka.
- The database-backed command worker retries transient processing failures separately (five retries after initial attempt). Carrier timeout, HTTP 408/409/425/429, 5xx, and malformed create results are not blindly retried as create calls; they enter `PENDING_RECONCILIATION`.
- Outbox event publishing has a separate existing policy: three main-topic attempts, then configured Outbox DLQ retries.
- Shipment owns command validation/business processing and durable state. Platform owns topic creation, ACLs, broker retention and Kafka DLT operation. Order-Commerce/platform must agree on command/DLT ownership and replay procedure.
- Database `DEAD` command replay uses `POST /api/v1/internal/ops/shipment-create-commands/{commandId}/replay`; cancellation processing parked in `NEEDS_OPERATOR` uses `POST /api/v1/internal/ops/order-cancellations/{orderId}/replay`. Both require the internal token, a separate operator token and trusted operator identity/role. Replay reuses the original idempotency key and is audited. Kafka consumer DLT replay remains a platform operation.

## Open integration checks

- Confirm shared Kafka header propagation for `traceparent`, `request_id`, and `event_id` in the platform producer/consumer conventions.
- Confirm proposed command topic, key, schema, result event types, DLT topic, retry and replay policy with Order-Commerce/platform before production.
- Confirm Notification's exact required payload fields and whether buyer email should be passed by Order at shipment creation or resolved by Notification.
