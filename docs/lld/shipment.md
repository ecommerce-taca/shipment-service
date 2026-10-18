# LLD — Shipment Service v0.2

## Responsibility

Shipment owns carrier adapters, shipment/tracking state, timeline, carrier webhook handling and outgoing `shipment.events.v1`. Order-Commerce owns order state, seller authorization, address/item source data and the decision to begin fulfillment. Shipment never reads `orderdb` or Auth User DB.

## REST and Kafka split

- REST: quote, shipment create, cancel, buyer/seller tracking reads.
- Kafka outbox: lifecycle events `shipment.created`, `shipment.status_changed`, `shipment.delivered`, `shipment.failed`, and `shipment.cancelled`.
- The selected Shipment ↔ Order-Commerce create direction is REST `POST /api/v1/internal/shipments`; Shipment later publishes lifecycle events. The API payload uses `shipping_fee` as the input name for the Order-owned checkout amount and maps it to `checkout_shipping_fee`.
- The opt-in `shipment.commands.v1` create consumer and `shipment.creation_pending` / `shipment.creation_failed` events are a separate Shipment-owned proposal, disabled by default and not selected as the Order-Commerce integration contract. The `order.cancelled` consumer is also opt-in. See `docs/decisions/order-commerce-contract-patch.md`.

## Quote contract ownership

Order-Commerce owns order state and seller scope. It passes full snapshots/items to Shipment over internal REST:
- `POST /internal/shipping/quote` for checkout estimate.
- `POST /internal/shipping/carrier-quotes` for seller carrier comparison.
The user-facing `GET /seller/orders/{orderId}/shipment/carriers` should be an Order-Commerce façade that checks `CONFIRMED`/no-shipment and then calls Shipment. This avoids Shipment trying to reconstruct order context from an ID.

## State rules

Canonical states: `CREATED`, `PICKED_UP`, `IN_TRANSIT`, `DELIVERED`, `FAILED`, `CANCELLED`, `PENDING_RECONCILIATION`. `NOT_CREATED` is virtual and not stored. Cancellation is only allowed before pickup. `FAILED` may transition back to `IN_TRANSIT` when carrier reports resumed delivery, so it is not terminal in the state machine; `DELIVERED` and `CANCELLED` are terminal. Carrier webhook is the only source allowed to mark delivered. Old timestamps and invalid transitions are ignored without regression.

## Create flow

The integration flow is Order obtains a quote by internal REST, owns checkout pricing and confirmation, then calls Shipment REST create with its snapshots and confirmed fee. Shipment validates and reserves the request idempotently, obtains a fresh carrier quote, calls the selected adapter, and transactionally persists the shipment state and Outbox event after confirmed creation. The event is delivered asynchronously and at least once; Order must deduplicate by `event_id` and retains ownership of its own order state. Full request/response mapping and unresolved items are in `docs/decisions/order-commerce-contract-patch.md`.

### Separate opt-in Kafka create proposal

This path is implemented in source but is not the selected Order-Commerce integration contract. The listener validates the envelope and payload, then stores command ID, canonical payload hash, idempotency key, correlation metadata, source Kafka coordinates and raw envelope in MySQL before returning (record ack follows successful listener return). A duplicate with identical content is acknowledged; command ID with different content is dead-lettered. A leased scheduled worker claims the inbox row in a short transaction, closes the transaction, then delegates to the existing `ShipmentCommandService`; carrier network I/O is not performed while holding the inbox row lock. Work interrupted by process death is reclaimable after lease expiry. See `docs/kafka/shipment-events.md`; keep this disabled unless separately approved.

Fee ownership, legacy field meaning, exact persistence mapping, and a source discrepancy in the positive reconciliation event payload are documented in `docs/decisions/order-commerce-contract-patch.md`. No actual carrier fee is currently supplied. The full REST create transaction/outbox behavior and Kafka create proposal must not be conflated.

Transient command infrastructure errors use bounded exponential retry with jitter; after exhaustion the command is retained in a durable `DEAD` state for privileged replay. Malformed/unsupported records and durable-inbox failures are retried by the Kafka listener error handler and then forwarded to the configured Kafka DLT. This is separate from Outbox retries and carrier reconciliation.

`order.cancelled` is durably recorded before the listener acknowledges it, including optional correlation metadata. A cancellation tombstone prevents a later create for the same Shop Order; cancellations racing an in-flight create are picked up by the cancellation worker after reservation/reconciliation. Cancellation only invokes the existing domain cancel flow if the resulting shipment remains cancellable. Resulting events use the source event ID as causation. The source has no transaction spanning Kafka or carrier I/O.

The existing Outbox keeps event identity inside its stored envelope. Correlation and command causation are added as optional envelope fields while retaining `event_id`, `event_type`, topic and key conventions. Unpublished events are claimed by per-aggregate `created_at`/ID order, preventing a later row by that ordering from being sent while an earlier row remains pending or is being dead-lettered. This is not a strict causal sequence because timestamps come from application clocks and there is no aggregate sequence number. Publisher acceptance followed by a DB update failure can still duplicate delivery; consumers deduplicate on `event_id`.

## Outbox

Envelope fields: `event_id`, `schema_version`, `event_type`, `occurred_at`, `aggregate_type`, `aggregate_id`, `payload`. Outbox poller publishes at-least-once; consumers dedupe on `event_id`. Publishers claim one event with `SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction and publish after releasing the DB lock. A lease token fences state updates; expired claims can be reclaimed after a worker stops. The lease defaults to 60 seconds and is configurable with `SHIPMENT_OUTBOX_LEASE_SECONDS`. After three main-topic failures it retries delivery to the configured DLQ topic up to three times. If DLQ delivery also fails three times, the record remains `DEAD` in the database for operator recovery. Kafka acceptance followed by a lost lease or database failure can still lead to duplicate delivery; consumers still need dedupe and operational retry/replay policy.

## Security / privacy

- Internal REST token header in this candidate is an implementation assumption and must be aligned with shared service-to-service auth/mTLS.
- Gateway actor headers are trusted only on private Gateway-to-service network paths.
- Generic HMAC webhook implementation is a test contract only; carrier-specific signature/IP requirements remain open.
- No raw webhook body, signature, address or phone is logged.
