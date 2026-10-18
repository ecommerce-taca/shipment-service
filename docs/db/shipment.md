# Database — Shipment Service v0.2

MySQL 8.4/InnoDB, database `shipmentdb`; UTC `DATETIME(6)`. Shipment's own ID is UUIDv7 stored as `BINARY(16)`. IDs from Order/Shop/User are opaque strings with no cross-service foreign keys.

## Tables

- `shipments`: one record per `(order_id, shop_id)`; carrier, tracking, state, address snapshots, legacy non-null `shipping_fee`, nullable explicit `checkout_shipping_fee`, nullable `carrier_quoted_fee` and nullable `carrier_actual_fee`, currency, COD, estimate dates, idempotency key and optimistic version. For new rows, `checkout_shipping_fee` is the request's commercial amount. `shipping_fee` retains its previous effective-fee semantics (carrier quote when present, request amount otherwise) for compatibility and is deprecated for checkout accounting. `carrier_quoted_fee` records the create-time quote only. `carrier_actual_fee` can only hold authoritative verified actual-charge data; no current adapter supplies it. Existing rows are not backfilled because their explicit checkout, quote and actual fee facts may not be recoverable.
- `shipment_items`: immutable product/SKU/title/quantity/weight snapshots; FK to shipment.
- `shipment_events`: canonical timeline and carrier event reference; unique `(carrier, external_event_id)`.
- `carrier_webhook_logs`: payload hash and authentication/processing result only; raw body is never stored.
- `idempotency_keys`: request hash/state/response snapshot, 24h expiry.
- `shipment_create_commands`: durable command inbox keyed by `command_id`, payload hash for conflict detection, Shop Order ID, stable idempotency key, correlation/causation, raw message and Kafka source coordinates, lifecycle state (`ACCEPTED`, `PROCESSING`, `RETRY_WAIT`, `PENDING_RECONCILIATION`, `COMPLETED`, `REJECTED`, `DEAD`), optional linked shipment ID, retry schedule and reclaimable lease. A unique command ID deduplicates redelivery; `(order_id, shop_id)` on `shipments` remains the business uniqueness constraint. Positive carrier reconciliation completes matching `PROCESSING` or `PENDING_RECONCILIATION` command rows for that Shop Order/shop and clears their leases, so a late worker cannot overwrite a confirmed outcome.
- `shipment_command_dlq`: original command, source topic/partition/offset, retry count, safe error code and replay audit fields for worker exhaustion. Replayed commands return to the inbox and retain the original idempotency key.
- `order_cancellation_intents`: durable `order.cancelled` tombstone keyed by Shop Order ID and unique source event ID, with source correlation metadata and lifecycle (`RECEIVED`, `PROCESSING`, `WAITING_RECONCILIATION`, `RETRY_WAIT`, `APPLIED`, `NEEDS_OPERATOR`). It prevents a cancellation received before create from being lost and is leased/retried independently.
- `outbox_events`: event envelope stored in the same transaction as domain changes, then published asynchronously.

## Migration

`V001__create_shipment_tables.sql` creates the baseline schema; `V002__add_outbox_claim_lease.sql` adds publisher leases; `V003__add_shipment_command_inbox.sql` adds command, cancellation and durable dead-letter state; `V004__require_explicit_shipment_currency.sql` removes the legacy VND default without changing `CHAR(3)`; `V005__separate_checkout_and_carrier_quote_fees.sql` adds nullable `checkout_shipping_fee` and `carrier_quoted_fee`; `V006__add_carrier_actual_fee.sql` adds nullable `carrier_actual_fee`. Existing `shipping_fee` values are retained unchanged; no speculative backfill is performed. New creates store the commercial amount in `checkout_shipping_fee`, keep the existing `shipping_fee` field's legacy effective-fee behavior, and store any available quote in `carrier_quoted_fee`; actual fee remains null until verified source data exists. Currency is supplied by create requests and persisted explicitly. Hibernate uses `ddl-auto=validate`; schema changes must be new Flyway migrations. Do not edit applied migrations. No destructive down migration is included.

## Production preflight

1. Check existing data for duplicate `(order_id, shop_id)` and duplicate `(carrier, tracking_code)` before enabling unique constraints on a pre-existing database.
2. Confirm Hibernate's UUID-to-`BINARY(16)` mapping against the selected Hibernate version and verify round-trip tests on MySQL 8.4.
3. Define retention/cleanup for expired idempotency keys and outbox DEAD records. Shipment timeline and webhook audit logs must not be hard-deleted without retention policy approval.
4. Keep address snapshots out of logs and API read responses.
5. Command and cancellation inboxes retain original Kafka messages, which may contain address and buyer PII. Restrict table access, enable approved database-at-rest protection and define retention/deletion policy with security/data owners.
6. The command/event contract remains proposal-only until Order-Commerce and platform owners approve it. Provision topics and ACLs centrally; the service does not create topics.
