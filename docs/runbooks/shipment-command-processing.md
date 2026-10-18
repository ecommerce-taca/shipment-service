# Runbook — Shipment Kafka commands

## Contract and production gate

The `shipment.commands.v1` command, `shop_order_id` Kafka key, create-result events and retry/DLT defaults are implemented as a Shipment-owned proposal. They are not an Order-Commerce/platform-approved contract and are not the selected Order-Commerce create path. REST `POST /api/v1/internal/shipments` plus Shipment lifecycle events is the documented integration direction. Do not enable cross-service production traffic on this optional Kafka proposal until owners separately approve its schema, topic ACLs/retention and result projection.

Do not submit the same business shipment through both REST and Kafka create paths.

## Local configuration

1. Start local MySQL and Kafka using the workspace's existing local environment; do not reset shared volumes.
2. Provision `shipment.commands.v1`, `shipment.events.v1`, `shipment.events.dlq.v1`, `shipment.consumer.dlq.v1`, and `order.events.v1` using the local/platform topic provisioning mechanism. Auto-topic creation is not used.
3. Set `KAFKA_BOOTSTRAP_SERVERS`, `SHIPMENT_CREATE_CONSUMER_ENABLED=true`, `SHIPMENT_KAFKA_CONSUMER_ENABLED=true`, and `SHIPMENT_KAFKA_PUBLISHER_ENABLED=true`. Keep `SHIPMENT_MOCK_ENABLED=true` and `APP_ENV=local` to use MOCK; GHN remains fail-closed unless its verified integration is implemented.
4. Start Shipment. Flyway applies `V003__add_shipment_command_inbox.sql`.
5. Produce the proposed JSON command from `docs/kafka/shipment-events.md` to `shipment.commands.v1` with Kafka key equal to `payload.shop_order_id`.

For a locally installed Kafka console producer, start:

```text
kafka-console-producer --bootstrap-server localhost:19092 --topic shipment.commands.v1 --property parse.key=true --property key.separator=|
```

Then enter one line as `<shop_order_id>|<single-line JSON envelope>` using the documented command example. The proposal uses the shop order as the partition key.

The create listener validates and durably stores a command before returning. A database-backed worker processes it asynchronously; observe `shipment_create_commands.status` through the approved operational database/monitoring access. Never log or paste raw payloads because they include buyer/address snapshots.

## Lifecycle and recovery

- `ACCEPTED` → `PROCESSING` → `COMPLETED` after create success.
- Ambiguous carrier result → `PENDING_RECONCILIATION`; reconciliation does not blindly repeat create. A positive carrier lookup completes all pending command rows linked to that shipment.
- Permanent business validation/rejection → `REJECTED` and proposed `shipment.creation_failed` Outbox event.
- Transient worker failures use bounded exponential retry with jitter. Exhaustion → `DEAD`, with original envelope, Kafka source topic/partition/offset, attempts and safe error code in `shipment_command_dlq`.
- Schema-invalid or unsupported messages are sent unchanged to configured `shipment.consumer.dlq.v1` after listener retries; standard Spring Kafka DLT headers preserve source topic/partition/offset, plus `shipment_dlq_attempts` and a safe exception-class `shipment_dlq_reason_code`.
- Outbox publishing has a separate claim/lease and retry/DLQ lifecycle: at most three main-topic send attempts total (2s/4s delays between attempts), then at most three DLQ attempts total (also 2s/4s). Kafka may receive an event before a database update failure, so receiving consumers must deduplicate by stable `event_id`.

## Operator replay

Only replay a DB-backed command in `DEAD` after checking the safe error code and external carrier outcome. Replay reuses the stored command and original idempotency key:

```http
POST /api/v1/internal/ops/shipment-create-commands/{commandId}/replay
X-Internal-Service-Token: <service-token>
X-Operator-Token: <separate-ops-token>
X-Operator-ID: <audited-operator-id>
X-Operator-Roles: SHIPMENT_COMMAND_REPLAY
```

The endpoint updates replay count/operator audit fields. It does not replay rejected business commands or commands still awaiting reconciliation. For malformed-message Kafka DLT records, use the platform-owned DLT replay procedure; Shipment has no broker DLT replay endpoint.

Cancellation work parked in `NEEDS_OPERATOR` can be requeued via `POST /api/v1/internal/ops/order-cancellations/{orderId}/replay` using the same authorization headers. A cancellation intent remains durable even when the shipment has not yet been created, preventing a later create command from silently bypassing it.

## Operations ownership and limitations

- Shipment owns validation, durable command state, domain processing, carrier reconciliation and the DB-backed replay endpoint.
- Platform owns broker availability, topic creation, ACLs, retention, consumer DLT monitoring and Kafka DLT replay.
- Security/Ops must provision and rotate the separate operator token and ensure operator identity/role headers come from a trusted ingress.
- Order-Commerce/platform must approve the proposal and define order projection behavior before enabling integrated traffic.
- GHN API create, quote, cancellation and authoritative lookup are not implemented in this source. Do not treat MOCK behavior as evidence of GHN behavior.
- Create commands and `order.cancelled` records are capped at 256 KiB UTF-8 by the Shipment consumers; align Kafka broker/topic limits with this provisional contract value.
