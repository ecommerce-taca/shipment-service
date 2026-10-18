# Taca Marketplace — Shipment Service (implementation candidate v0.2)

Shipment Service owns carrier integration, shipment/tracking state, tracking timeline and shipment events. It does not own order lifecycle, payment/refund or user/address source data.

## 1. Implemented in this source tree

- Spring Boot 3.5.x project targeting Java 25.
- MySQL 8.4 schema with UUIDv7 IDs stored as `BINARY(16)`, shipment items/events, webhook log, idempotency and transactional outbox.
- Internal REST create/cancel and quote endpoints. Order-Commerce supplies address/item snapshots; Shipment never queries another service database. REST quote/create plus Shipment lifecycle events is the selected integration direction documented for Order-Commerce preparation.
- Buyer/seller tracking reads with actor headers and scope checks.
- MOCK carrier for local end-to-end flows.
- GHN adapter is intentionally fail-closed and **not yet connected to the real GHN API**. No fake tracking code is returned for GHN.
- Webhook HMAC-SHA256 adapter boundary, IP allowlist, deduplication and status transition guard. This generic HMAC policy is for local/test only until it is replaced with the carrier's documented signature scheme.
- Transactional outbox publisher plus an opt-in durable Kafka create-command inbox/worker and `order.cancelled` cancellation-intent consumer. Kafka create-command processing is a separate Shipment-owned proposal, disabled by default and not the selected Order-Commerce create contract.
- Reconciliation job only recovers when the carrier adapter can positively find a created shipment by idempotency key. It never blindly creates a second shipment.
- Create-command and cancellation recovery state is persisted in MySQL; command dead-letter replay requires privileged operator credentials.

## 2. Prerequisites

- JDK 25.
- Maven 3.9+ (or use the project's Maven wrapper if added by the team).
- Docker Desktop with Docker Compose.

## 3. Start the local Shipment stack (MySQL + Kafka + Shipment)

For a machine that already has its local `.env`:

```powershell
docker compose config
docker compose up -d --build
docker compose ps
```

For a new clone, create the ignored local file first:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up -d --build
docker compose ps
```

Change local credentials/tokens and host ports in `.env` if needed; keep `DB_URL_DOCKER` on hostname `shipment-mysql` and `KAFKA_BOOTSTRAP_SERVERS_DOCKER` on `kafka:9092`. Keep MySQL's `MYSQL_USER`/`MYSQL_PASSWORD` aligned with `DB_USERNAME`/`DB_PASSWORD`. If a host port is occupied, choose another value for `SHIPMENT_MYSQL_PORT`, `SHIPMENT_KAFKA_PORT`, or `SHIPMENT_HTTP_PORT`; also update the host-side `DB_URL` / `KAFKA_BOOTSTRAP_SERVERS` when using those addresses outside Compose. If changing the MySQL database name, keep it aligned in `DB_URL` and `DB_URL_DOCKER`. On an already initialized MySQL volume, changing `MYSQL_*` in `.env` does not rotate existing database credentials; preserve the volume and configure credentials deliberately rather than deleting data.

Compose runs project-local MySQL 8.4 and Kafka 3.9.1 in KRaft mode, builds Shipment, and waits for MySQL/Kafka healthchecks and successful topic initialization. Host ports are bound to `127.0.0.1`: MySQL defaults to `15406`, Kafka to `19092`, and Shipment to `18086` (container port `8086`).

Host-side tools use `localhost:${SHIPMENT_KAFKA_PORT}`. A host-run application should use `DB_URL` and `KAFKA_BOOTSTRAP_SERVERS` from `.env` after loading those variables into its process environment; Spring does not automatically load Compose's `.env` file. The Shipment container receives `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` and `DB_URL=jdbc:mysql://shipment-mysql:3306/...`; do not use `localhost` for either dependency from inside the container.

The initializer creates the configured lifecycle and local proposal topics; defaults are `shipment.events.v1`, `shipment.events.dlq.v1`, `shipment.commands.v1`, `shipment.consumer.dlq.v1`, and `order.events.v1`. Topics use one partition and replication factor one for local development only. The Outbox publisher is enabled by default. The current machine `.env` enables the create-command consumer for explicit local exercises; the `.env.example` keeps it disabled. `shipment.commands.v1` creation and `order.events.v1` consumption are opt-in proposals, not the selected REST shipment-create contract. See the [Shipment ↔ Order-Commerce integration contract](docs/decisions/order-commerce-contract-patch.md).

## 4. Verify local services

```powershell
docker compose ps
docker compose logs --tail=100 kafka shipment-topic-init shipment-service
curl.exe --fail http://localhost:18086/health/ready
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

From the host, use Kafka's external listener:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092
Test-NetConnection -ComputerName localhost -Port 19092
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic shipment.events.v1 --from-beginning
```

Flyway creates/updates the schema on startup. JPA is set to `ddl-auto=validate`; do not use Hibernate schema generation to replace migrations. To build/run only MySQL and Shipment without Kafka is not supported by this Compose file because the default local stack intentionally includes Kafka.

## 5. Enable local Kafka

```powershell
docker compose up -d
$env:SHIPMENT_KAFKA_CONSUMER_ENABLED='true'
$env:SHIPMENT_CREATE_CONSUMER_ENABLED='true'
docker compose up -d --force-recreate shipment-service
```

The `order.events.v1` consumer is off by default. To enable it for a specific local integration exercise, set `SHIPMENT_KAFKA_CONSUMER_ENABLED=true` in `.env` and recreate the service. The create-command consumer is separately controlled by `SHIPMENT_CREATE_CONSUMER_ENABLED`; enabling it here is only for local exercises of the proposal and does not change the selected REST create direction. Do not submit the same business shipment through both create paths.

To replay a DB-backed command parked as `DEAD`, configure a separate `SHIPMENT_OPS_TOKEN` and send the internal token, operator token, operator identity, and trusted `ADMIN` or `SHIPMENT_COMMAND_REPLAY` role to:

```http
POST /api/v1/internal/ops/shipment-create-commands/{commandId}/replay
X-Internal-Service-Token: <service-token>
X-Operator-Token: <ops-token>
X-Operator-ID: <audited-operator-id>
X-Operator-Roles: SHIPMENT_COMMAND_REPLAY
```

Cancellation intents parked as `NEEDS_OPERATOR` have a corresponding replay route documented in `docs/kafka/shipment-events.md`. Kafka consumer DLT replay remains a platform operation; the service does not provide a broker DLT replay tool.

## 6. Internal REST authentication (local contract)

All `/api/v1/internal/**` requests require:

```http
X-Internal-Service-Token: local-dev-token-change-me
X-Request-ID: <request-id>
```

This header is a local implementation of the existing “internal service token/mTLS” requirement. Before integration, align the header/token validation with the shared Taca service-to-service authentication library or mTLS policy. Do not expose the service directly to the public internet.

Buyer/seller tracking requires trusted Gateway actor headers. `X-User-*` headers must only be accepted from the trusted Gateway network; the service is not a replacement for network-level enforcement.

## 7. Quote flow

See the [Shipment ↔ Order-Commerce integration contract](docs/decisions/order-commerce-contract-patch.md) for the source-checked request/response/event shapes, fee mapping, retry behavior, sequence and open decisions.

Order-Commerce sends the complete snapshots and items over REST:

- `POST /api/v1/internal/shipping/quote` for checkout's default carrier estimate.
- `POST /api/v1/internal/shipping/carrier-quotes` for seller carrier comparison.

The seller-facing `GET /seller/orders/{orderId}/shipment/carriers` should be served by Order-Commerce (or its BFF), which checks order state/seller scope and calls the internal carrier-quotes endpoint. Shipment must not infer order data from an `orderId` or read `orderdb`.

## 8. Legacy REST create

```http
POST /api/v1/internal/shipments
Idempotency-Key: <unique-key>
X-Internal-Service-Token: <service-token>
Content-Type: application/json
```

This is the current Shipment-side REST create contract for Order-Commerce preparation. It is not removed or superseded by the opt-in Kafka command proposal. See the [integration contract](docs/decisions/order-commerce-contract-patch.md) before implementing a client. The request's `shipping_fee` is the checkout-confirmed amount; response/event consumers must use `checkout_shipping_fee`.

## 9. Validation

```powershell
mvn test
```

The current contract implementation change has not been built or tested as part of this task. Do not infer runtime readiness from source review. Run the project validation separately before integration.

## 10. Important limitations

- GHN's real request/response contract, authentication, service codes, location mapping and webhook signing have not been wired in this candidate; the adapter fails closed.
- SPX/J&T adapters are not implemented. They appear unavailable in seller quote comparison until adapters are added.
- The HMAC webhook verifier is a generic contract test implementation, not a claim about GHN's actual signature mechanism.
- Reconciliation can only recover a provider result when an adapter implements `findByIdempotencyKey`; MOCK keeps its map in memory, GHN lookup is not implemented.
- Outbox delivery is at-least-once. Consumers must deduplicate using envelope `event_id`.
- Command create topic/key/result events and consumer DLT defaults are proposal-only until Order-Commerce and platform Kafka owners confirm them.
- Durable inbox/dead-letter tables contain original messages with address/buyer data; database access and retention must follow approved PII controls.
- Email delivery for `shipment.delivered` and `shipment.failed` depends on resolving buyer email ownership/input as documented.
