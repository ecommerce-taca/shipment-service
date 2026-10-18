CREATE TABLE shipments (
  id BINARY(16) NOT NULL,
  order_id VARCHAR(80) NOT NULL,
  checkout_group_id VARCHAR(80) NULL,
  shop_id VARCHAR(80) NOT NULL,
  buyer_user_id VARCHAR(80) NOT NULL,
  buyer_email VARCHAR(254) NULL,
  carrier VARCHAR(16) NOT NULL,
  tracking_code VARCHAR(64) NULL,
  status VARCHAR(32) NOT NULL,
  from_address_snapshot JSON NOT NULL,
  to_address_snapshot JSON NOT NULL,
  shipping_fee BIGINT NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'VND',
  cod_amount BIGINT NOT NULL DEFAULT 0,
  estimated_delivery_min_date DATE NULL,
  estimated_delivery_max_date DATE NULL,
  external_shipment_id VARCHAR(128) NULL,
  create_idempotency_key VARCHAR(128) NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT uk_shipments_order_shop UNIQUE (order_id, shop_id),
  CONSTRAINT uk_shipments_carrier_tracking UNIQUE (carrier, tracking_code),
  INDEX idx_shipments_buyer_created (buyer_user_id, created_at),
  INDEX idx_shipments_shop_status_created (shop_id, status, created_at),
  INDEX idx_shipments_status_updated (status, updated_at),
  INDEX idx_shipments_create_key (create_idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE shipment_items (
  id BINARY(16) NOT NULL,
  shipment_id BINARY(16) NOT NULL,
  sku_id VARCHAR(80) NOT NULL,
  product_id VARCHAR(80) NULL,
  title_snapshot VARCHAR(255) NOT NULL,
  quantity INT NOT NULL,
  weight_grams INT NULL,
  created_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_shipment_items_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(id),
  CONSTRAINT chk_shipment_items_quantity CHECK (quantity > 0),
  INDEX idx_shipment_items_shipment (shipment_id),
  INDEX idx_shipment_items_sku_created (sku_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE shipment_events (
  id BINARY(16) NOT NULL,
  shipment_id BINARY(16) NOT NULL,
  carrier VARCHAR(16) NOT NULL,
  canonical_status VARCHAR(32) NOT NULL,
  carrier_status VARCHAR(80) NULL,
  description_safe VARCHAR(500) NULL,
  source VARCHAR(24) NOT NULL,
  external_event_id VARCHAR(128) NULL,
  occurred_at DATETIME(6) NOT NULL,
  received_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_shipment_events_shipment FOREIGN KEY (shipment_id) REFERENCES shipments(id),
  CONSTRAINT uk_shipment_events_carrier_external UNIQUE (carrier, external_event_id),
  INDEX idx_shipment_events_shipment_occurred (shipment_id, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE carrier_webhook_logs (
  id BINARY(16) NOT NULL,
  carrier VARCHAR(16) NOT NULL,
  external_event_id VARCHAR(128) NOT NULL,
  payload_hash CHAR(64) NOT NULL,
  signature_valid BOOLEAN NOT NULL,
  status VARCHAR(24) NOT NULL,
  safe_error_code VARCHAR(80) NULL,
  received_at DATETIME(6) NOT NULL,
  processed_at DATETIME(6) NULL,
  PRIMARY KEY (id),
  CONSTRAINT uk_carrier_webhook_event UNIQUE (carrier, external_event_id),
  INDEX idx_carrier_webhook_status_received (status, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE idempotency_keys (
  idempotency_key VARCHAR(128) NOT NULL,
  operation VARCHAR(32) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  state VARCHAR(24) NOT NULL,
  shipment_id BINARY(16) NULL,
  response_snapshot JSON NULL,
  created_at DATETIME(6) NOT NULL,
  expires_at DATETIME(6) NOT NULL,
  PRIMARY KEY (idempotency_key, operation),
  INDEX idx_idempotency_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE outbox_events (
  id BINARY(16) NOT NULL,
  aggregate_type VARCHAR(40) NOT NULL,
  aggregate_id VARCHAR(80) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  topic VARCHAR(160) NOT NULL,
  event_key VARCHAR(160) NOT NULL,
  payload JSON NOT NULL,
  headers JSON NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(6) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  published_at DATETIME(6) NULL,
  last_error_code VARCHAR(80) NULL,
  PRIMARY KEY (id),
  INDEX idx_outbox_dispatch (status, next_attempt_at, created_at),
  INDEX idx_outbox_aggregate (aggregate_type, aggregate_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
