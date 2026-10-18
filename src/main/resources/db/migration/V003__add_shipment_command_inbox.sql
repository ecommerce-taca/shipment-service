CREATE TABLE shipment_create_commands (
  command_id VARCHAR(128) NOT NULL,
  request_hash CHAR(64) NOT NULL,
  shop_order_id VARCHAR(80) NOT NULL,
  idempotency_key VARCHAR(128) NOT NULL,
  correlation_id VARCHAR(128) NOT NULL,
  causation_id VARCHAR(128) NULL,
  payload JSON NOT NULL,
  original_message MEDIUMTEXT NOT NULL,
  source_topic VARCHAR(249) NOT NULL,
  source_partition INT NOT NULL,
  source_offset BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(6) NOT NULL,
  lease_token CHAR(36) NULL,
  lease_until DATETIME(6) NULL,
  shipment_id VARCHAR(80) NULL,
  error_code VARCHAR(80) NULL,
  error_message_safe VARCHAR(500) NULL,
  replay_count INT NOT NULL DEFAULT 0,
  replayed_by VARCHAR(128) NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  completed_at DATETIME(6) NULL,
  CONSTRAINT ck_shipment_create_command_status CHECK
    (status IN ('ACCEPTED', 'PROCESSING', 'RETRY_WAIT', 'PENDING_RECONCILIATION', 'COMPLETED', 'REJECTED', 'DEAD')),
  CONSTRAINT ck_shipment_create_command_lease CHECK
    ((lease_token IS NULL AND lease_until IS NULL) OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
  PRIMARY KEY (command_id),
  INDEX idx_shipment_create_command_dispatch (status, next_attempt_at, lease_until, created_at),
  INDEX idx_shipment_create_command_order (shop_order_id, status),
  INDEX idx_shipment_create_command_idempotency (idempotency_key),
  INDEX idx_shipment_create_command_shipment (shipment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE order_cancellation_intents (
  order_id VARCHAR(80) NOT NULL,
  event_id VARCHAR(128) NOT NULL,
  correlation_id VARCHAR(128) NULL,
  reason VARCHAR(255) NOT NULL,
  source_topic VARCHAR(249) NOT NULL,
  source_partition INT NOT NULL,
  source_offset BIGINT NOT NULL,
  original_message MEDIUMTEXT NOT NULL,
  status VARCHAR(24) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(6) NOT NULL,
  lease_token CHAR(36) NULL,
  lease_until DATETIME(6) NULL,
  error_code VARCHAR(80) NULL,
  error_message_safe VARCHAR(500) NULL,
  replay_count INT NOT NULL DEFAULT 0,
  replayed_by VARCHAR(128) NULL,
  created_at DATETIME(6) NOT NULL,
  updated_at DATETIME(6) NOT NULL,
  completed_at DATETIME(6) NULL,
  CONSTRAINT ck_order_cancellation_status CHECK
    (status IN ('RECEIVED', 'PROCESSING', 'WAITING_RECONCILIATION', 'RETRY_WAIT', 'APPLIED', 'NEEDS_OPERATOR')),
  CONSTRAINT ck_order_cancellation_lease CHECK
    ((lease_token IS NULL AND lease_until IS NULL) OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
  PRIMARY KEY (order_id),
  UNIQUE KEY uk_order_cancellation_event (event_id),
  INDEX idx_order_cancellation_dispatch (status, next_attempt_at, lease_until, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE shipment_command_dlq (
  id BIGINT NOT NULL AUTO_INCREMENT,
  command_id VARCHAR(128) NOT NULL,
  original_message MEDIUMTEXT NOT NULL,
  source_topic VARCHAR(249) NOT NULL,
  source_partition INT NOT NULL,
  source_offset BIGINT NOT NULL,
  attempts INT NOT NULL,
  error_code VARCHAR(80) NOT NULL,
  error_message_safe VARCHAR(500) NULL,
  created_at DATETIME(6) NOT NULL,
  replayed_at DATETIME(6) NULL,
  replayed_by VARCHAR(128) NULL,
  PRIMARY KEY (id),
  INDEX idx_shipment_command_dlq_command (command_id, created_at),
  INDEX idx_shipment_command_dlq_created (created_at, replayed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE shipment_command_replay_audit (
  id BIGINT NOT NULL AUTO_INCREMENT,
  target_type VARCHAR(32) NOT NULL,
  target_id VARCHAR(128) NOT NULL,
  operator_id VARCHAR(128) NOT NULL,
  request_id VARCHAR(128) NULL,
  replayed_at DATETIME(6) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_shipment_replay_audit_target (target_type, target_id, replayed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
