ALTER TABLE outbox_events
  ADD COLUMN lease_token CHAR(36) NULL,
  ADD COLUMN lease_until DATETIME(6) NULL,
  ADD INDEX idx_outbox_lease_dispatch (status, next_attempt_at, lease_until, created_at);
