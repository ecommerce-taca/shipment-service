ALTER TABLE shipments
  ADD COLUMN order_code VARCHAR(32) NULL AFTER order_id;
