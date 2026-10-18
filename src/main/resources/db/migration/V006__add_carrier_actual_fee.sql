ALTER TABLE shipments
  ADD COLUMN carrier_actual_fee BIGINT NULL AFTER carrier_quoted_fee;
