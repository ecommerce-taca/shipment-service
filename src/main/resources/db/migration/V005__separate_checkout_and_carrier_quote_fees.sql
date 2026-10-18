ALTER TABLE shipments
  ADD COLUMN checkout_shipping_fee BIGINT NULL AFTER shipping_fee,
  ADD COLUMN carrier_quoted_fee BIGINT NULL AFTER checkout_shipping_fee;
