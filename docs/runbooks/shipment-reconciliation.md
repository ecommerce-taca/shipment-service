# Runbook — `PENDING_RECONCILIATION`

## Meaning

The carrier create call may have succeeded but Shipment did not receive a definitive response. This is not a terminal status and must not be blindly retried.

## Automated behavior

1. Shipment reserves `(order_id, shop_id)` and the idempotency key before the external create call.
2. If the provider outcome is ambiguous, the API returns `202` with `PENDING_RECONCILIATION`.
3. The scheduled reconciliation job asks the carrier adapter to look up the result by the stored idempotency key.
4. Only a positive provider result automatically changes the shipment to `CREATED` and emits `shipment.created`.
5. An empty lookup result is **not** treated as proof that the provider did not create the shipment. The job leaves it pending until the adapter can return authoritative evidence or an operator resolves it.

## Operator steps (manual until GHN lookup is implemented)

- Find the shipment by `order_id`, `shop_id`, and request ID/idempotency key using restricted database access.
- Search the carrier sandbox/merchant portal for the order reference or idempotency reference.
- If the carrier confirms the waybill exists, record the carrier's external ID/tracking code through an approved reconciliation operation (not direct ad-hoc SQL in production).
- If the carrier confirms no waybill exists, close the pending attempt as cancelled/failed according to the agreed policy before allowing a new create attempt.
- If the provider cannot establish the outcome, keep the shipment pending and escalate; do not retry create with a new key.

## Missing automation

The real GHN lookup contract, authoritative negative lookup semantics and operator repair endpoint are not implemented yet. This runbook is guidance, not a claim that all steps are automated.
