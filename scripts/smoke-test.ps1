$ErrorActionPreference = 'Stop'
$BaseUrl = 'http://localhost:8086/api/v1'
$InternalToken = if ($env:SHIPMENT_INTERNAL_TOKEN) { $env:SHIPMENT_INTERNAL_TOKEN } else { 'local-dev-token-change-me' }
$RequestId = [guid]::NewGuid().ToString()
$IdempotencyKey = 'smoke-' + [guid]::NewGuid().ToString()
$Headers = @{
  'X-Internal-Service-Token' = $InternalToken
  'X-Request-ID' = $RequestId
  'Idempotency-Key' = $IdempotencyKey
}
$Body = @{
  order_id = 'order-smoke-001'
  checkout_group_id = 'checkout-smoke-001'
  shop_id = 'shop-smoke-001'
  buyer_user_id = 'buyer-smoke-001'
  buyer_email = 'buyer@example.com'
  carrier = 'MOCK'
  from_address_snapshot = @{ contact_name='Demo Shop'; phone='0900000000'; line1='Warehouse A'; ward='Ben Thanh'; district='District 1'; province='Ho Chi Minh City' }
  to_address_snapshot = @{ contact_name='Demo Buyer'; phone='0912345678'; line1='12 Nguyen Hue'; ward='Ben Nghe'; district='District 1'; province='Ho Chi Minh City' }
  items = @(@{ sku_id='sku-smoke-001'; product_id='product-smoke-001'; title='Sample item'; quantity=1; weight_gram=300 })
  cod_amount = 0
  shipping_fee = 32000
  payment_method = 'VNPAY'
} | ConvertTo-Json -Depth 10

Write-Host '1) Checkout quote via MOCK'
$QuoteBody = @{
  from_shop_id = 'shop-smoke-001'
  from_address_snapshot = @{ line1='Warehouse A'; ward='Ben Thanh'; district='District 1'; province='Ho Chi Minh City' }
  to_address_snapshot = @{ line1='12 Nguyen Hue'; ward='Ben Nghe'; district='District 1'; province='Ho Chi Minh City' }
  items = @(@{ sku_id='sku-smoke-001'; product_id='product-smoke-001'; title='Sample item'; quantity=1; weight_gram=300 })
  carrier = 'MOCK'
} | ConvertTo-Json -Depth 10
Invoke-RestMethod -Method Post -Uri "$BaseUrl/internal/shipping/quote" -Headers @{ 'X-Internal-Service-Token'=$InternalToken; 'X-Request-ID'=$RequestId } -ContentType 'application/json' -Body $QuoteBody | ConvertTo-Json -Depth 10

Write-Host '2) Create shipment via MOCK'
$Created = Invoke-RestMethod -Method Post -Uri "$BaseUrl/internal/shipments" -Headers $Headers -ContentType 'application/json' -Body $Body
$Created | ConvertTo-Json -Depth 10
$OrderId = $Created.data.order_id

Write-Host '3) Buyer tracking read (trusted Gateway headers simulated locally)'
$TrackingHeaders = @{ 'X-User-ID'='buyer-smoke-001'; 'X-User-Roles'='BUYER'; 'X-Request-ID'=[guid]::NewGuid().ToString() }
Invoke-RestMethod -Method Get -Uri "$BaseUrl/orders/$OrderId/shipment" -Headers $TrackingHeaders | ConvertTo-Json -Depth 10

Write-Host 'Smoke test complete. Use a fresh order_id/shop_id and Idempotency-Key for another create test.'
