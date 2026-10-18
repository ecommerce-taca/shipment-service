package com.taca.shipment.shipment.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.taca.shipment.common.id.UuidV7;
import com.taca.shipment.shipment.domain.Carrier;
import com.taca.shipment.shipment.domain.ShipmentStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "shipments", uniqueConstraints = {
        @UniqueConstraint(name = "uk_shipments_order_shop", columnNames = { "order_id", "shop_id" }),
        @UniqueConstraint(name = "uk_shipments_carrier_tracking", columnNames = { "carrier", "tracking_code" }) })
public class ShipmentEntity {
    @Id
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
    @Column(name = "order_id", nullable = false, length = 80)
    private String orderId;
    @Column(name = "checkout_group_id", length = 80)
    private String checkoutGroupId;
    @Column(name = "shop_id", nullable = false, length = 80)
    private String shopId;
    @Column(name = "buyer_user_id", nullable = false, length = 80)
    private String buyerUserId;
    @Column(name = "buyer_email", length = 254)
    private String buyerEmail;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Carrier carrier;
    @Column(name = "tracking_code", length = 64)
    private String trackingCode;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private ShipmentStatus status;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "from_address_snapshot", nullable = false, columnDefinition = "json")
    private JsonNode fromAddressSnapshot;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "to_address_snapshot", nullable = false, columnDefinition = "json")
    private JsonNode toAddressSnapshot;
    @Column(name = "shipping_fee", nullable = false)
    private long shippingFee;
    @Column(name = "checkout_shipping_fee")
    private Long checkoutShippingFee;
    @Column(name = "carrier_quoted_fee")
    private Long carrierQuotedFee;
    @Column(name = "carrier_actual_fee")
    private Long carrierActualFee;
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "currency", nullable = false, length = 3, columnDefinition = "CHAR(3)")
    private String currency;
    @Column(name = "cod_amount", nullable = false)
    private long codAmount;
    @Column(name = "estimated_delivery_min_date")
    private LocalDate estimatedDeliveryMinDate;
    @Column(name = "estimated_delivery_max_date")
    private LocalDate estimatedDeliveryMaxDate;
    @Column(name = "external_shipment_id", length = 128)
    private String externalShipmentId;
    @Column(name = "create_idempotency_key", nullable = false, length = 128)
    private String createIdempotencyKey;
    @Version
    @Column(nullable = false)
    private long version = 1;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    protected ShipmentEntity() {
    }

    public static ShipmentEntity pending(String orderId, String checkoutGroupId, String shopId, String buyerUserId,
            String buyerEmail, Carrier carrier,
            JsonNode from, JsonNode to, long legacyEffectiveFee, Long checkoutShippingFee, Long carrierQuotedFee,
            String currency, long cod, Integer minDays, Integer maxDays, String key) {
        var s = new ShipmentEntity();
        s.id = UuidV7.generate();
        s.orderId = orderId;
        s.checkoutGroupId = checkoutGroupId;
        s.shopId = shopId;
        s.buyerUserId = buyerUserId;
        s.buyerEmail = buyerEmail;
        s.carrier = carrier;
        s.fromAddressSnapshot = from.deepCopy();
        s.toAddressSnapshot = to.deepCopy();
        s.shippingFee = legacyEffectiveFee;
        s.checkoutShippingFee = checkoutShippingFee;
        s.carrierQuotedFee = carrierQuotedFee;
        s.currency = currency;
        s.codAmount = cod;
        s.estimatedDeliveryMinDate = minDays == null ? null
                : LocalDate.now(java.time.Clock.systemUTC()).plusDays(minDays);
        s.estimatedDeliveryMaxDate = maxDays == null ? null
                : LocalDate.now(java.time.Clock.systemUTC()).plusDays(maxDays);
        s.createIdempotencyKey = key;
        s.status = ShipmentStatus.PENDING_RECONCILIATION;
        s.createdAt = now();
        s.updatedAt = s.createdAt;
        return s;
    }

    public void carrierCreated(String externalId, String tracking) {
        this.externalShipmentId = externalId;
        this.trackingCode = tracking;
        this.status = ShipmentStatus.CREATED;
        this.updatedAt = now();
    }

    public void changeStatus(ShipmentStatus next) {
        this.status = next;
        this.updatedAt = now();
    }

    private static LocalDateTime now() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public UUID getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCheckoutGroupId() {
        return checkoutGroupId;
    }

    public String getShopId() {
        return shopId;
    }

    public String getBuyerUserId() {
        return buyerUserId;
    }

    public String getBuyerEmail() {
        return buyerEmail;
    }

    public Carrier getCarrier() {
        return carrier;
    }

    public String getTrackingCode() {
        return trackingCode;
    }

    public ShipmentStatus getStatus() {
        return status;
    }

    public JsonNode getFromAddressSnapshot() {
        return fromAddressSnapshot;
    }

    public JsonNode getToAddressSnapshot() {
        return toAddressSnapshot;
    }

    public long getShippingFee() {
        return shippingFee;
    }

    public Long getCarrierQuotedFee() {
        return carrierQuotedFee;
    }

    public Long getCheckoutShippingFee() {
        return checkoutShippingFee;
    }

    public Long getCarrierActualFee() {
        return carrierActualFee;
    }

    public String getCurrency() {
        return currency;
    }

    public long getCodAmount() {
        return codAmount;
    }

    public LocalDate getEstimatedDeliveryMinDate() {
        return estimatedDeliveryMinDate;
    }

    public LocalDate getEstimatedDeliveryMaxDate() {
        return estimatedDeliveryMaxDate;
    }

    public String getExternalShipmentId() {
        return externalShipmentId;
    }

    public String getCreateIdempotencyKey() {
        return createIdempotencyKey;
    }

    public long getVersion() {
        return version;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
}
