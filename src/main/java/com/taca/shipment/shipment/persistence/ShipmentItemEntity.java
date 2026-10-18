package com.taca.shipment.shipment.persistence;

import com.taca.shipment.common.id.UuidV7;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "shipment_items", indexes = { @Index(name = "idx_shipment_items_shipment", columnList = "shipment_id"),
        @Index(name = "idx_shipment_items_sku_created", columnList = "sku_id,created_at") })
public class ShipmentItemEntity {
    @Id
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
    @JdbcTypeCode(SqlTypes.BINARY)
    @Column(name = "shipment_id", nullable = false, columnDefinition = "BINARY(16)")
    private UUID shipmentId;
    @Column(name = "sku_id", nullable = false, length = 80)
    private String skuId;
    @Column(name = "product_id", length = 80)
    private String productId;
    @Column(name = "title_snapshot", nullable = false, length = 255)
    private String titleSnapshot;
    @Column(nullable = false)
    private int quantity;
    @Column(name = "weight_grams")
    private Integer weightGrams;
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    protected ShipmentItemEntity() {
    }

    public static ShipmentItemEntity create(UUID shipmentId, String skuId, String productId, String title, int quantity,
            Integer weight) {
        var i = new ShipmentItemEntity();
        i.id = UuidV7.generate();
        i.shipmentId = shipmentId;
        i.skuId = skuId;
        i.productId = productId;
        i.titleSnapshot = title;
        i.quantity = quantity;
        i.weightGrams = weight;
        i.createdAt = LocalDateTime.now(ZoneOffset.UTC);
        return i;
    }
}
