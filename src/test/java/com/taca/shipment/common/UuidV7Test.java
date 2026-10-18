package com.taca.shipment.common;

import com.taca.shipment.common.id.UuidV7;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UuidV7Test {
    @Test
    void generatedUuidHasVersionSevenAndRfcVariant() {
        var id = UuidV7.generate();
        assertEquals(7, id.version());
        assertEquals(2, id.variant());
    }
}
