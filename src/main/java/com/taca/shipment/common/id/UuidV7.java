package com.taca.shipment.common.id;

import java.security.SecureRandom;
import java.util.UUID;

/** UUIDv7 generator: millisecond timestamp in the high bits and random tail. */
public final class UuidV7 {
    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static UUID generate() {
        long millis = System.currentTimeMillis();
        long most = (millis & 0xFFFFFFFFFFFFL) << 16;
        most |= 0x7000L | (RANDOM.nextInt(1 << 12));
        long least = RANDOM.nextLong();
        least &= 0x3FFFFFFFFFFFFFFFL;
        least |= 0x8000000000000000L;
        return new UUID(most, least);
    }
}
