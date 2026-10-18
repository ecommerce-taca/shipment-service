package com.taca.shipment.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class OutboxClaimServiceTest {

    @Test
    void rejectsLeaseThatWouldOverflowDurationNanos() {
        assertThrows(IllegalArgumentException.class,
                () -> new OutboxClaimService(
                        mock(JdbcTemplate.class),
                        mock(PlatformTransactionManager.class),
                        OutboxClaimService.MAX_LEASE_SECONDS + 1));
    }

    @Test
    void rejectsOverflowingPerClaimDurationBeforeStartingATransaction() {
        OutboxClaimService service = new OutboxClaimService(
                mock(JdbcTemplate.class),
                mock(PlatformTransactionManager.class),
                60);

        assertThrows(IllegalArgumentException.class,
                () -> service.claimNext(Duration.ofSeconds(OutboxClaimService.MAX_LEASE_SECONDS + 1)));
    }
}
