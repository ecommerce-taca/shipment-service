package com.taca.shipment.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;
import java.util.Map;

@RestController
public class HealthController {
    private final JdbcTemplate jdbc;
    private final String topic;

    public HealthController(JdbcTemplate jdbc, @Value("${shipment.kafka.shipment-events-topic}") String topic) {
        this.jdbc = jdbc;
        this.topic = topic;
    }

    @GetMapping("/health/live")
    public Map<String, String> live() {
        return Map.of("status", "UP");
    }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, String>> ready() {
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            if (topic == null || topic.isBlank())
                return ResponseEntity.status(503)
                        .body(Map.of("status", "DOWN", "reason", "KAFKA_TOPIC_NOT_CONFIGURED"));
            return ResponseEntity.ok(Map.of("status", "UP", "database", "UP", "kafka_topic_configured", "true"));
        } catch (Exception ex) {
            return ResponseEntity.status(503).body(Map.of("status", "DOWN", "database", "DOWN"));
        }
    }
}
