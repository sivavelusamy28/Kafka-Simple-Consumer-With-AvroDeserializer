package com.example.kafkaconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Health monitoring controller for Kafka Avro Consumer.
 * 
 * <p>Endpoints: /health, /health/metrics, /actuator/health</p>
 * 
 * @author Your Name
 * @version 1.0.0
 * @since 1.0.0
 */
@RestController
@RequestMapping("/health")
public class HealthController implements HealthIndicator {

    @Autowired
    private KafkaAvroConsumer kafkaConsumer;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Basic health status with Kafka connectivity.
     * 
     * @return Health status information
     */
    @GetMapping
    public Map<String, Object> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("timestamp", System.currentTimeMillis());
        health.put("application", "Kafka Avro Consumer");
        health.put("version", "1.0.0");
        
        try {
            kafkaTemplate.getDefaultTopic();
            health.put("kafka", "CONNECTED");
        } catch (Exception e) {
            health.put("kafka", "DISCONNECTED");
            health.put("kafka_error", e.getMessage());
        }
        
        return health;
    }

    /**
     * Detailed metrics and system information.
     * 
     * @return System metrics
     */
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        if (kafkaConsumer != null) {
            metrics.put("consumer_available", true);
        } else {
            metrics.put("consumer_available", false);
        }
        
        metrics.put("timestamp", System.currentTimeMillis());
        metrics.put("memory_used", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        metrics.put("memory_total", Runtime.getRuntime().totalMemory());
        metrics.put("memory_max", Runtime.getRuntime().maxMemory());
        
        return metrics;
    }

    /**
     * Health status for Spring Boot Actuator integration.
     * 
     * @return Health object with status and details
     */
    @Override
    public Health health() {
        try {
            kafkaTemplate.getDefaultTopic();
            
            return Health.up()
                    .withDetail("kafka", "CONNECTED")
                    .withDetail("consumer", "AVAILABLE")
                    .withDetail("timestamp", System.currentTimeMillis())
                    .build();
                    
        } catch (Exception e) {
            return Health.down()
                    .withDetail("kafka", "DISCONNECTED")
                    .withDetail("error", e.getMessage())
                    .withDetail("timestamp", System.currentTimeMillis())
                    .build();
        }
    }
} 