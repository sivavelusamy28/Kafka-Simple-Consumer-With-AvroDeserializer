// 1. Main Consumer Class - SimpleDelayKafkaConsumer.java
package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

@Service
public class SimpleDelayKafkaConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(SimpleDelayKafkaConsumer.class);
    private static final long DELAY_MILLIS = 5000; // 5 seconds
    
    @KafkaListener(
        topics = "${kafka.topic.name}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithDelay(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        try {
            if (records.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }
            
            // Get the newest (last) record in the batch
            ConsumerRecord<String, String> newestRecord = records.get(records.size() - 1);
            long newestRecordTime = newestRecord.timestamp();
            
            // Calculate when this newest record can be released (5 seconds after it entered Kafka)
            long releaseTime = newestRecordTime + DELAY_MILLIS;
            
            // Check if we need to wait
            long currentTime = System.currentTimeMillis();
            long waitTime = releaseTime - currentTime;
            
            if (waitTime > 0) {
                log.info("Waiting {} ms to ensure 5-second delay. Batch size: {}", 
                    waitTime, records.size());
                Thread.sleep(waitTime);
            } else {
                log.info("No wait needed. Records already aged. Batch size: {}", 
                    records.size());
            }
            
            // Now process all records - they've all waited at least 5 seconds
            processBatch(records);
            
            // Acknowledge after successful processing
            acknowledgment.acknowledge();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting", e);
        } catch (Exception e) {
            log.error("Error processing batch", e);
            // Don't acknowledge on error - let Kafka retry
        }
    }
    
    private void processBatch(List<ConsumerRecord<String, String>> records) {
        log.info("Processing {} records", records.size());
        
        for (ConsumerRecord<String, String> record : records) {
            try {
                // YOUR BUSINESS LOGIC GOES HERE
                processRecord(record);
            } catch (Exception e) {
                log.error("Error processing record with key: {}", record.key(), e);
                // Handle individual record failures as needed
            }
        }
    }
    
    private void processRecord(ConsumerRecord<String, String> record) {
        // YOUR ACTUAL BUSINESS LOGIC
        // Example:
        log.debug("Processing: key={}, value={}", record.key(), record.value());
        
        // Add your processing logic here:
        // - Parse the message
        // - Save to database
        // - Call external service
        // - etc.
    }
}

// ============================================================
// 2. Kafka Configuration Class - KafkaConsumerConfig.java
// ============================================================
package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // Batch settings
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // CRITICAL: Prevent rebalancing during 5-second delay
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Enable batch processing
        factory.setBatchListener(true);
        
        // Manual acknowledgment
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        return factory;
    }
}

// ============================================================
// 3. Application Properties - application.yml
// ============================================================
/*
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: delayed-consumer-group
      enable-auto-commit: false
      max-poll-records: 500
      properties:
        max.poll.interval.ms: 300000  # 5 minutes - prevents rebalancing

kafka:
  topic:
    name: your-topic-name

logging:
  level:
    root: INFO
    com.example.kafka: DEBUG
*/

// ============================================================
// 4. Main Application Class - KafkaDelayApplication.java
// ============================================================
package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaDelayApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaDelayApplication.class, args);
    }
}

// ============================================================
// 5. Maven Dependencies - pom.xml
// ============================================================
/*
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    
    <!-- Optional but recommended -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>
*/