package com.example.kafkaconsumer;

import com.example.kafkaconsumer.avro.UserEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka producer for sending Avro-encoded UserEvent messages.
 * 
 * <p>Features: async sending, error handling, schema registry integration.</p>
 * 
 * @author Your Name
 * @version 1.0.0
 * @since 1.0.0
 */
@Service
public class KafkaAvroProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaAvroProducer.class);

    @Autowired
    private KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${kafka.consumer.topic}")
    private String topic;

    /**
     * Sends UserEvent with specified userId and eventType.
     * 
     * @param userId User identifier
     * @param eventType Event type (e.g., "LOGIN", "PURCHASE")
     * @throws IllegalArgumentException if parameters are null or empty
     */
    public void sendUserEvent(String userId, String eventType) {
        if (userId == null || userId.trim().isEmpty()) {
            throw new IllegalArgumentException("userId cannot be null or empty");
        }
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new IllegalArgumentException("eventType cannot be null or empty");
        }
        
        UserEvent userEvent = new UserEvent(userId, eventType, System.currentTimeMillis());
        sendUserEvent(userEvent);
    }

    /**
     * Sends UserEvent to Kafka with error handling.
     * 
     * @param userEvent UserEvent to send
     * @throws IllegalArgumentException if userEvent is null
     */
    public void sendUserEvent(UserEvent userEvent) {
        if (userEvent == null) {
            throw new IllegalArgumentException("userEvent cannot be null");
        }
        
        ProducerRecord<String, UserEvent> record = new ProducerRecord<>(topic, userEvent.getUserId(), userEvent);
        
        CompletableFuture<SendResult<String, UserEvent>> future = kafkaTemplate.send(record);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Successfully sent UserEvent: userId={}, eventType={}, timestamp={}, partition={}, offset={}",
                        userEvent.getUserId(), userEvent.getEventType(), userEvent.getTimestamp(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send UserEvent: userId={}, eventType={}, error={}", 
                        userEvent.getUserId(), userEvent.getEventType(), ex.getMessage(), ex);
            }
        });
    }
} 