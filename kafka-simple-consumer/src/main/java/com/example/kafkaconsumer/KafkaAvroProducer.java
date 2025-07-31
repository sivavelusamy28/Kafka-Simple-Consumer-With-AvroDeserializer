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

@Service
public class KafkaAvroProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaAvroProducer.class);

    @Autowired
    private KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${kafka.consumer.topic}")
    private String topic;

    public void sendUserEvent(String userId, String eventType) {
        UserEvent userEvent = new UserEvent(userId, eventType, System.currentTimeMillis());
        sendUserEvent(userEvent);
    }

    public void sendUserEvent(UserEvent userEvent) {
        ProducerRecord<String, UserEvent> record = new ProducerRecord<>(topic, userEvent.getUserId(), userEvent);
        
        CompletableFuture<SendResult<String, UserEvent>> future = kafkaTemplate.send(record);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Successfully sent UserEvent: userId={}, eventType={}, timestamp={}, partition={}, offset={}",
                        userEvent.getUserId(), userEvent.getEventType(), userEvent.getTimestamp(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send UserEvent: userId={}, eventType={}", 
                        userEvent.getUserId(), userEvent.getEventType(), ex);
            }
        });
    }
} 