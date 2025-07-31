package com.example.kafkaconsumer;

import com.example.kafkaconsumer.avro.UserEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "kafka.consumer.enabled", havingValue = "true")
public class KafkaAvroConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);

    @Value("${kafka.consumer.topic}")
    private String topic;

    @KafkaListener(topics = "${kafka.consumer.topic}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, UserEvent> record, Acknowledgment acknowledgment) {
        try {
            UserEvent event = record.value();
            logger.info("Received UserEvent: userId={}, eventType={}, timestamp={}, partition={}, offset={}",
                    event.getUserId(), event.getEventType(), event.getTimestamp(), record.partition(), record.offset());
            // Process the event here
            acknowledgment.acknowledge();
        } catch (Exception e) {
            logger.error("Error processing UserEvent: ", e);
            // Optionally, you can send the message to a dead-letter topic or take other actions
        }
    }
} 