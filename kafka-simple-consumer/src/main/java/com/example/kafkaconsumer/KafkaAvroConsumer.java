package com.example.kafkaconsumer;

import com.example.kafkaconsumer.avro.UserEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka consumer for processing Avro-encoded UserEvent messages.
 * 
 * <p>Features: manual acknowledgment, error handling, retry logic, DLQ support.</p>
 * 
 * @author Your Name
 * @version 1.0.0
 * @since 1.0.0
 */
@Service
@ConditionalOnProperty(name = "kafka.consumer.enabled", havingValue = "true")
public class KafkaAvroConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);
    private static final Logger errorLogger = LoggerFactory.getLogger("ERROR_LOGGER");
    
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    @Value("${kafka.consumer.topic}")
    private String topic;

    @Value("${kafka.consumer.error.topic:${kafka.consumer.topic}-dlq}")
    private String deadLetterTopic;

    @Value("${kafka.consumer.max.retries:3}")
    private int maxRetries;

    @Value("${kafka.consumer.retry.delay.ms:1000}")
    private long retryDelayMs;

    @Value("${kafka.consumer.backoff.multiplier:2.0}")
    private double backoffMultiplier;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Processes Kafka messages with comprehensive error handling.
     * 
     * <p>Error handling strategy:</p>
     * <ul>
     *   <li>Validation errors → DLQ</li>
     *   <li>Transient errors → Retry with backoff</li>
     *   <li>Permanent errors → DLQ</li>
     * </ul>
     * 
     * @param record Kafka record containing UserEvent
     * @param acknowledgment Manual acknowledgment control
     * @throws ValidationException when validation fails
     * @throws TransientException when retryable error occurs
     */
    @KafkaListener(topics = "${kafka.consumer.topic}", containerFactory = "kafkaListenerContainerFactory")
    @Retryable(
        value = {Exception.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    public void listen(ConsumerRecord<String, UserEvent> record, Acknowledgment acknowledgment) {
        String messageId = generateMessageId(record);
        long startTime = System.currentTimeMillis();
        
        try {
            validateMessage(record);
            UserEvent event = record.value();
            processUserEvent(event, record);
            acknowledgment.acknowledge();
            
            long processingTime = System.currentTimeMillis() - startTime;
            int processed = processedCount.incrementAndGet();
            
            logger.info("Successfully processed message: messageId={}, userId={}, eventType={}, partition={}, offset={}, processingTime={}ms, totalProcessed={}",
                    messageId, event.getUserId(), event.getEventType(), record.partition(), record.offset(), processingTime, processed);
                    
        } catch (ValidationException e) {
            errorLogger.error("Validation error for message: messageId={}, partition={}, offset={}, error={}",
                    messageId, record.partition(), record.offset(), e.getMessage());
            acknowledgment.acknowledge();
            sendToDeadLetterQueue(record, e.getMessage(), "VALIDATION_ERROR");
            
        } catch (TransientException e) {
            int errors = errorCount.incrementAndGet();
            errorLogger.warn("Transient error for message: messageId={}, partition={}, offset={}, error={}, retryCount={}",
                    messageId, record.partition(), record.offset(), e.getMessage(), errors);
            throw e;
            
        } catch (Exception e) {
            int errors = errorCount.incrementAndGet();
            errorLogger.error("Permanent error for message: messageId={}, partition={}, offset={}, error={}, totalErrors={}",
                    messageId, record.partition(), record.offset(), e.getMessage(), errors, e);
            
            sendToDeadLetterQueue(record, e.getMessage(), "PERMANENT_ERROR");
            acknowledgment.acknowledge();
        }
    }

    /**
     * Validates message structure and content.
     * 
     * @param record Consumer record to validate
     * @throws ValidationException if validation fails
     */
    private void validateMessage(ConsumerRecord<String, UserEvent> record) {
        if (record.value() == null) {
            throw new ValidationException("Message value is null");
        }
        
        UserEvent event = record.value();
        if (event.getUserId() == null || event.getUserId().trim().isEmpty()) {
            throw new ValidationException("User ID is null or empty");
        }
        
        if (event.getEventType() == null || event.getEventType().trim().isEmpty()) {
            throw new ValidationException("Event type is null or empty");
        }
        
        if (event.getTimestamp() <= 0) {
            throw new ValidationException("Invalid timestamp: " + event.getTimestamp());
        }
    }

    /**
     * Processes UserEvent according to business logic.
     * 
     * @param event UserEvent to process
     * @param record Original Kafka record for context
     * @throws TransientException if processing is interrupted
     */
    private void processUserEvent(UserEvent event, ConsumerRecord<String, UserEvent> record) {
        try {
            Thread.sleep(10); // Simulate processing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientException("Processing interrupted", e);
        }
        
        logger.debug("Processing UserEvent: userId={}, eventType={}, timestamp={}",
                event.getUserId(), event.getEventType(), event.getTimestamp());
    }

    /**
     * Sends failed message to dead letter queue.
     * 
     * @param record Original Kafka record
     * @param errorMessage Error description
     * @param errorType Error category
     */
    private void sendToDeadLetterQueue(ConsumerRecord<String, UserEvent> record, String errorMessage, String errorType) {
        try {
            DeadLetterMessage dlqMessage = new DeadLetterMessage(
                    record.key(),
                    record.value(),
                    errorMessage,
                    errorType,
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    record.partition(),
                    record.offset(),
                    record.topic()
            );
            
            kafkaTemplate.send(deadLetterTopic, record.key(), dlqMessage);
            logger.info("Sent message to DLQ: topic={}, partition={}, offset={}, errorType={}",
                    record.topic(), record.partition(), record.offset(), errorType);
                    
        } catch (Exception e) {
            errorLogger.error("Failed to send message to DLQ: partition={}, offset={}, error={}",
                    record.partition(), record.offset(), e.getMessage(), e);
        }
    }

    /**
     * Generates unique message ID for tracking.
     * 
     * @param record Kafka record
     * @return Unique message identifier
     */
    private String generateMessageId(ConsumerRecord<String, UserEvent> record) {
        return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
    }

    /**
     * Exception for validation failures.
     */
    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

    /**
     * Exception for transient errors (retryable).
     */
    public static class TransientException extends RuntimeException {
        public TransientException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Dead letter queue message wrapper.
     */
    public static class DeadLetterMessage {
        private final String originalKey;
        private final UserEvent originalValue;
        private final String errorMessage;
        private final String errorType;
        private final String timestamp;
        private final int partition;
        private final long offset;
        private final String originalTopic;

        public DeadLetterMessage(String originalKey, UserEvent originalValue, String errorMessage, 
                               String errorType, String timestamp, int partition, long offset, String originalTopic) {
            this.originalKey = originalKey;
            this.originalValue = originalValue;
            this.errorMessage = errorMessage;
            this.errorType = errorType;
            this.timestamp = timestamp;
            this.partition = partition;
            this.offset = offset;
            this.originalTopic = originalTopic;
        }

        public String getOriginalKey() { return originalKey; }
        public UserEvent getOriginalValue() { return originalValue; }
        public String getErrorMessage() { return errorMessage; }
        public String getErrorType() { return errorType; }
        public String getTimestamp() { return timestamp; }
        public int getPartition() { return partition; }
        public long getOffset() { return offset; }
        public String getOriginalTopic() { return originalTopic; }
    }
} 