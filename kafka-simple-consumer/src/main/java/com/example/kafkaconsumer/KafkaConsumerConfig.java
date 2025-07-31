package com.example.kafkaconsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka consumer with Avro deserialization.
 * 
 * <p>Features: manual acknowledgment, retry logic, error handling, performance tuning.</p>
 * 
 * @author Your Name
 * @version 1.0.0
 * @since 1.0.0
 */
@Configuration
@ConditionalOnProperty(name = "kafka.consumer.enabled", havingValue = "true")
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.consumer.max.retries:3}")
    private int maxRetries;

    @Value("${kafka.consumer.retry.delay.ms:1000}")
    private long retryDelayMs;

    /**
     * Creates Kafka consumer factory with Avro deserialization.
     * 
     * @return Configured ConsumerFactory
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Basic Kafka Configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        
        // Schema Registry Configuration
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", true);
        
        // Consumer Behavior Configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000);
        
        // Error Handling Configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Performance Configuration
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Creates Kafka listener container factory with error handling.
     * 
     * @return Configured ConcurrentKafkaListenerContainerFactory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Listener Configuration
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setFixPartitionOffsets(true);
        
        // Error Handler Configuration
        ExponentialBackOff backOff = new ExponentialBackOff(retryDelayMs, 2.0);
        backOff.setMaxElapsedTime(60000); // 1 minute max
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (consumerRecord, exception) -> {
                // Log the error and send to DLQ
                System.err.println("Error processing record: " + consumerRecord + ", error: " + exception.getMessage());
            },
            backOff
        );
        
        // Configure which exceptions to retry
        errorHandler.addRetryableExceptions(
            org.springframework.kafka.listener.ListenerExecutionFailedException.class,
            org.springframework.kafka.support.serializer.DeserializationException.class
        );
        
        // Configure which exceptions to skip
        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class
        );
        
        factory.setCommonErrorHandler(errorHandler);
        
        return factory;
    }
} 