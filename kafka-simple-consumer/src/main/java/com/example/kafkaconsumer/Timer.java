import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class OptimizedBatchDelayConsumer {
    
    private static final long DELAY_SECONDS = 5;
    
    @Value("${kafka.consumer.max-poll-interval-ms:300000}") // 5 minutes default
    private long maxPollIntervalMs;
    
    @Value("${kafka.consumer.heartbeat-interval-ms:3000}") // 3 seconds default
    private long heartbeatIntervalMs;
    
    /**
     * Optimized approach that checks the newest (last) record in batch
     * but calculates the exact wait time needed
     */
    @KafkaListener(
        topics = "${kafka.topic.name}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeBatchWithSmartDelay(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("Received batch of {} records", records.size());
            
            if (records.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }
            
            // Get the NEWEST record (last in batch) - this ensures all records are at least 5 seconds old
            ConsumerRecord<String, String> newestRecord = records.get(records.size() - 1);
            long newestTimestamp = newestRecord.timestamp();
            
            // Calculate how long to wait
            long targetProcessTime = newestTimestamp + (DELAY_SECONDS * 1000);
            long currentTime = System.currentTimeMillis();
            long waitTime = targetProcessTime - currentTime;
            
            // Log batch time span for monitoring
            ConsumerRecord<String, String> oldestRecord = records.get(0);
            long batchTimeSpan = newestTimestamp - oldestRecord.timestamp();
            log.info("Batch time span: {} ms (oldest: {}, newest: {})", 
                batchTimeSpan,
                Instant.ofEpochMilli(oldestRecord.timestamp()),
                Instant.ofEpochMilli(newestTimestamp));
            
            if (waitTime > 0) {
                // Safety check: ensure we don't wait longer than max.poll.interval.ms
                long maxSafeWaitTime = maxPollIntervalMs - (currentTime - startTime) - 10000; // 10s safety margin
                
                if (waitTime > maxSafeWaitTime) {
                    log.warn("Required wait time {} ms exceeds safe limit {} ms. Processing immediately to avoid rebalancing.",
                        waitTime, maxSafeWaitTime);
                    waitTime = 0;
                } else {
                    log.info("Waiting {} ms before processing batch (newest record needs {} second delay)",
                        waitTime, DELAY_SECONDS);
                    
                    // Smart wait with periodic heartbeats (Spring Kafka handles this automatically)
                    Thread.sleep(waitTime);
                }
            } else {
                log.info("All records in batch are older than {} seconds, processing immediately", DELAY_SECONDS);
            }
            
            // Process the batch
            processBatch(records);
            
            // Acknowledge after successful processing
            acknowledgment.acknowledge();
            
            long totalProcessingTime = System.currentTimeMillis() - startTime;
            log.info("Batch processed successfully in {} ms", totalProcessingTime);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting", e);
            // Don't acknowledge - let Kafka retry
        } catch (Exception e) {
            log.error("Error processing batch", e);
            // Depending on your error handling strategy:
            // Option 1: Don't acknowledge, let Kafka retry
            // Option 2: Send to DLQ and acknowledge
            // Option 3: Partial processing with seek
        }
    }
    
    /**
     * Alternative approach: Check multiple timestamps for better accuracy
     */
    @KafkaListener(
        topics = "${kafka.topic.name.advanced}",
        containerFactory = "kafkaListenerContainerFactory",
        autoStartup = "false" // Disable by default, enable if you want to use this approach
    )
    public void consumeBatchWithAdvancedDelay(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("Advanced: Received batch of {} records", records.size());
            
            if (records.isEmpty()) {
                acknowledgment.acknowledge();
                return;
            }
            
            // Sample approach: Check first, middle, and last records
            int size = records.size();
            ConsumerRecord<String, String> firstRecord = records.get(0);
            ConsumerRecord<String, String> middleRecord = records.get(size / 2);
            ConsumerRecord<String, String> lastRecord = records.get(size - 1);
            
            // Use the most recent timestamp to ensure all records meet the delay requirement
            long mostRecentTimestamp = Math.max(
                Math.max(firstRecord.timestamp(), middleRecord.timestamp()),
                lastRecord.timestamp()
            );
            
            // Calculate precise wait time
            long targetProcessTime = mostRecentTimestamp + (DELAY_SECONDS * 1000);
            long currentTime = System.currentTimeMillis();
            long waitTime = targetProcessTime - currentTime;
            
            // Validate wait time against consumer timeout
            if (waitTime > 0 && waitTime < (maxPollIntervalMs - 20000)) { // 20s safety margin
                log.info("Advanced: Waiting {} ms for batch delay", waitTime);
                Thread.sleep(waitTime);
            } else if (waitTime >= (maxPollIntervalMs - 20000)) {
                log.warn("Advanced: Wait time {} ms too long, processing immediately", waitTime);
            }
            
            processBatch(records);
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("Advanced: Error processing batch", e);
        }
    }
    
    private void processBatch(List<ConsumerRecord<String, String>> records) {
        // Process records in parallel for better performance
        records.parallelStream().forEach(record -> {
            try {
                processSingleRecord(record);
            } catch (Exception e) {
                log.error("Error processing record with key: {}", record.key(), e);
                // Track failed records for retry or DLQ
            }
        });
    }
    
    private void processSingleRecord(ConsumerRecord<String, String> record) {
        long actualDelay = System.currentTimeMillis() - record.timestamp();
        
        log.debug("Processing record: key={}, actualDelay={}ms, timestamp={}", 
            record.key(), 
            actualDelay,
            Instant.ofEpochMilli(record.timestamp()));
        
        // Your business logic here
        // ...
    }
}