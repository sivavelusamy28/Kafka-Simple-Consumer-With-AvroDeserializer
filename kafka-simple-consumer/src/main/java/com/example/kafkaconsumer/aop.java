// KafkaMetricsAspect.java
package com.example.kafka.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Gauge;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Aspect
@Component
public class KafkaMetricsAspect {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsAspect.class);
    
    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, Counter> messageCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> processingTimers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> tpsCalculators = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastTpsValue = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastResetTime = new ConcurrentHashMap<>();
    
    public KafkaMetricsAspect(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    // Pointcut for any method annotated with @KafkaConsumerMetrics
    @Pointcut("@annotation(kafkaConsumerMetrics)")
    public void kafkaConsumerMethod(KafkaConsumerMetrics kafkaConsumerMetrics) {}
    
    // Pointcut for KafkaListener annotated methods
    @Pointcut("@annotation(org.springframework.kafka.annotation.KafkaListener)")
    public void kafkaListenerMethod() {}
    
    // Around advice for methods with @KafkaConsumerMetrics annotation
    @Around("kafkaConsumerMethod(kafkaConsumerMetrics)")
    public Object measureKafkaConsumer(ProceedingJoinPoint joinPoint, 
                                       KafkaConsumerMetrics kafkaConsumerMetrics) throws Throwable {
        
        String topic = extractTopic(joinPoint, kafkaConsumerMetrics);
        if (topic == null) {
            return joinPoint.proceed();
        }
        
        return processWithMetrics(joinPoint, topic);
    }
    
    // Around advice for @KafkaListener methods
    @Around("kafkaListenerMethod()")
    public Object measureKafkaListener(ProceedingJoinPoint joinPoint) throws Throwable {
        
        String topic = extractTopicFromArgs(joinPoint);
        if (topic == null) {
            return joinPoint.proceed();
        }
        
        return processWithMetrics(joinPoint, topic);
    }
    
    private Object processWithMetrics(ProceedingJoinPoint joinPoint, String topic) throws Throwable {
        
        // Start timing
        long startNanos = System.nanoTime();
        
        try {
            // Record message received
            recordMessageReceived(topic);
            
            // Execute the actual method
            Object result = joinPoint.proceed();
            
            // Record successful processing time
            long processingTimeNanos = System.nanoTime() - startNanos;
            recordProcessingTime(topic, processingTimeNanos);
            
            if (log.isDebugEnabled()) {
                log.debug("Processed message for topic: {} in {} ms", 
                         topic, processingTimeNanos / 1_000_000);
            }
            
            return result;
            
        } catch (Exception e) {
            // Record error
            recordProcessingError(topic);
            
            log.error("Error processing message for topic: {}", topic, e);
            throw e;
        }
    }
    
    private String extractTopic(ProceedingJoinPoint joinPoint, KafkaConsumerMetrics annotation) {
        // First check if topic is specified in annotation
        if (!annotation.topic().isEmpty()) {
            return annotation.topic();
        }
        
        // Try to extract from method arguments
        return extractTopicFromArgs(joinPoint);
    }
    
    private String extractTopicFromArgs(ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord) {
                return ((ConsumerRecord<?, ?>) arg).topic();
            }
            if (arg instanceof String) {
                // Check if this is from @Header(KafkaHeaders.RECEIVED_TOPIC)
                MethodSignature signature = (MethodSignature) joinPoint.getSignature();
                // You might need more sophisticated parameter inspection here
                return null; // Simplified for brevity
            }
        }
        
        // If we have a ConsumerRecord in the first argument position
        if (args.length > 0 && args[0] instanceof ConsumerRecord) {
            return ((ConsumerRecord<?, ?>) args[0]).topic();
        }
        
        return null;
    }
    
    private void recordMessageReceived(String topic) {
        // Increment counter
        getOrCreateCounter(topic).increment();
        
        // Update TPS calculator
        getTpsCalculator(topic).increment();
    }
    
    private void recordProcessingTime(String topic, long durationNanos) {
        getOrCreateTimer(topic).record(durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }
    
    private void recordProcessingError(String topic) {
        getOrCreateErrorCounter(topic).increment();
    }
    
    @Scheduled(fixedDelay = 10000) // Calculate TPS every 10 seconds
    public void calculateTps() {
        long currentTime = System.currentTimeMillis();
        
        tpsCalculators.forEach((topic, adder) -> {
            long lastReset = lastResetTime.computeIfAbsent(topic, k -> currentTime).get();
            long timeDiffSeconds = (currentTime - lastReset) / 1000;
            
            if (timeDiffSeconds > 0) {
                long count = adder.sumThenReset();
                double tps = (double) count / timeDiffSeconds;
                
                lastTpsValue.computeIfAbsent(topic, k -> new AtomicLong())
                    .set(Double.doubleToLongBits(tps));
                
                lastResetTime.get(topic).set(currentTime);
            }
        });
    }
    
    private Counter getOrCreateCounter(String topic) {
        return messageCounters.computeIfAbsent(topic, t -> 
            Counter.builder("kafka.consumer.messages.consumed")
                .description("Total number of messages consumed")
                .tag("topic", t)
                .register(meterRegistry)
        );
    }
    
    private Timer getOrCreateTimer(String topic) {
        return processingTimers.computeIfAbsent(topic, t -> {
            // Register TPS gauge when timer is created
            registerTpsGauge(t);
            
            return Timer.builder("kafka.consumer.processing.time")
                .description("Message processing time")
                .tag("topic", t)
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .publishPercentileHistogram(false)
                .serviceLevelObjectives(
                    Duration.ofMillis(10),
                    Duration.ofMillis(50),
                    Duration.ofMillis(100),
                    Duration.ofMillis(250),
                    Duration.ofMillis(500),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(5)
                )
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(meterRegistry);
        });
    }
    
    private void registerTpsGauge(String topic) {
        AtomicLong tpsValue = lastTpsValue.computeIfAbsent(topic, k -> new AtomicLong());
        
        Gauge.builder("kafka.consumer.tps", tpsValue, al -> Double.longBitsToDouble(al.get()))
            .description("Messages processed per second")
            .tag("topic", topic)
            .register(meterRegistry);
    }
    
    private Counter getOrCreateErrorCounter(String topic) {
        return errorCounters.computeIfAbsent(topic, t ->
            Counter.builder("kafka.consumer.errors")
                .description("Number of processing errors")
                .tag("topic", t)
                .register(meterRegistry)
        );
    }
    
    private LongAdder getTpsCalculator(String topic) {
        return tpsCalculators.computeIfAbsent(topic, t -> new LongAdder());
    }
}

// KafkaConsumerMetrics.java - Custom annotation for explicit metrics
package com.example.kafka.metrics;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaConsumerMetrics {
    String topic() default "";
    boolean recordErrors() default true;
    boolean recordTiming() default true;
}

// SimplifiedKafkaConsumer.java - Clean consumer without metrics logic
package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class SimplifiedKafkaConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(SimplifiedKafkaConsumer.class);
    
    private final MessageProcessor messageProcessor;
    
    public SimplifiedKafkaConsumer(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }
    
    @KafkaListener(
        topics = "${kafka.consumer.topics}",
        containerFactory = "kafkaListenerContainerFactory",
        concurrency = "${kafka.consumer.concurrency:10}"
    )
    public void consume(ConsumerRecord<String, String> record,
                       Acknowledgment acknowledgment) {
        
        // Clean business logic - metrics handled by AOP
        messageProcessor.processMessage(
            record.value(), 
            record.key(), 
            record.timestamp()
        );
        
        // Acknowledge successful processing
        acknowledgment.acknowledge();
    }
}

// MessageProcessor.java - Can also use metrics annotation
package com.example.kafka.consumer;

import com.example.kafka.metrics.KafkaConsumerMetrics;
import org.springframework.stereotype.Component;

@Component
public class MessageProcessor {
    
    public void processMessage(String message, String key, long timestamp) {
        // Your business logic here
        validateMessage(message);
        transformAndProcess(message, key, timestamp);
    }
    
    // You can also apply metrics to specific processing methods
    @KafkaConsumerMetrics(topic = "orders")
    public void processOrderMessage(String message) {
        // Order-specific processing
    }
    
    @KafkaConsumerMetrics(topic = "payments")
    public void processPaymentMessage(String message) {
        // Payment-specific processing
    }
    
    private void validateMessage(String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Message cannot be empty");
        }
    }
    
    private void transformAndProcess(String message, String key, long timestamp) {
        // Implement your business logic
    }
}

// AopConfiguration.java
package com.example.kafka.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableAspectJAutoProxy
@EnableScheduling
public class AopConfiguration {
    // Enable AOP and scheduling for metrics
}

// AdvancedMetricsAspect.java - Additional metrics capabilities
package com.example.kafka.metrics;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Aspect
@Component
public class AdvancedMetricsAspect {
    
    private final MeterRegistry meterRegistry;
    
    public AdvancedMetricsAspect(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    // Measure any method annotated with @Timed
    @Around("@annotation(timed)")
    public Object measureTimedMethod(ProceedingJoinPoint joinPoint, Timed timed) throws Throwable {
        
        String metricName = timed.value().isEmpty() ? 
            joinPoint.getSignature().toShortString() : timed.value();
        
        List<Tag> tags = Arrays.asList(
            Tag.of("class", joinPoint.getTarget().getClass().getSimpleName()),
            Tag.of("method", joinPoint.getSignature().getName())
        );
        
        return meterRegistry.timer(metricName, tags)
            .recordCallable(() -> {
                try {
                    return joinPoint.proceed();
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            });
    }
    
    // Measure batch processing if needed
    @Around("@annotation(com.example.kafka.metrics.BatchMetrics)")
    public Object measureBatch(ProceedingJoinPoint joinPoint) throws Throwable {
        // Custom batch processing metrics
        return joinPoint.proceed();
    }
}

// BatchMetrics.java - Annotation for batch processing
package com.example.kafka.metrics;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BatchMetrics {
    String value() default "";
}