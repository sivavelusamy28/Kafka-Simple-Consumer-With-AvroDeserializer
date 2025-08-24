import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class StatsLogger {
    private static final Logger logger = LoggerFactory.getLogger(StatsLogger.class);
    
    // Example stats object
    public static class StatEntry {
        private final String key;
        private final Object value;
        private final String category;
        private final long timestamp;
        
        public StatEntry(String key, Object value, String category) {
            this.key = key;
            this.value = value;
            this.category = category;
            this.timestamp = System.currentTimeMillis();
        }
        
        // Getters
        public String getKey() { return key; }
        public Object getValue() { return value; }
        public String getCategory() { return category; }
        public long getTimestamp() { return timestamp; }
    }
    
    /**
     * Method 1: Stream-based approach with predicates (most flexible)
     * Best for complex filtering logic
     */
    public static void logStatsWithStream(Collection<StatEntry> stats, 
                                         Predicate<StatEntry> filter,
                                         String contextId) {
        try {
            // Set context in MDC
            MDC.put("contextId", contextId);
            MDC.put("operation", "statsLogging");
            
            stats.parallelStream()  // Use parallel for large collections
                 .filter(filter)
                 .forEach(stat -> {
                     // Add stat-specific MDC context
                     MDC.put("statKey", stat.getKey());
                     MDC.put("statCategory", stat.getCategory());
                     
                     logger.info("Stat: {} = {} [category: {}]", 
                               stat.getKey(), stat.getValue(), stat.getCategory());
                     
                     // Clean up stat-specific MDC
                     MDC.remove("statKey");
                     MDC.remove("statCategory");
                 });
        } finally {
            // Clean up context MDC
            MDC.remove("contextId");
            MDC.remove("operation");
        }
    }
    
    /**
     * Method 2: Traditional loop with batching (memory efficient)
     * Best for very large collections that need memory management
     */
    public static void logStatsWithBatching(Collection<StatEntry> stats,
                                          Predicate<StatEntry> filter,
                                          String contextId,
                                          int batchSize) {
        try {
            MDC.put("contextId", contextId);
            MDC.put("operation", "batchedStatsLogging");
            
            Iterator<StatEntry> iterator = stats.iterator();
            int processed = 0;
            int batchCount = 0;
            
            while (iterator.hasNext()) {
                List<StatEntry> batch = new ArrayList<>(batchSize);
                
                // Collect batch
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                    StatEntry stat = iterator.next();
                    if (filter.test(stat)) {
                        batch.add(stat);
                    }
                }
                
                // Process batch
                if (!batch.isEmpty()) {
                    MDC.put("batchNumber", String.valueOf(++batchCount));
                    MDC.put("batchSize", String.valueOf(batch.size()));
                    
                    logger.info("Processing batch {} with {} stats", batchCount, batch.size());
                    
                    for (StatEntry stat : batch) {
                        MDC.put("statKey", stat.getKey());
                        logger.info("Stat: {} = {}", stat.getKey(), stat.getValue());
                        MDC.remove("statKey");
                        processed++;
                    }
                    
                    MDC.remove("batchNumber");
                    MDC.remove("batchSize");
                }
            }
            
            logger.info("Completed processing {} stats in {} batches", processed, batchCount);
            
        } finally {
            MDC.remove("contextId");
            MDC.remove("operation");
        }
    }
    
    /**
     * Method 3: Enhanced iterator with custom filtering (most performant)
     * Best for simple filters and maximum performance
     */
    public static void logStatsOptimized(Map<String, StatEntry> statsMap,
                                       Set<String> keysToLog,
                                       String contextId) {
        try {
            MDC.put("contextId", contextId);
            MDC.put("operation", "optimizedStatsLogging");
            
            int loggedCount = 0;
            
            // Direct key lookup - O(1) for each key vs O(n) filtering
            for (String key : keysToLog) {
                StatEntry stat = statsMap.get(key);
                if (stat != null) {
                    MDC.put("statKey", key);
                    MDC.put("statCategory", stat.getCategory());
                    
                    logger.info("Stat: {} = {} [{}]", 
                              key, stat.getValue(), stat.getCategory());
                    
                    MDC.remove("statKey");
                    MDC.remove("statCategory");
                    loggedCount++;
                }
            }
            
            logger.info("Logged {} out of {} requested stats", loggedCount, keysToLog.size());
            
        } finally {
            MDC.remove("contextId");
            MDC.remove("operation");
        }
    }
    
    /**
     * Method 4: Asynchronous logging for very large collections
     */
    public static void logStatsAsync(Collection<StatEntry> stats,
                                   Predicate<StatEntry> filter,
                                   String contextId) {
        // For async, we need to capture MDC context
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        
        // Process asynchronously
        stats.parallelStream()
             .filter(filter)
             .forEach(stat -> {
                 // Set MDC context in the thread
                 if (contextMap != null) {
                     MDC.setContextMap(contextMap);
                 }
                 MDC.put("contextId", contextId);
                 MDC.put("statKey", stat.getKey());
                 
                 logger.info("Async Stat: {} = {}", stat.getKey(), stat.getValue());
                 
                 // Clean up
                 MDC.clear();
             });
    }
    
    // Example usage and testing
    public static void main(String[] args) {
        // Create sample large collection
        List<StatEntry> largeStatsList = new ArrayList<>();
        Map<String, StatEntry> statsMap = new ConcurrentHashMap<>();
        
        // Populate with sample data
        for (int i = 0; i < 100000; i++) {
            String key = "metric_" + i;
            StatEntry stat = new StatEntry(key, i * 1.5, 
                                         i % 3 == 0 ? "performance" : "business");
            largeStatsList.add(stat);
            statsMap.put(key, stat);
        }
        
        // Example 1: Log only performance metrics
        System.out.println("=== Stream-based filtering ===");
        logStatsWithStream(largeStatsList.subList(0, 1000), // First 1000 for demo
                          stat -> "performance".equals(stat.getCategory()),
                          "ctx-001");
        
        // Example 2: Batched processing
        System.out.println("\n=== Batched processing ===");
        logStatsWithBatching(largeStatsList.subList(0, 1000),
                           stat -> stat.getValue() instanceof Number && 
                                  ((Number)stat.getValue()).doubleValue() > 50,
                           "ctx-002", 100);
        
        // Example 3: Optimized lookup
        System.out.println("\n=== Optimized lookup ===");
        Set<String> keysToLog = Set.of("metric_10", "metric_50", "metric_100");
        logStatsOptimized(statsMap, keysToLog, "ctx-003");
    }
    
    // Utility class for common filters
    public static class StatFilters {
        public static Predicate<StatEntry> byCategory(String category) {
            return stat -> category.equals(stat.getCategory());
        }
        
        public static Predicate<StatEntry> byValueThreshold(double threshold) {
            return stat -> stat.getValue() instanceof Number && 
                          ((Number)stat.getValue()).doubleValue() > threshold;
        }
        
        public static Predicate<StatEntry> recentOnly(long maxAgeMs) {
            long cutoff = System.currentTimeMillis() - maxAgeMs;
            return stat -> stat.getTimestamp() > cutoff;
        }
        
        public static Predicate<StatEntry> keyPattern(String pattern) {
            return stat -> stat.getKey().matches(pattern);
        }
    }
}