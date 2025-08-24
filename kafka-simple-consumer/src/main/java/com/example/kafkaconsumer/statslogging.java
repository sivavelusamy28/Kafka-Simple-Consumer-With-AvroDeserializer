import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import java.util.*;
import java.util.stream.Collectors;

public class StatsToMdcExtractor {
    private static final Logger logger = LoggerFactory.getLogger(StatsToMdcExtractor.class);
    
    /**
     * Method 1: Extract specific stats and put them directly in MDC
     * Most efficient for known keys
     */
    public static void extractStatsToMdc(Map<String, Object> statsObject, 
                                        List<String> keysToExtract,
                                        String mdcPrefix) {
        // Clear any existing MDC entries with our prefix first (optional)
        clearMdcWithPrefix(mdcPrefix);
        
        int extractedCount = 0;
        for (String key : keysToExtract) {
            Object value = statsObject.get(key);
            if (value != null) {
                // Store in MDC with prefix to avoid conflicts
                String mdcKey = mdcPrefix != null ? mdcPrefix + "." + key : key;
                MDC.put(mdcKey, String.valueOf(value));
                extractedCount++;
            }
        }
        
        // Store metadata about extraction
        MDC.put(mdcPrefix + ".extracted_count", String.valueOf(extractedCount));
        MDC.put(mdcPrefix + ".total_requested", String.valueOf(keysToExtract.size()));
        
        logger.info("Extracted {} out of {} requested stats to MDC", 
                   extractedCount, keysToExtract.size());
    }
    
    /**
     * Method 2: Extract stats and return them as a separate map + store in MDC
     * Useful when you need both the extracted data and MDC logging
     */
    public static Map<String, Object> extractAndStoreinMdc(Map<String, Object> statsObject,
                                                          List<String> keysToExtract,
                                                          String mdcPrefix) {
        Map<String, Object> extractedStats = new HashMap<>();
        
        for (String key : keysToExtract) {
            Object value = statsObject.get(key);
            if (value != null) {
                extractedStats.put(key, value);
                // Also store in MDC
                String mdcKey = mdcPrefix != null ? mdcPrefix + "." + key : key;
                MDC.put(mdcKey, String.valueOf(value));
            }
        }
        
        // Store extraction metadata
        MDC.put(mdcPrefix + ".extracted_count", String.valueOf(extractedStats.size()));
        
        logger.info("Extracted stats: {}", extractedStats.keySet());
        return extractedStats;
    }
    
    /**
     * Method 3: Stream-based extraction (functional approach)
     * Good for when you want to transform values while extracting
     */
    public static void extractStatsWithStream(Map<String, Object> statsObject,
                                            List<String> keysToExtract,
                                            String mdcPrefix) {
        Map<String, String> extractedForMdc = keysToExtract.stream()
            .filter(statsObject::containsKey)
            .collect(Collectors.toMap(
                key -> mdcPrefix != null ? mdcPrefix + "." + key : key,
                key -> String.valueOf(statsObject.get(key))
            ));
        
        // Bulk add to MDC
        extractedForMdc.forEach(MDC::put);
        
        // Add metadata
        MDC.put(mdcPrefix + ".extracted_count", String.valueOf(extractedForMdc.size()));
        
        logger.info("Stream extracted {} stats to MDC", extractedForMdc.size());
    }
    
    /**
     * Method 4: Enhanced version with value transformation and validation
     * Most comprehensive approach
     */
    public static void extractStatsAdvanced(Map<String, Object> statsObject,
                                          List<String> keysToExtract,
                                          String mdcPrefix,
                                          boolean includeNulls,
                                          int maxValueLength) {
        int extracted = 0;
        int skipped = 0;
        
        for (String key : keysToExtract) {
            Object value = statsObject.get(key);
            
            // Handle null values based on preference
            if (value == null && !includeNulls) {
                skipped++;
                continue;
            }
            
            // Convert value to string and handle length limits
            String stringValue = value != null ? String.valueOf(value) : "null";
            if (stringValue.length() > maxValueLength) {
                stringValue = stringValue.substring(0, maxValueLength) + "...";
            }
            
            // Store in MDC
            String mdcKey = mdcPrefix != null ? mdcPrefix + "." + key : key;
            MDC.put(mdcKey, stringValue);
            extracted++;
        }
        
        // Store comprehensive metadata
        MDC.put(mdcPrefix + ".extracted_count", String.valueOf(extracted));
        MDC.put(mdcPrefix + ".skipped_count", String.valueOf(skipped));
        MDC.put(mdcPrefix + ".total_stats_available", String.valueOf(statsObject.size()));
        
        logger.info("Advanced extraction: {} extracted, {} skipped from {} total keys requested", 
                   extracted, skipped, keysToExtract.size());
    }
    
    /**
     * Method 5: Batch extraction for very large key lists
     * Processes keys in batches to avoid memory issues
     */
    public static void extractStatsInBatches(Map<String, Object> statsObject,
                                           List<String> keysToExtract,
                                           String mdcPrefix,
                                           int batchSize) {
        int totalExtracted = 0;
        int batchNumber = 0;
        
        // Process in batches
        for (int i = 0; i < keysToExtract.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, keysToExtract.size());
            List<String> batch = keysToExtract.subList(i, endIndex);
            batchNumber++;
            
            int batchExtracted = 0;
            for (String key : batch) {
                Object value = statsObject.get(key);
                if (value != null) {
                    String mdcKey = mdcPrefix + ".batch" + batchNumber + "." + key;
                    MDC.put(mdcKey, String.valueOf(value));
                    batchExtracted++;
                }
            }
            
            totalExtracted += batchExtracted;
            MDC.put(mdcPrefix + ".batch" + batchNumber + ".count", String.valueOf(batchExtracted));
            
            logger.info("Batch {} processed: {} keys extracted", batchNumber, batchExtracted);
        }
        
        MDC.put(mdcPrefix + ".total_extracted", String.valueOf(totalExtracted));
        MDC.put(mdcPrefix + ".total_batches", String.valueOf(batchNumber));
    }
    
    /**
     * Utility method to clear MDC entries with specific prefix
     */
    public static void clearMdcWithPrefix(String prefix) {
        if (prefix == null) return;
        
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        if (contextMap != null) {
            contextMap.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(MDC::remove);
        }
    }
    
    /**
     * Utility method to log all current MDC contents (for debugging)
     */
    public static void logMdcContents(String context) {
        Map<String, String> mdcMap = MDC.getCopyOfContextMap();
        if (mdcMap != null && !mdcMap.isEmpty()) {
            logger.info("MDC Contents [{}]: {}", context, mdcMap);
        } else {
            logger.info("MDC is empty [{}]", context);
        }
    }
    
    // Example usage
    public static void main(String[] args) {
        // Sample large stats object
        Map<String, Object> statsObject = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            statsObject.put("metric_" + i, Math.random() * 100);
            statsObject.put("counter_" + i, i);
            statsObject.put("status_" + i, i % 2 == 0 ? "active" : "inactive");
        }
        
        // Keys we want to extract and log
        List<String> keysToLog = Arrays.asList(
            "metric_10", "metric_25", "metric_50", 
            "counter_100", "counter_200", 
            "status_5", "status_15", "nonexistent_key"
        );
        
        System.out.println("=== Method 1: Direct extraction to MDC ===");
        extractStatsToMdc(statsObject, keysToLog, "stats");
        logMdcContents("after extraction");
        
        // Clear MDC for next example
        MDC.clear();
        
        System.out.println("\n=== Method 2: Extract and return map ===");
        Map<String, Object> extracted = extractAndStoreinMdc(statsObject, keysToLog, "perf");
        System.out.println("Returned map size: " + extracted.size());
        logMdcContents("after extract and store");
        
        // Clear MDC
        MDC.clear();
        
        System.out.println("\n=== Method 3: Stream-based extraction ===");
        extractStatsWithStream(statsObject, keysToLog, "stream");
        logMdcContents("after stream extraction");
        
        // Clear MDC
        MDC.clear();
        
        System.out.println("\n=== Method 4: Advanced extraction ===");
        extractStatsAdvanced(statsObject, keysToLog, "advanced", false, 50);
        logMdcContents("after advanced extraction");
        
        // Clean up
        MDC.clear();
    }
}