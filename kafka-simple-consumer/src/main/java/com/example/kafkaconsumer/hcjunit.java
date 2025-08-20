package com.example.health;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for HolisticHealthCheck class.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("HolisticHealthCheck Tests")
class HolisticHealthCheckTest {
    
    private static final String TEST_CURRENT_REGION = "us-east-1";
    private static final String TEST_FAILOVER_REGION = "us-west-2";
    private static final String TEST_LISTENER_ID = "test-listener";
    private static final long DEFAULT_TIMEOUT = 1000L;
    
    @Mock
    private ResiliencyProcessor resiliencyProcessor;
    
    @Mock
    private ApplicationProperties applicationProperties;
    
    @Mock
    private KafkaListenerEndpointRegistry kafkaListenerRegistry;
    
    @Mock
    private MessageListenerContainer messageListenerContainer;
    
    @Mock
    private ResiliencyResponse resiliencyResponse;
    
    private HolisticHealthCheck holisticHealthCheck;
    
    @BeforeEach
    void setUp() {
        // Setup application properties
        when(applicationProperties.getCurrentRegion()).thenReturn(TEST_CURRENT_REGION);
        when(applicationProperties.getFailoverRegion()).thenReturn(TEST_FAILOVER_REGION);
        when(applicationProperties.getListenerId()).thenReturn(TEST_LISTENER_ID);
        
        // Setup Kafka registry
        when(kafkaListenerRegistry.getListenerContainer(TEST_LISTENER_ID))
                .thenReturn(messageListenerContainer);
        
        // Create instance
        holisticHealthCheck = new HolisticHealthCheck(
                resiliencyProcessor,
                applicationProperties,
                kafkaListenerRegistry
        );
    }
    
    @Test
    @DisplayName("Should initialize configuration values correctly")
    void testInitialize() {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        
        // When
        holisticHealthCheck.initialize();
        
        // Then
        verify(applicationProperties).getCurrentRegion();
        verify(applicationProperties).getFailoverRegion();
        verify(applicationProperties).isPrimaryRegion();
        verify(applicationProperties).getListenerId();
    }
    
    @Test
    @DisplayName("Should resume consumer in primary region when current region is active")
    void testPrimaryRegionWithActiveCurrentRegion() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Arrays.asList(TEST_CURRENT_REGION, "eu-central-1");
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(true);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).resume();
        verify(messageListenerContainer, never()).pause();
    }
    
    @Test
    @DisplayName("Should pause consumer in primary region when current region is not active")
    void testPrimaryRegionWithInactiveCurrentRegion() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Arrays.asList(TEST_FAILOVER_REGION, "eu-central-1");
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(false);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should pause consumer in secondary region when failover region is active")
    void testSecondaryRegionWithActiveFailoverRegion() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(false);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Arrays.asList(TEST_FAILOVER_REGION, "eu-central-1");
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(false);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should resume consumer in secondary region when failover region is not active")
    void testSecondaryRegionWithInactiveFailoverRegion() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(false);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Arrays.asList(TEST_CURRENT_REGION, "eu-central-1");
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(true);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).resume();
        verify(messageListenerContainer, never()).pause();
    }
    
    @Test
    @DisplayName("Should not pause consumer if already paused")
    void testDoNotPauseIfAlreadyPaused() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Collections.singletonList(TEST_FAILOVER_REGION);
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(true); // Already paused
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should not resume consumer if already running")
    void testDoNotResumeIfAlreadyRunning() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Collections.singletonList(TEST_CURRENT_REGION);
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(false); // Already running
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should handle null resiliency response gracefully")
    void testHandleNullResiliencyResponse() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(null);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should handle null active regions gracefully")
    void testHandleNullActiveRegions() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        when(resiliencyResponse.getActiveRegions()).thenReturn(null);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should handle missing consumer gracefully")
    void testHandleMissingConsumer() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Collections.singletonList(TEST_CURRENT_REGION);
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(kafkaListenerRegistry.getListenerContainer(TEST_LISTENER_ID)).thenReturn(null);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        // Should not throw exception, just log error
        verify(kafkaListenerRegistry).getListenerContainer(TEST_LISTENER_ID);
    }
    
    @Test
    @DisplayName("Should handle exception from resiliency processor")
    void testHandleResiliencyProcessorException() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT))
                .thenThrow(new RuntimeException("Connection timeout"));
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should handle empty active regions list")
    void testHandleEmptyActiveRegions() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Collections.emptyList();
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(false);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).pause();
        verify(messageListenerContainer, never()).resume();
    }
    
    @Test
    @DisplayName("Should handle multiple regions in active list correctly")
    void testMultipleActiveRegions() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Arrays.asList(
                TEST_CURRENT_REGION, 
                TEST_FAILOVER_REGION, 
                "eu-central-1", 
                "ap-southeast-1"
        );
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(true);
        
        // When
        holisticHealthCheck.runResiliencyProbe();
        
        // Then
        verify(messageListenerContainer).resume();
        verify(messageListenerContainer, never()).pause();
    }
    
    @Test
    @DisplayName("Should verify pause and resume operations are idempotent")
    void testIdempotentOperations() throws Exception {
        // Given
        when(applicationProperties.isPrimaryRegion()).thenReturn(true);
        holisticHealthCheck.initialize();
        
        List<String> activeRegions = Collections.singletonList(TEST_CURRENT_REGION);
        when(resiliencyResponse.getActiveRegions()).thenReturn(activeRegions);
        when(resiliencyProcessor.process(DEFAULT_TIMEOUT)).thenReturn(resiliencyResponse);
        when(messageListenerContainer.isPauseRequested()).thenReturn(false); // Already running
        
        // When - call multiple times
        holisticHealthCheck.runResiliencyProbe();
        holisticHealthCheck.runResiliencyProbe();
        holisticHealthCheck.runResiliencyProbe();
        
        // Then - operations should not be called since state hasn't changed
        verify(messageListenerContainer, never()).pause();
        verify(messageListenerContainer, never()).resume();
    }
}