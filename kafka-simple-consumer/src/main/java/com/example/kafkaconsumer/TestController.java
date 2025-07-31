package com.example.kafkaconsumer;

import com.example.kafkaconsumer.avro.UserEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class TestController {

    @Autowired
    private KafkaAvroProducer producer;

    @PostMapping("/send")
    public Map<String, String> sendUserEvent(@RequestBody Map<String, String> request) {
        String userId = request.get("userId");
        String eventType = request.get("eventType");
        
        if (userId == null || eventType == null) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "userId and eventType are required");
            return error;
        }
        
        producer.sendUserEvent(userId, eventType);
        
        Map<String, String> response = new HashMap<>();
        response.put("message", "UserEvent sent successfully");
        response.put("userId", userId);
        response.put("eventType", eventType);
        return response;
    }

    @PostMapping("/send-custom")
    public Map<String, String> sendCustomUserEvent(@RequestBody UserEvent userEvent) {
        producer.sendUserEvent(userEvent);
        
        Map<String, String> response = new HashMap<>();
        response.put("message", "Custom UserEvent sent successfully");
        response.put("userId", userEvent.getUserId());
        response.put("eventType", userEvent.getEventType());
        response.put("timestamp", String.valueOf(userEvent.getTimestamp()));
        return response;
    }

    @GetMapping("/test")
    public Map<String, String> sendTestEvent() {
        producer.sendUserEvent("test-user-" + System.currentTimeMillis(), "TEST_EVENT");
        
        Map<String, String> response = new HashMap<>();
        response.put("message", "Test UserEvent sent successfully");
        return response;
    }
} 