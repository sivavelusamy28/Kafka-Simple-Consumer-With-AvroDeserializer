package com.example.kafkaconsumer.avro;

import java.util.Objects;

public class UserEvent {
    private String userId;
    private String eventType;
    private long timestamp;

    public UserEvent() {}

    public UserEvent(String userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserEvent userEvent = (UserEvent) o;
        return timestamp == userEvent.timestamp &&
                Objects.equals(userId, userEvent.userId) &&
                Objects.equals(eventType, userEvent.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, eventType, timestamp);
    }
} 