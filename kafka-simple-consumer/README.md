# Kafka Simple Consumer with Avro Deserializer

This is a Spring Boot application that consumes Avro-encoded messages from a Kafka topic, using Confluent Schema Registry for schema management.

## Features
- Kafka consumer with concurrency of 3
- Reads one record at a time
- Avro deserialization using Schema Registry
- Manual acknowledgment
- Error handling and logging

## Prerequisites
- Java 17+
- Kafka broker running (default: `localhost:9092`)
- Confluent Schema Registry running (default: `http://localhost:8081`)
- Avro schema registered for your topic

## Configuration
Edit `src/main/resources/application.yml` to set your Kafka and Schema Registry endpoints, topic name, and other properties as needed.

## Running the Application
```
cd kafka-simple-consumer
./mvnw spring-boot:run
```

## Customization
- Change the topic name in `@KafkaListener` in `KafkaAvroConsumer.java`.
- Replace `Object` with your generated Avro class for type safety.

## Error Handling
Errors during message processing are logged. You can extend the error handling logic in `KafkaAvroConsumer` as needed (e.g., dead-letter topic). 