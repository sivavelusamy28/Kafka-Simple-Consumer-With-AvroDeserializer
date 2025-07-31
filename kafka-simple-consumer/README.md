# Kafka Simple Consumer with Avro Deserializer

This is a Spring Boot application that consumes Avro-encoded messages from a Kafka topic, using Confluent Schema Registry for schema management.

## Features
- Kafka consumer with concurrency of 3
- Reads one record at a time
- Avro deserialization using Schema Registry
- Manual acknowledgment
- Error handling and logging
- Kafka producer for sending Avro messages
- REST API for testing message production
- **Automatic Avro class generation** from schema during compilation
- **Configurable consumer enable/disable** via application.yml

## Prerequisites
- Java 17+
- Maven (for building and generating Avro classes)
- Kafka broker running (default: `localhost:9092`)
- Confluent Schema Registry running (default: `http://localhost:8081`)
- Avro schema registered for your topic

## Building the Application
The Avro classes are automatically generated from the schema during compilation:

```bash
cd kafka-simple-consumer
mvn clean compile
```

This will generate the `UserEvent` class from `src/main/avro/UserEvent.avsc` into `src/main/java/com/example/kafkaconsumer/avro/`.

## Configuration

### Consumer Enable/Disable
The Kafka consumer can be enabled or disabled via configuration:

```yaml
kafka:
  consumer:
    enabled: true  # Set to false to disable the consumer
    topic: your-avro-topic
```

### Running Modes

**Full application (consumer + producer):**
```bash
mvn spring-boot:run
```

**Producer only (consumer disabled):**
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=producer-only
```

**Custom configuration:**
```bash
mvn spring-boot:run -Dkafka.consumer.enabled=false
```

## Running the Application
```bash
mvn spring-boot:run
```

## Testing the Producer
The application includes a REST API for testing message production:

### Send a test event
```bash
curl -X GET http://localhost:8080/api/kafka/test
```

### Send a custom event
```bash
curl -X POST http://localhost:8080/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123", "eventType": "LOGIN"}'
```

### Send a custom event with timestamp
```bash
curl -X POST http://localhost:8080/api/kafka/send-custom \
  -H "Content-Type: application/json" \
  -d '{"userId": "user456", "eventType": "PURCHASE", "timestamp": 1640995200000}'
```

## Customization
- Change the topic name in `application.yml` under `kafka.consumer.topic`
- Modify the Avro schema in `src/main/avro/UserEvent.avsc` and rebuild to regenerate the Java class.
- Add new Avro schemas to `src/main/avro/` directory and rebuild.
- Enable/disable consumer by setting `kafka.consumer.enabled` in `application.yml`

## Error Handling
Errors during message processing are logged. You can extend the error handling logic in `KafkaAvroConsumer` as needed (e.g., dead-letter topic).

## Sample Avro Schema
The application includes a sample `UserEvent` schema with fields:
- `userId` (string)
- `eventType` (string) 
- `timestamp` (long)

The corresponding Java class is automatically generated during compilation. 