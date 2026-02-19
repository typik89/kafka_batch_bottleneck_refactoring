### Prerequisites

Before starting the application, you must spin up the required infrastructure (Kafka, etc.) using
the compose subproject:

```   bash
   ./gradlew :compose:batchComposeUp
```

### Running the Application

The application supports two processing modes defined by Spring profiles.

```
   # At Least Once Processing
   ./gradlew bootRun --args='--spring.profiles.active=atLeastOnce'
   # Batch Processing
   ./gradlew bootRun --args='--spring.profiles.active=batch'
```

### Test Data Generation

Use KafkaTestDataGenerator.kt script to send messages to the Kafka Topic.

You can control the data volume using the `count` parameter.

