# Kafka Batch Bottleneck Refactoring

## Processing Modes

- `batch`
- `parallelConsumer`
- `atLeastOnce`

## Run Infrastructure

Full environment with the application container:

```bash
.\gradlew.bat :compose:batchComposeUp
.\gradlew.bat :compose:parallelConsumerComposeUp
.\gradlew.bat :compose:atLeastOnceComposeUp
```

Development environment without the application container:

```bash
.\gradlew.bat :compose:devBatchComposeUp
.\gradlew.bat :compose:devParallelConsumerComposeUp
.\gradlew.bat :compose:devAtLeastOnceComposeUp
```

## Run Application Locally

Start one of the `dev<profile>ComposeUp` environments first, then run the app on the local JVM:

```bash
.\gradlew.bat :app:bootRun --args="--spring.profiles.active=batch"
.\gradlew.bat :app:bootRun --args="--spring.profiles.active=parallelConsumer"
.\gradlew.bat :app:bootRun --args="--spring.profiles.active=atLeastOnce"
```

## Build Docker Images

```bash
.\gradlew.bat :app:bootBuildImage
.\gradlew.bat :connector:docker
```

## Generate Kafka Load

```bash
.\gradlew.bat :app:runLoadGenerator --args="count=500000"
```

