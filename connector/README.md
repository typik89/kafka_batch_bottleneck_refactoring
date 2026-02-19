## Build the Image

Run the `docker` Gradle task to create the image with Kafka Connect Tasks:

   ```bash
   ./gradlew :docker
   ```

## Rest Api

https://docs.confluent.io/platform/current/connect/references/restapi.html

#### Get connectors

```
curl -s http://localhost:8083/connectors
```

#### Get connector state

```
curl -s http://localhost:8083/connectors/expired.source/status
```

#### Logging

```
curl -s -X PUT -H "Content-Type: application/json" \
http://localhost:8083/admin/loggers/io.confluent.connect.jdbc \
-d '{"level": "TRACE"}'
```



