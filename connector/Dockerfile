FROM confluentinc/cp-kafka-connect-base:7.8.0

ENV KAFKA_HEAP_OPTS="-XX:+UseContainerSupport"
ENV CONNECT_SECURITY_PROTOCOL="PLAINTEXT"
ENV CONNECT_REST_PORT="8083"
ENV CONNECT_KEY_CONVERTER="org.apache.kafka.connect.storage.StringConverter"
ENV CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.storage.StringConverter"
ENV CONNECT_REST_ADVERTISED_HOST_NAME="localhost"
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
ENV REPLICATION_FACTOR=1

USER root

USER appuser
COPY --chown=appuser /scripts /app/scripts
COPY --chown=appuser /tasks /app/tasks
COPY /plugins /usr/share/java

#RUN rm -rf /usr/share/java/cp-base-new/slf4j-log4j12*.jar

RUN chmod -R 777 /app


CMD ["/app/scripts/init.sh"]

# Polling period  : 5 seconds
# Timeout period  :10 seconds (if the polling does not return within this time, treat as a failed poll)
# Start-up period : 2 minutes (during which failures are not counted as failures)
# Retry period    : 8 minutes (after which container is deemed unhealthy)
# All settings can be overriden at run-time in Docker/Docker Compose.
HEALTHCHECK --start-period=120s --interval=5s --timeout=10s --retries=96 \
  CMD bash /app/scripts/probe.sh