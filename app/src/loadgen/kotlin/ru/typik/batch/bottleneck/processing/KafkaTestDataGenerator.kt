package ru.typik.batch.bottleneck.processing

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

fun main(args: Array<String>) {
    val config = LoadGeneratorConfig.fromArgs(args)
    val objectMapper = ObjectMapper().findAndRegisterModules()
    val sender = createSender(config.bootstrapServers)

    val records = Flux.range(1, config.count)
        .map { counter ->
            val operationId = UUID.randomUUID().toString()
            createSenderRecord(
                topic = config.topic,
                key = operationId,
                value = objectMapper.writeValueAsString(
                    mapOf(
                        "operationId" to operationId,
                        "operationDate" to LocalDateTime.now()
                            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        "cardNumber" to generateCardNumber(),
                        "price" to ThreadLocalRandom.current().nextDouble(50.0, 100.0),
                        "terminalId" to UUID.randomUUID().toString(),
                        "additionalData" to "${counter % 10}"
                    )
                )
            )
        }

    println(
        "Sending ${config.count} messages to topic '${config.topic}' via '${config.bootstrapServers}'"
    )

    try {
        sender.send(records)
            .doOnError { error -> println("Error sending message: ${error.message}") }
            .doOnComplete { println("Successfully sent ${config.count} messages") }
            .blockLast()
    } finally {
        sender.close()
    }
}

private data class LoadGeneratorConfig(
    val bootstrapServers: String,
    val topic: String,
    val count: Int
) {
    companion object {
        fun fromArgs(args: Array<String>): LoadGeneratorConfig {
            val values = args
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .associate { arg ->
                    val parts = arg.split("=", limit = 2)
                    require(parts.size == 2) {
                        "Invalid argument '$arg'. Use key=value format."
                    }
                    parts[0] to parts[1]
                }

            return LoadGeneratorConfig(
                bootstrapServers = values["bootstrapServers"] ?: "localhost:9092",
                topic = values["topic"] ?: "testTopic",
                count = values["count"]?.toIntOrNull()
                    ?: error("Argument 'count' must be an integer if specified.")
            )
        }
    }
}

private fun createSenderRecord(topic: String, key: String, value: String) =
    SenderRecord.create(topic, null, null, key, value, null)

private fun createSender(bootstrapServers: String) =
    KafkaSender.create(
        SenderOptions.create<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all"
            )
        )
    )

private fun generateCardNumber(): String {
    val random = ThreadLocalRandom.current()
    return (1..16).joinToString("") { random.nextInt(0, 10).toString() }
}
