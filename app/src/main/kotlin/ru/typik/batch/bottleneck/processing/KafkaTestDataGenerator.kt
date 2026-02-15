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
import java.util.*
import java.util.concurrent.ThreadLocalRandom

fun main(args: Array<String>) {
    val objectMapper = ObjectMapper().findAndRegisterModules()
    val count = 1
    val sender = createSender()

    val records = Flux.range(1, count)
        .map { counter ->
            val operationId = UUID.randomUUID().toString()
            createSenderRecord(
                operationId,
                objectMapper.writeValueAsString(
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

    try {
        sender.send(records)
            .doOnError { error -> println("Error sending message: ${error.message}") }
            .doOnComplete { println("Successfully sent $count messages") }
            .blockLast()
    } finally {
        sender.close()
    }
}

private fun createSenderRecord(key: String, value: String) =
    SenderRecord.create("testTopic", null, null, key, value, null)

private fun createSender() =
    KafkaSender.create(
        SenderOptions.create<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
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

