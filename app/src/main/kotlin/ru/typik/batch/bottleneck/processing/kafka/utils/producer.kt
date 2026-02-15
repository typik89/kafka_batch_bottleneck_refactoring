package ru.typik.batch.bottleneck.processing.kafka.utils

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import reactor.kotlin.core.publisher.toMono
import ru.typik.batch.bottleneck.processing.kafka.header.OriginalValueHeaders
import ru.typik.batch.bottleneck.processing.kafka.header.OriginalValueHeaders.*

private fun RecordHeaders.addHeader(originalValueHeaders: OriginalValueHeaders, value: String) =
    apply { add(originalValueHeaders.headerName(), value.toByteArray()) }

private val objectMapper: ObjectMapper = ObjectMapper().findAndRegisterModules()

fun KafkaSender<String, String>.sendWithOriginalHeaders(
    topic: String,
    record: ConsumerRecord<String, String>,
    key: String,
    value: String?,
    additionalHeaders: Headers
): Mono<Void> =
    send(
        SenderRecord.create(
            ProducerRecord(
                topic,
                null,
                null,
                key,
                value,
                RecordHeaders()
                    .addHeader(TOPIC, record.topic())
                    .addHeader(PARTITION, record.partition().toString())
                    .addHeader(OFFSET, record.offset().toString())
                    .addHeader(KEY, record.key())
                    .addHeader(TIMESTAMP, record.timestamp().toString())
                    .addHeader(
                        HEADERS,
                        objectMapper.writeValueAsString(
                            record.headers().associate { header ->
                                header.key() to String(header.value())
                            }
                        )
                    )
                    .apply {
                        additionalHeaders.forEach { header -> add(header) }
                    }
            ), null
        ).toMono()
    ).then()

fun KafkaSender<String, String>.sendDeadLetter(
    deadLetterTopic: String,
    record: ConsumerRecord<String, String>,
    throwable: Throwable
): Mono<Void> = sendWithOriginalHeaders(
    deadLetterTopic,
    record,
    record.key(),
    record.value(),
    RecordHeaders().apply {
        add("Exception", throwable::class.java.toString().toByteArray())
        add("Stacktrace", throwable.stackTraceToString().toByteArray())
    }
)