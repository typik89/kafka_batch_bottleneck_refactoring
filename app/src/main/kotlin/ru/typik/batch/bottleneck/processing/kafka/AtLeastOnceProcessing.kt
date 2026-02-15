package ru.typik.batch.bottleneck.processing.kafka

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import ru.typik.batch.bottleneck.processing.kafka.header.WaitHeaders.EXPIRATION_TIME
import ru.typik.batch.bottleneck.processing.kafka.header.WaitHeaders.EXPIRATION_UUID
import ru.typik.batch.bottleneck.processing.kafka.utils.sendDeadLetter
import ru.typik.batch.bottleneck.processing.kafka.utils.sendWithOriginalHeaders
import ru.typik.batch.bottleneck.processing.processor.RecordProcessor
import ru.typik.batch.bottleneck.processing.properties.KafkaConfigurationProperties
import ru.typik.batch.bottleneck.processing.properties.createReceiver
import ru.typik.batch.bottleneck.processing.properties.createSender
import java.time.LocalDateTime
import java.util.*


@Profile("atLeastOnce")
@Service
class AtLeastOnceProcessing(
    val kafkaConfigurationProperties: KafkaConfigurationProperties,
    val recordProcessor: RecordProcessor
) {

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
    }

    private lateinit var kafkaReceiver: KafkaReceiver<String, String>
    private lateinit var sender: KafkaSender<String, String>
    private lateinit var kafkaTask: Disposable


    @PostConstruct
    fun init() {
        kafkaReceiver = kafkaConfigurationProperties.createReceiver()
        sender = kafkaConfigurationProperties.createSender()

        kafkaTask = kafkaReceiver.receive(1)
            .doOnNext { log.info("Received offset: ${it.offset()}") }
            .flatMapSequential(
                { record ->
                    val recordWaitWrapper = RecordWaitWrapper(
                        record,
                        UUID.randomUUID().toString(),
                        LocalDateTime.now().plusHours(1)
                    )
                    sendWaitRecord(recordWaitWrapper)
                        .doOnSuccess { record.receiverOffset().acknowledge() }
                        .thenReturn(recordWaitWrapper)
                },
                kafkaConfigurationProperties.consumer.maxPollRecords,
                kafkaConfigurationProperties.consumer.maxPollRecords
            )
            .flatMap(
                { recordWaitWrapper ->
                    val record = recordWaitWrapper.record
                    Mono.fromCallable { log.info("processing ${record.offset()}") }
                        .then(recordProcessor.invoke(record))
                        .doOnTerminate {
                            log.info("processed ${record.offset()}")
                        }
                        .onErrorResume { ex ->
                            log.error("Error in processing ${record.offset()}", ex)
                            sendDeadLetter(record, ex)
                        }
                    //.then(Mono.defer { sendTombstoneWaitRecord(recordWaitWrapper) })

                },
                kafkaConfigurationProperties.consumer.maxPollRecords
            )
            .subscribe()

    }

    @PreDestroy
    fun destroy() {
        kafkaTask.dispose()
    }

    private fun sendWaitRecord(recordWaitWrapper: RecordWaitWrapper): Mono<Void> =
        sender.sendWithOriginalHeaders(
            kafkaConfigurationProperties.waitTopic,
            recordWaitWrapper.record,
            recordWaitWrapper.expirationUUID,
            recordWaitWrapper.record.value(),
            recordWaitWrapper.recordHeaders
        )

    private fun sendTombstoneWaitRecord(recordWaitWrapper: RecordWaitWrapper): Mono<Void> =
        sender.sendWithOriginalHeaders(
            kafkaConfigurationProperties.waitTopic,
            recordWaitWrapper.record,
            recordWaitWrapper.expirationUUID,
            null,
            recordWaitWrapper.recordHeaders
        )

    private fun sendDeadLetter(record: ConsumerRecord<String, String>, ex: Throwable): Mono<Void> =
        sender.sendDeadLetter(kafkaConfigurationProperties.deadletterTopic, record, ex)

    private data class RecordWaitWrapper(
        val record: ReceiverRecord<String, String>,
        val expirationUUID: String,
        val expirationTime: LocalDateTime
    ) {
        val recordHeaders: Headers =
            RecordHeaders()
                .add(EXPIRATION_UUID.headerName(), expirationUUID.toByteArray())
                .add(EXPIRATION_TIME.headerName(), expirationTime.toString().toByteArray())
    }


}