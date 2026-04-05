package ru.typik.batch.bottleneck.processing.kafka

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.sender.KafkaSender
import ru.typik.batch.bottleneck.processing.kafka.utils.sendDeadLetter
import ru.typik.batch.bottleneck.processing.processor.RecordProcessor
import ru.typik.batch.bottleneck.processing.properties.KafkaConfigurationProperties
import ru.typik.batch.bottleneck.processing.properties.consumerConfig
import ru.typik.batch.bottleneck.processing.properties.createSender

@Profile("batch")
@Service
class BatchProcessing(
    val kafkaConfigurationProperties: KafkaConfigurationProperties,
    val recordProcessor: RecordProcessor
) {

    private val logger = LoggerFactory.getLogger(BatchProcessing::class.java)
    private var kafkaTask: Disposable? = null
    private lateinit var kafkaReceiver: KafkaReceiver<String, String>
    private lateinit var sender: KafkaSender<String, String>

    @PostConstruct
    fun init() {
        kafkaReceiver = KafkaReceiver.create(
            ReceiverOptions.create<String?, String?>(
                kafkaConfigurationProperties.consumerConfig(enabledAutoCommit = false)
            )
                .subscription(listOf(kafkaConfigurationProperties.consumer.topic))
        )
        sender = kafkaConfigurationProperties.createSender(isTransactional = true)

        kafkaTask = kafkaReceiver.receiveExactlyOnce(sender.transactionManager())
            .concatMap<Void> { batch ->
                batch
                    .doOnNext { record -> logger.info("record : ${record.offset()} : ${record.key()} : ${record.value()}") }
                    .flatMapSequential { record ->
                        Mono.fromCallable { logger.info("processing ${record.offset()}") }
                            .then(recordProcessor.invoke(record))
                            .doFinally { logger.info("processed ${record.offset()}") }
                            .onErrorResume { ex -> sendDeadLetter(record, ex) }
                    }
                    .then(sender.transactionManager().commit())

            }
            .subscribe()
    }

    private fun sendDeadLetter(record: ConsumerRecord<String, String>, ex: Throwable): Mono<Void> =
        sender.sendDeadLetter(kafkaConfigurationProperties.deadletterTopic, record, ex)


    @PreDestroy
    fun destroy() {
        kafkaTask?.dispose()
    }
}