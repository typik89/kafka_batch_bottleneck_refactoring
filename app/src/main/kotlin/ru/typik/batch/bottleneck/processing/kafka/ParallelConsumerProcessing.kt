package ru.typik.batch.bottleneck.processing.kafka

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.reactor.ReactorProcessor
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.kafka.sender.KafkaSender
import ru.typik.batch.bottleneck.processing.kafka.utils.sendDeadLetter
import ru.typik.batch.bottleneck.processing.processor.RecordProcessor
import ru.typik.batch.bottleneck.processing.properties.KafkaConfigurationProperties
import ru.typik.batch.bottleneck.processing.properties.consumerConfig
import ru.typik.batch.bottleneck.processing.properties.createSender

@Profile("parallelConsumer")
@Service
class ParallelConsumerProcessing(
    val kafkaConfigurationProperties: KafkaConfigurationProperties,
    val recordProcessor: RecordProcessor,
    val meterRegistry: MeterRegistry
) {

    companion object {
        private val log = LoggerFactory.getLogger(ParallelConsumerProcessing::class.java)
    }

    private lateinit var consumer: Consumer<String, String>
    private lateinit var sender: KafkaSender<String, String>
    private lateinit var parallelConsumer: ReactorProcessor<String, String>
    private lateinit var kafkaClientMetrics: KafkaClientMetrics

    @PostConstruct
    fun init() {
        consumer = KafkaConsumer(kafkaConfigurationProperties.consumerConfig(false))
        sender = kafkaConfigurationProperties.createSender(false)

        parallelConsumer = ReactorProcessor(
            ParallelConsumerOptions.builder<String, String>()
                .consumer(consumer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
                .maxConcurrency(kafkaConfigurationProperties.consumer.maxPollRecords)
                .messageBufferSize(1)
                .initialLoadFactor(1)
                .maximumLoadFactor(1)
                // Metrics
                .meterRegistry(meterRegistry)
                .pcInstanceTag("app-parallel-consumer")
                .build()
        )
        kafkaClientMetrics = KafkaClientMetrics(consumer).apply { bindTo(meterRegistry) }

        parallelConsumer.subscribe(listOf(kafkaConfigurationProperties.consumer.topic))
        parallelConsumer.react { context ->
            val record = context.singleConsumerRecord
            log.info("Processing ${record.offset()}")

            recordProcessor(record)
                .doOnSuccess { log.info("Processed ${record.offset()}") }
                .onErrorResume { ex ->
                    log.error("Error in processing ${record.offset()}", ex)
                    sender.sendDeadLetter(
                        kafkaConfigurationProperties.deadletterTopic, record, ex
                    )
                }
                .thenReturn(record)
        }

        parallelConsumer
    }


    @PreDestroy
    fun destroy() {
        if (::parallelConsumer.isInitialized) {
            parallelConsumer.closeDrainFirst()
        }
        if (::consumer.isInitialized) {
            consumer.close()
        }
        if (::sender.isInitialized) {
            sender.close()
        }
    }
}
