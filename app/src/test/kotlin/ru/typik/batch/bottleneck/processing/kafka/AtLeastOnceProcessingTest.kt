package ru.typik.batch.bottleneck.processing.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import reactor.kotlin.core.publisher.toMono
import ru.typik.batch.bottleneck.processing.kafka.header.OriginalValueHeaders
import ru.typik.batch.bottleneck.processing.kafka.header.OriginalValueHeaders.*
import ru.typik.batch.bottleneck.processing.kafka.header.WaitHeaders
import ru.typik.batch.bottleneck.processing.kafka.header.WaitHeaders.EXPIRATION_TIME
import ru.typik.batch.bottleneck.processing.kafka.header.WaitHeaders.EXPIRATION_UUID
import ru.typik.batch.bottleneck.processing.processor.RecordProcessor
import ru.typik.batch.bottleneck.processing.properties.KafkaConfigurationProperties
import ru.typik.batch.bottleneck.processing.properties.createSender
import java.time.Duration
import java.util.*


@Testcontainers
class AtLeastOnceProcessingTest {


    companion object {
        const val PROCESS_TOPIC = "testTopic"
        const val WAIT_TOPIC = "waitTopic"
        const val DLQ_TOPIC = "deadletterTopic"

        @Container
        val kafka: KafkaContainer =
            KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))

    }

    private lateinit var kafkaProperties: KafkaConfigurationProperties
    private lateinit var recordProcessor: RecordProcessor
    private lateinit var atLeastOnceProcessing: AtLeastOnceProcessing
    private lateinit var kafkaReceiver: KafkaReceiver<String, String>
    private lateinit var kafkaSender: KafkaSender<String, String>


    @BeforeEach
    fun init() {
        kafkaProperties = KafkaConfigurationProperties(
            bootstrapServers = kafka.bootstrapServers,
            waitTopic = WAIT_TOPIC,
            deadletterTopic = DLQ_TOPIC,
            consumer = KafkaConfigurationProperties.ConsumerProperties(
                PROCESS_TOPIC,
                "testGroup",
                10
            )
        )

        recordProcessor = mock {
            on { invoke(any()) } doAnswer { invocation ->
                val record = invocation.arguments[0] as ReceiverRecord<*, *>
                if (record.key().toString().contains("error")) {
                    RuntimeException("Test error").toMono()
                } else {
                    Mono.empty()
                }
            }
        }

        atLeastOnceProcessing = AtLeastOnceProcessing(kafkaProperties, recordProcessor)
        atLeastOnceProcessing.init()


        kafkaSender = kafkaProperties.createSender(isTransactional = false)
    }

    @Test
    fun success() {
        val key = "success${System.currentTimeMillis()}"
        val value = "value${UUID.randomUUID()}"

        val sendResult = kafkaSender.sendBlocking(PROCESS_TOPIC, key, value)!!

        val records = await().atMost(Duration.ofSeconds(10))
            .until({
                readTopic(kafkaProperties.bootstrapServers, WAIT_TOPIC, key)
            }) { it.size == 2 }

        assertWaitRecord(key, value, sendResult.recordMetadata(), records[0])
        assertTombstoneWaitRecord(records[0], records[1])
    }

    @Test
    fun error() {
        val key = "error${System.currentTimeMillis()}"
        val value = "value${UUID.randomUUID()}"

        val recordMetadata = kafkaSender.sendBlocking(PROCESS_TOPIC, key, value).recordMetadata()

        val records = await().atMost(Duration.ofSeconds(1000))
            .until({
                readTopic(kafkaProperties.bootstrapServers, WAIT_TOPIC, key)
            }) { it.size == 2 }

        assertWaitRecord(key, value, recordMetadata, records[0])
        assertTombstoneWaitRecord(records[0], records[1])

        assertEquals(1, readTopic(kafkaProperties.bootstrapServers, DLQ_TOPIC, key).size)
    }

    fun assertWaitRecord(
        originalKey: String,
        originalValue: String,
        recordMetadata: RecordMetadata,
        waitRecord: ConsumerRecord<String, String?>
    ) {
        assertNotNull(waitRecord.key())
        assertEquals(originalValue, waitRecord.value())

        assertEquals(originalKey, waitRecord.getHeader(KEY))
        assertEquals(recordMetadata.topic(), waitRecord.getHeader(TOPIC))
        assertEquals(recordMetadata.offset().toString(), waitRecord.getHeader(OFFSET))
        assertEquals(recordMetadata.partition().toString(), waitRecord.getHeader(PARTITION))
        assertEquals(recordMetadata.timestamp().toString(), waitRecord.getHeader(TIMESTAMP))
        assertEquals("{}", waitRecord.getHeader(HEADERS))

        assertEquals(waitRecord.key(), waitRecord.getHeader(EXPIRATION_UUID))
        assertNotNull(waitRecord.getHeader(EXPIRATION_TIME))
    }

    fun assertTombstoneWaitRecord(
        record: ConsumerRecord<String, String?>,
        tombstone: ConsumerRecord<String, String?>
    ) {
        assertEquals(record.key(), tombstone.key())
        assertNull(tombstone.value())

        assertEquals(record.getHeader(KEY), tombstone.getHeader(KEY))
        assertEquals(record.getHeader(TOPIC), tombstone.getHeader(TOPIC))
        assertEquals(record.getHeader(OFFSET), tombstone.getHeader(OFFSET))
        assertEquals(record.getHeader(PARTITION), tombstone.getHeader(PARTITION))
        assertEquals(record.getHeader(TIMESTAMP), tombstone.getHeader(TIMESTAMP))
        assertEquals(record.getHeader(HEADERS), tombstone.getHeader(HEADERS))

        assertEquals(record.getHeader(EXPIRATION_UUID), tombstone.getHeader(EXPIRATION_UUID))
        assertEquals(record.getHeader(EXPIRATION_TIME), tombstone.getHeader(EXPIRATION_TIME))
    }


    private fun ConsumerRecord<String, String?>.getHeader(headerName: String): String? =
        headers().lastHeader(headerName)?.value()?.let { String(it) }

    private fun ConsumerRecord<String, String?>.getHeader(originalValueHeaders: OriginalValueHeaders): String? =
        getHeader(originalValueHeaders.headerName())

    private fun ConsumerRecord<String, String?>.getHeader(waitHeaders: WaitHeaders): String? =
        getHeader(waitHeaders.headerName())


    private fun KafkaSender<String, String>.sendBlocking(
        topic: String,
        key: String,
        value: String
    ) = send(SenderRecord.create(ProducerRecord(topic, key, value), null).toMono())
        .blockLast()


}
