package ru.typik.batch.bottleneck.processing.properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.context.properties.ConfigurationProperties
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import java.util.*

@ConfigurationProperties("kafka")
data class KafkaConfigurationProperties(
    val bootstrapServers: String,
    val consumer: ConsumerProperties,
    val deadletterTopic: String = "deadLetterTopic",
    val waitTopic: String = "waitTopic"
) {

    data class ConsumerProperties(
        val topic: String,
        val groupId: String,
        val maxPollRecords: Int
    )
}

fun KafkaConfigurationProperties.consumerConfig(enabledAutoCommit: Boolean) =
    mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to consumer.groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to consumer.maxPollRecords,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to "100",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to enabledAutoCommit.toString(),
    )


fun KafkaConfigurationProperties.createSender(isTransactional: Boolean): KafkaSender<String, String> =
    KafkaSender.create(
        SenderOptions.create(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            ) + if (isTransactional) mapOf(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG to UUID.randomUUID().toString()
            ) else emptyMap()
        )
    )
