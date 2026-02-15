package ru.typik.batch.bottleneck.processing.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import ru.typik.batch.bottleneck.processing.kafka.header.OriginalValueHeaders
import java.time.Duration


fun readTopic(bootstrapServers: String, topic: String, originalKey: String) =
    readTopic(bootstrapServers, topic)
        .filter { record ->
            record.headers().lastHeader(OriginalValueHeaders.KEY.headerName())
                .value().let { String(it) } == originalKey
        }

fun readTopic(bootstrapServers: String, topic: String): List<ConsumerRecord<String, String?>> {
    val consumer = KafkaConsumer<String, String?>(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        )
    )
    val partitions =
        consumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
    val highWaterMarks = consumer.endOffsets(partitions)
        .filter { it.value > 0 }
        .toMutableMap()

    consumer.assign(partitions)
    consumer.seekToBeginning(partitions)

    val result = mutableListOf<ConsumerRecord<String, String?>>()
    while (true) {
        if (highWaterMarks.isEmpty())
            return result

        consumer.poll(Duration.ofSeconds(1))
            .forEach { record ->
                result.add(record)
                val partition = TopicPartition(record.topic(), record.partition())
                if (highWaterMarks[partition] != null && highWaterMarks[partition]!! - 1 <= record.offset())
                    highWaterMarks.remove(partition)
            }
    }

}