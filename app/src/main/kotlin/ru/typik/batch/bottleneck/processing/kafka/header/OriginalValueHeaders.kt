package ru.typik.batch.bottleneck.processing.kafka.header

enum class OriginalValueHeaders {
    TOPIC, PARTITION, OFFSET, KEY, TIMESTAMP, HEADERS;

    fun headerName() = "ORIGINAL_$name"
}