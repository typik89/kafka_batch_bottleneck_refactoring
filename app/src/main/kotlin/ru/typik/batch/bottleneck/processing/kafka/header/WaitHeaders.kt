package ru.typik.batch.bottleneck.processing.kafka.header

enum class WaitHeaders {
    EXPIRATION_UUID,
    EXPIRATION_TIME;

    fun headerName() = "WAIT_$name"
}