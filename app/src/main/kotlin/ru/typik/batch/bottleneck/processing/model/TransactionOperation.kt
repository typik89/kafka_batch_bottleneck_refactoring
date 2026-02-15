package ru.typik.batch.bottleneck.processing.model

data class TransactionOperation(
    val operationId: String,
    val operationDate: String,
    val cardNumber: String,
    val price: Long,
    val terminalId: String,
    val additionalData: String? = null
)