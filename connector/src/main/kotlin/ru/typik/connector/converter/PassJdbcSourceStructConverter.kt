package ru.typik.connector.converter


import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.Converter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*


class PassJdbcSourceStructConverter : Converter {
    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
    }

    override fun fromConnectData(topic: String?, schema: Schema?, value: Any?): ByteArray =
        TODO()

    override fun toConnectData(topic: String?, value: ByteArray): SchemaAndValue =
        (value as Struct).let { SchemaAndValue(it.schema(), it) }.also {
            LOG.info("Schema and value: $it")
        }


    private fun String.parseDate() = Date.from(
        LocalDateTime.parse(this).atZone(ZoneId.of("UTC"))
            .toInstant()
    )

    private fun Headers.lastHeaderValue(header: String): String? =
        lastHeader(header)?.value()?.let { String((it)) }
}
