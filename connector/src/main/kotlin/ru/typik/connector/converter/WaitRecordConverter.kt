package ru.typik.connector.converter

import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.*
import org.apache.kafka.connect.storage.Converter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Date

class WaitRecordConverter : Converter {
    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(this::class.java)
    }

    private val schema = SchemaBuilder.struct()
        .apply {
            field("body", Schema.OPTIONAL_STRING_SCHEMA)
            field("original_topic", Schema.OPTIONAL_STRING_SCHEMA)
            field("original_partition", Schema.OPTIONAL_INT32_SCHEMA)
            field("original_offset", Schema.OPTIONAL_INT32_SCHEMA)
            field("original_key", Schema.OPTIONAL_STRING_SCHEMA)
            field("original_timestamp", Timestamp.SCHEMA)
            field("original_headers", Schema.OPTIONAL_STRING_SCHEMA)
            field("wait_expiration_uuid", Schema.OPTIONAL_STRING_SCHEMA)
            field("wait_expiration_time", Timestamp.SCHEMA)
        }
        .build()

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
    }

    override fun fromConnectData(topic: String?, schema: Schema?, value: Any?): ByteArray {
        throw UnsupportedOperationException()
    }

    override fun toConnectData(topic: String?, value: ByteArray?): SchemaAndValue =
        toConnectData(topic, null, value)

    override fun toConnectData(
        topic: String?,
        headers: Headers?,
        value: ByteArray?
    ): SchemaAndValue =
        if (value == null) SchemaAndValue(null, null)
        else
            SchemaAndValue(
                schema,
                Struct(schema)
                    .apply {
                        put("body", value?.let { String(it) })
                        put("original_topic", headers?.lastHeaderValue("ORIGINAL_TOPIC"))
                        put(
                            "original_partition",
                            headers?.lastHeaderValue("ORIGINAL_PARTITION")?.toInt()
                        )
                        put(
                            "original_offset",
                            headers?.lastHeaderValue("ORIGINAL_OFFSET")?.toInt()
                        )
                        put("original_key", headers?.lastHeaderValue("ORIGINAL_KEY"))
                        put("original_headers", headers?.lastHeaderValue("ORIGINAL_HEADERS"))

                        put(
                            "original_timestamp",
                            headers?.lastHeaderValue("ORIGINAL_TIMESTAMP")?.toLong()?.let {
                                Date.from(Instant.ofEpochMilli(it))
                            }
                        )
                        put(
                            "wait_expiration_uuid",
                            headers?.lastHeaderValue("WAIT_EXPIRATION_UUID")
                        )
                        put(
                            "wait_expiration_time",
                            headers?.lastHeaderValue("WAIT_EXPIRATION_TIME")?.parseDate()
                        )
                    }
            )

    private fun String.parseDate() = Date.from(
        LocalDateTime.parse(this).atZone(ZoneId.of("UTC"))
            .toInstant()
    )

    private fun Headers.lastHeaderValue(header: String): String? =
        lastHeader(header)?.value()?.let { String((it)) }
}
