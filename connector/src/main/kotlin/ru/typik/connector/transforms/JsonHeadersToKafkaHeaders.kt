package ru.typik.connector.transforms

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.slf4j.Logger
import org.slf4j.LoggerFactory


class JsonHeadersToKafkaHeaders<R : ConnectRecord<R>> : Transformation<R> {

    private val mapper = ObjectMapper().findAndRegisterModules()
    private val mapTypeReference: TypeReference<Map<String, String>> =
        object : TypeReference<Map<String, String>>() {}
    private lateinit var field: String

    override fun configure(configs: Map<String?, *>) {
        this.field = configs[FIELD_CONFIG] as String
    }

    override fun apply(record: R): R = record.apply {
        LOG.info("Record value: ${valueSchema()} ${value()}")
        mapper.readValue((value() as Struct).getString(field), mapTypeReference)
            .forEach { (headerName, headerValue) ->
                record.headers().addString(headerName, headerValue)
            }
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    override fun close() {
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(this::class.java)

        const val FIELD_CONFIG: String = "json.field"

        private val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                FIELD_CONFIG, ConfigDef.Type.STRING, "original_headers",
                ConfigDef.Importance.HIGH, "Field containing JSON headers"
            )
    }
}
