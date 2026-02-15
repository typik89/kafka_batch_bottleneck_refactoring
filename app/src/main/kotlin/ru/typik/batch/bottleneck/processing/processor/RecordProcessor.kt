package ru.typik.batch.bottleneck.processing.processor

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.DependsOn
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@DependsOn("webClientConfig")
@Service
class RecordProcessor(
    private val webClient: WebClient
) : (ConsumerRecord<String, String>) -> Mono<Void> {

    private val logger = LoggerFactory.getLogger(RecordProcessor::class.java)

    override fun invoke(record: ConsumerRecord<String, String>): Mono<Void> =
        post(record.value())
            .metricKafkaReceiver(record.topic())

    private fun post(value: String) =
        webClient.post()
            .uri("/authData")
            .bodyValue(value)
            .retrieve()
            .bodyToMono(String::class.java)
            .doOnNext { logger.info("response: $it") }
            .then()

    private fun <T> Mono<T>.metricKafkaReceiver(topicSource: String): Mono<T> =
        this.name("kafka.processed")
            .tag("topic", topicSource)
            .metrics()

}