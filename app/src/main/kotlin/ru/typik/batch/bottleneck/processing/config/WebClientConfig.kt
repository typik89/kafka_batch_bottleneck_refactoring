package ru.typik.batch.bottleneck.processing.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClient

@Configuration
class WebClientConfig(
    private val webClientBuilder: WebClient.Builder,
    @Value("\${auth-service.base-url}") val baseUrl: String
) {

    @Bean
    fun webClient(): WebClient =
        webClientBuilder
            .baseUrl(baseUrl)
            .build()
}