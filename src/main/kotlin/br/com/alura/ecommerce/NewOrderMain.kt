package br.com.alura.ecommerce

import java.util.UUID

fun main() {
    KafkaDispatcher().use { dispatcher ->
        for (i in 1..100) {
            val key = UUID.randomUUID().toString()
            val value = "132123,6753132,2314123912"
            dispatcher.send("ecommerce.new.order", key, value)
            val email = "Thank you for your order! We are processing your order!"
            dispatcher.send("ecommerce.send.email", key, email)
        }
    }
}
