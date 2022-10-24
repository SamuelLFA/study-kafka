package br.com.alura.ecommerce

import java.math.BigDecimal
import java.util.UUID

fun main() {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        KafkaDispatcher<Email>().use { emailDispatcher ->
            for (i in 1..100) {
                val userId = UUID.randomUUID().toString()
                val orderId = UUID.randomUUID().toString()
                val amount = BigDecimal(Math.random() * 5000 + 1)
                val order = Order(userId, orderId, amount)
                orderDispatcher.send("ecommerce.new.order", userId, order)
                val email = Email("","Thank you for your order! We are processing your order!")
                emailDispatcher.send("ecommerce.send.email", userId, email)
            }
        }
    }
}
