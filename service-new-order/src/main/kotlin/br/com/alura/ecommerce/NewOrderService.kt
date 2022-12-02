package br.com.alura.ecommerce

import java.math.BigDecimal
import java.util.UUID

fun main() {
    KafkaDispatcher<Order>().use { orderDispatcher ->
        KafkaDispatcher<Email>().use { emailDispatcher ->
            for (i in 1..10) {
                val orderId = UUID.randomUUID().toString()
                val amount = BigDecimal(Math.random() * 5000 + 1)
                val email = "${Math.random()}@gmail.com"

                val order = Order(orderId, amount, email)
                orderDispatcher.send("ecommerce.new.order", email, order)

                val emailCode = Email("","Thank you for your order! We are processing your order!")
                emailDispatcher.send("ecommerce.send.email", email, emailCode)
            }
        }
    }
}
