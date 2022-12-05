package br.com.alura.ecommerce

import java.math.BigDecimal

data class Order (
    val orderId: String,
    val amount: BigDecimal,
    val email: String,
)
