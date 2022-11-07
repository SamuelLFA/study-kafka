package br.com.alura.ecommerce

import java.math.BigDecimal

data class Order (
    val userId: String,
    val orderId: String,
    val amount: BigDecimal,
) {
    fun isFraud() = this.amount >= BigDecimal("4500")
}

