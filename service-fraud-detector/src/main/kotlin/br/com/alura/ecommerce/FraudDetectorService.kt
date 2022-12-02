package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord

fun main() {
    val fraudDetectorService = FraudDetectorService()
    val consumer = KafkaService(
        FraudDetectorService::class.java.simpleName,
        "ecommerce.new.order",
        fraudDetectorService::parse,
        Order::class.java,
        emptyMap(),
    )
    consumer.use { consumer.run() }
}

class FraudDetectorService {
    private val orderDispatcher: KafkaDispatcher<Order> = KafkaDispatcher()
    fun parse(record: ConsumerRecord<String, Order>) {
        println("-------------------------------")
        println("Processing new order, checking for fraud")
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())
        Thread.sleep(5000)
        val order = record.value()
        if (order.isFraud()) {
            orderDispatcher.send("ecommerce.order.rejected", order.email, order)
            println("Order is a fraud: $order")
        } else {
            orderDispatcher.send("ecommerce.order.approved", order.email, order)
            println("Approved: $order")
        }
    }
}
