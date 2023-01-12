package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord
class FraudDetectorService {
    private val kafkaDispatcher = KafkaDispatcher<Order>()

    fun parse(record: ConsumerRecord<String, Order>) {
        println("-------------------------------")
        println("Processing new order, checking for fraud")
        println("Key: ${record.key()}")
        println("Value: ${record.value()}")
        println("Partition: ${record.partition()}")
        println("Offset: ${record.offset()}")

        val order = record.value()
        Thread.sleep(5000)

        if (order.isFraud()) {
            kafkaDispatcher.send("ecommerce.order.rejected", order.email, order)
            println("Order is a fraud: $order")
        } else {
            kafkaDispatcher.send("ecommerce.order.approved", order.email, order)
            println("Approved: $order")
        }
    }
}

fun main() {
    val fraudDetectorService = FraudDetectorService()
    val consumer = KafkaService(
        FraudDetectorService::class.java.simpleName,
        "ecommerce.new.order",
        fraudDetectorService::parse,
        Order::class.java,
        emptyMap()
    )
    consumer.use { consumer.run() }
}
