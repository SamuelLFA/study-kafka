package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.Collections.emptyMap

class EmailService {
    fun parse(record: ConsumerRecord<String, Email>) {
        println("-------------------------------")
        println("Sending email")
        println("Key: ${record.key()}")
        println("Value: ${record.value()}")
        println("Partition: ${record.partition()}")
        println("Offset: ${record.offset()}")
        Thread.sleep(1000)
        println("Email sent")
    }
}

fun main() {
    val emailService = EmailService()
    val kafkaService = KafkaService(
        EmailService::class.java.simpleName,
        "ecommerce.send.email",
        emailService::parse,
        Email::class.java,
        emptyMap(),
    )
    kafkaService.use { kafkaService.run() }
}
