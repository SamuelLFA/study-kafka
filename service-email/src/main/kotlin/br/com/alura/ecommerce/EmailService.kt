package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.Collections.emptyMap

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

class EmailService {
    fun parse(record: ConsumerRecord<String, Email>) {
        println("-------------------------------")
        println("Sending email")
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())
        Thread.sleep(1000)
        println("Email sent")
    }
}
