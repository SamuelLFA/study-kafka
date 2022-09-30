package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

fun main() {
    val kafkaService = KafkaService("ecommerce.send.email", ::parse)
    kafkaService.run()
}

fun parse(record: ConsumerRecord<String, String>) {
    println("-------------------------------")
    println("Sending email")
    println(record.key())
    println(record.value())
    println(record.partition())
    println(record.offset())
    Thread.sleep(1000)
    println("Email sent")
}
