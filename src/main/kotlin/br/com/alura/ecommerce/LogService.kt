package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

fun main() {
    val consumer = KafkaConsumer<String, String>(properties())
    consumer.subscribe(Pattern.compile("ecommerce.*"))
    while(true) {
        val records = consumer.poll(Duration.ofMillis(100))
        if (!records.isEmpty) {
            println("Found ${records.count()}")
            records.forEach{record ->
                println("-------------------------------")
                println("LOG")
                println(record.key())
                println(record.value())
                println(record.partition())
                println(record.offset())
            }
        }
    }
}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "LogService")
    return properties
}