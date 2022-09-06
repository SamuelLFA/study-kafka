package br.com.alura.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main() {
    val producer = KafkaProducer<String, String>(properties())
    val value = "132123,6753132,2314123912"
    val record = ProducerRecord("ecommerce.new.order", value, value)
    producer.send(record) { data, ex ->
        run {
            if (ex != null) {
                ex.printStackTrace()
                return@run
            }
            println("${data.topic()} ::: ${data.partition()} / ${data.offset()} / ${data.timestamp()}")
        }
    }.get()
}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return properties
}