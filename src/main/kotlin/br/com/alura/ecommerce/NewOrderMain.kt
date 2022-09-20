package br.com.alura.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Exception
import java.util.Properties
import java.util.UUID

fun main() {
    val producer = KafkaProducer<String, String>(properties())
    for (i in 1..100) {
        val key = UUID.randomUUID().toString()
        val value = "132123,6753132,2314123912"
        val record = ProducerRecord("ecommerce.new.order", key, value)
        val email = "Thank you for your order! We are processing your order!"
        val emailRecord = ProducerRecord("ecommerce.send.email", key, email)
        val callback: (metadata: RecordMetadata, exception: Exception?) -> Unit = { data, ex ->
            run {
                if (ex != null) {
                    ex.printStackTrace()
                    return@run
                }
                println("${data.topic()} ::: ${data.partition()} / ${data.offset()} / ${data.timestamp()}")
            }
        }
        producer.send(record, callback).get()
        producer.send(emailRecord, callback).get()
    }

}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return properties
}