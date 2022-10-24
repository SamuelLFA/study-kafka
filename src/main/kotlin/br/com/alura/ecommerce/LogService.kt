package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.regex.Pattern

fun main() {
    val logService = LogService()
    val consumer = KafkaService(
        LogService::class.java.simpleName,
        Pattern.compile("ecommerce.*"),
        logService::parse,
        String::class.java,
        mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name)
    )
    consumer.run()
}

class LogService {
    fun parse(record: ConsumerRecord<String, String>) {
        println("-------------------------------")
        println("LOG")
        println(record.key())
        println(record.value())
        println(record.partition())
        println(record.offset())
    }
}
