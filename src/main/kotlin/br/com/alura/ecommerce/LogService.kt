package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.regex.Pattern

fun main() {
    val logService = LogService()
    val consumer = KafkaService(
        LogService::class.java.simpleName,
        Pattern.compile("ecommerce.*"),
        logService::parse,
        String::class.java
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
