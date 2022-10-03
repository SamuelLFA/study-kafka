package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.Closeable
import java.time.Duration
import java.util.*
import kotlin.reflect.KFunction1

class KafkaService (
    private val groupId: String,
    private val topic: String,
    private val parse: KFunction1<ConsumerRecord<String, String>, Unit>
) : Closeable {
    private val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(properties())

    fun run() {
        consumer.subscribe(listOf(topic))
        while(true) {
            val records = consumer.poll(Duration.ofMillis(100))
            if (!records.isEmpty) {
                println("Found ${records.count()}")
                records.forEach{record ->
                    parse(record)
                }
            }
        }
    }

    private fun properties(maxPollRecordsConfig: String = "1"): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsConfig)
        return properties
    }

    override fun close() {
        consumer.close()
    }
}
