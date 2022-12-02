package br.com.alura.ecommerce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.Closeable
import java.time.Duration
import java.util.*
import java.util.regex.Pattern
import kotlin.reflect.KFunction1

class KafkaService<T> : Closeable {
    private var parse: KFunction1<ConsumerRecord<String, T>, Unit>
    private var consumer: KafkaConsumer<String, T>

    constructor(groupId: String, topic: String, parse: KFunction1<ConsumerRecord<String, T>, Unit>, type: Class<T>, properties: Map<String, String>)
            : this(groupId, parse, type, properties) {
        consumer.subscribe(listOf(topic))
    }
    constructor(groupId: String, topic: Pattern, parse: KFunction1<ConsumerRecord<String, T>, Unit>, type: Class<T>, properties: Map<String, String>)
            : this(groupId, parse, type, properties) {
        consumer.subscribe(topic)
    }

    private constructor(groupId: String, parse: KFunction1<ConsumerRecord<String, T>, Unit>, type: Class<T>, properties: Map<String, String>) {
        this.parse = parse
        this.consumer = KafkaConsumer<String, T>(getProperties(groupId, type, properties))
    }

    fun run() {
        while(true) {
            val records = consumer.poll(Duration.ofMillis(100))
            if (!records.isEmpty) {
                records.forEach{record ->
                    parse(record)
                }
            }
        }
    }

    private fun getProperties(groupId: String, type: Class<T>, overrideProperties: Map<String, String>, maxPollRecordsConfig: String = "1"): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString())
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsConfig)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.name)
        properties.putAll(overrideProperties)
        return properties
    }

    override fun close() {
        consumer.close()
    }
}
