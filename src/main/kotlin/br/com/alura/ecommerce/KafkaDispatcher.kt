package br.com.alura.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.lang.Exception
import java.util.*

class KafkaDispatcher : Closeable {
    private val producer = KafkaProducer<String, String>(properties())

    fun send(topic: String, key: String, value: String) {
        val record = ProducerRecord(topic, key, value)
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
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        return properties
    }

    override fun close() {
        producer.close()
    }
}
