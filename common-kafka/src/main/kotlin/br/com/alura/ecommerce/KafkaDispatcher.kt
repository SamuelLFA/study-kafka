package br.com.alura.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.lang.Exception
import java.util.*

class KafkaDispatcher<T : Any> : Closeable {
    private val producer = KafkaProducer<String, T>(properties())

    fun send(topic: String, key: String, value: T) {
        val record = ProducerRecord(topic, key, value)
        val callback: (metadata: RecordMetadata, exception: Exception?) -> Unit = { metadata, ex ->
            run {
                if (ex != null) {
                    ex.printStackTrace()
                    return@run
                }
                println("${metadata.topic()} ::: ${metadata.partition()} / ${metadata.offset()} / ${metadata.timestamp()}")
            }
        }
        producer.send(record, callback).get()
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = GsonSerializer::class.java.name
        return properties
    }

    override fun close() {
        producer.close()
    }
}
