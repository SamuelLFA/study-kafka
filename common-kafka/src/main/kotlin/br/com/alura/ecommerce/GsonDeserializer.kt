package br.com.alura.ecommerce

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer

class GsonDeserializer<T : Any> : Deserializer<T> {
    private lateinit var type: Class<T>
    private val gson = GsonBuilder().create()

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        type = Class.forName(configs[TYPE_CONFIG] as String?) as Class<T>
    }

    override fun deserialize(topic: String, data: ByteArray): T {
        return gson.fromJson(String(data), type)
    }

    companion object {
        const val TYPE_CONFIG = "br.com.samuellfa.ecommerce.type_config"
    }
}
