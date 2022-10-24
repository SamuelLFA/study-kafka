package br.com.alura.ecommerce

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class GsonDeserializer<T> : Deserializer<T> {
    private lateinit var type: Class<T>
    private val gson = GsonBuilder().create()

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val typeName = configs[TYPE_CONFIG]
        val type = Class.forName(typeName as String?)
        this.type = type as Class<T>
    }

    override fun deserialize(topic: String, data: ByteArray): T {
        return gson.fromJson(String(data), type)
    }

    companion object {
        var TYPE_CONFIG: String = "br.com.samuellfa.ecommerce.type_config"
    }
}
