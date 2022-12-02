package br.com.ecommerce.alura

import br.com.alura.ecommerce.KafkaService
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

class CreateUserService {
    private var connection: Connection

    init {
        val url = "jdbc:sqlite:target/users_database.db"
        this.connection = DriverManager.getConnection(url)
        try {
            connection.createStatement().execute("""
                create table Users (
                    uuid varchar(200) primary key,
                    email varchar(200)
                )
            """)
        } catch (ex: SQLException) {
            ex.printStackTrace()
        }

    }

    fun parse(record: ConsumerRecord<String, Order>) {
        println("-------------------------------")
        println("Processing new order, checking for new user")
        println(record.value())
        val order = record.value()
        if (isNewUser(order.email)) {
            insertNewUser(order.email)
        }
    }

    private fun insertNewUser(email: String) {
        val insert = connection.prepareStatement("""
            insert into Users (uuid, email)
            values (?, ?)
        """)
        insert.setString(1, UUID.randomUUID().toString())
        insert.setString(2, email)
        insert.execute()
        println("User created: $email")
    }

    private fun isNewUser(email: String): Boolean {
        val exists = connection.prepareStatement("""
            select uuid from Users
            where email = ? limit 1
        """)
        exists.setString(1, email)
        val results = exists.executeQuery()
        return !results.next()
    }
}

fun main() {
    val createUserService = CreateUserService()
    val consumer = KafkaService(
        CreateUserService::class.java.simpleName,
        "ecommerce.new.order",
        createUserService::parse,
        Order::class.java,
        emptyMap(),
    )
    consumer.use { consumer.run() }
}