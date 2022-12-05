package br.com.alura.ecommerce

import jakarta.servlet.http.HttpServlet
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import java.math.BigDecimal
import java.util.*

class NewOrderServlet(
    private val orderDispatcher: KafkaDispatcher<Order> = KafkaDispatcher(),
    private val emailDispatcher: KafkaDispatcher<Email> = KafkaDispatcher(),
) : HttpServlet() {
    override fun destroy() {
        super.destroy()
        orderDispatcher.close()
        emailDispatcher.close()
    }

    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val email = req.getParameter("email")
        val amount = BigDecimal(req.getParameter("amount"))

        val orderId = UUID.randomUUID().toString()

        val order = Order(orderId, amount, email)
        orderDispatcher.send("ecommerce.new.order", email, order)

        val emailCode = Email("","Thank you for your order! We are processing your order!")
        emailDispatcher.send("ecommerce.send.email", email, emailCode)

        println("New order sent successfully")
        resp.writer.println("New order sent successfully")
        resp.status = HttpServletResponse.SC_OK
    }
}
