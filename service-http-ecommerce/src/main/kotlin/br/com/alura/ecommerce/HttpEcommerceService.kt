package br.com.alura.ecommerce

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

fun main() {
    val server = Server(8080)

    val context = ServletContextHandler()
    context.contextPath = "/"
    context.addServlet(ServletHolder(NewOrderServlet()), "/new")
    server.handler = context

    server.start()
    server.join()
}