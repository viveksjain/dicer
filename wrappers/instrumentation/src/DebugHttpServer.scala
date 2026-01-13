package com.databricks.instrumentation

import io.opencensus.contrib.zpages.ZPageHandlers
import io.opencensus.contrib.grpc.metrics.RpcViews

import com.databricks.common.util.Lock
import com.databricks.logging.ConsoleLogging
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import scalatags.Text.all._
import scalatags.Text.TypedTag

/**
 * A global, in-process singleton HTTP server for ZPages-style debugging endpoints. This is
 * implemented as a singleton to align with the internal codebase, allowing callers to register
 * custom debugging pages in a same way as the internal codebase does.
 *
 * Callers can call `start()` to start the server with a given port, if no port is provided,
 * a random free port will be used. It's recommended to have the server running for the lifetime of
 * the process, so that the debugging dashboard is always available. `stop()` can be used to stop
 * the server when the process is shutting down.
 *
 * Use `getPort()` to get the port the server port. The debugging dashboard is available at
 * `http://localhost:<port>`. and contains links to all ZPages endpoints. Custom debugging pages
 * can be registered using [[DebugStringServletRegistry]].
 */
object DebugHttpServer extends ConsoleLogging {

  override def loggerName: String = "DebugHttpServer"

  /** The lock guarding the state of the server. */
  private val lock = new ReentrantLock()

  /** The state of the server. */
  private sealed trait ServerState
  private case object Uninitialized extends ServerState
  private case class Started(server: HttpServer) extends ServerState
  private case object Stopped extends ServerState

  /** The current state of the server. */
  @GuardedBy("lock")
  private var state: ServerState = Uninitialized

  /**
   * Starts the local debugging HTTP server on the given port, and waits until it's fully started.
   *
   * @param port The port to bind to, or 0 to use a random free port.
   */
  @throws[IllegalStateException]("if the server has been stopped or already started")
  def start(port: Int = 0): Unit = Lock.withLock(lock) {
    state match {
      case Stopped =>
        throw new IllegalStateException("Cannot restart a stopped server")

      case Started(_) =>
        throw new IllegalStateException("Cannot start a server that has already been started")

      case Uninitialized =>
        // Initialize OpenCensus for RPC metrics collection
        RpcViews.registerAllGrpcViews()

        // Create HTTP server with backlog=0, which uses the system default for the maximum
        // number of queued incoming connections.
        val httpServer = HttpServer.create(new InetSocketAddress(port), 0)
        // Use the default executor (direct execution in I/O threads).
        httpServer.setExecutor(null)
        httpServer.start()
        val serverPort = httpServer.getAddress.getPort

        logger.info(s"Started local debugging HTTP server on http://localhost:$serverPort")

        // Start the custom debugging page handler.
        httpServer
          .createContext(DebugStringServletRegistry.DEBUG_URL_PATH, DebugStringServletRegistry)

        // Start ZPages debugging server and register handlers on the same server.
        ZPageHandlers.registerAllToHttpServer(httpServer)

        // Register our custom root handler for the dashboard.
        httpServer.createContext("/", new DebuggingRootPageHandler("Debug Dashboard"))

        state = Started(httpServer)
    }
  }

  /** Returns the port the server is bound to if it is running, or None if not. */
  def getPort: Option[Int] = Lock.withLock(lock) {
    state match {
      case Started(httpServer) => Some(httpServer.getAddress.getPort)
      case _ => None
    }
  }

  /**
   * Stops the local debugging HTTP server. If the server is not running, an IllegalStateException
   * is thrown.
   *
   * @param delaySeconds The maximum time in seconds to wait until server exchanges have completed.
   */
  @throws[IllegalStateException]("if the server has not been started")
  def stop(delaySeconds: Int): Unit = Lock.withLock(lock) {
    state match {
      case Started(httpServer) =>
        val delay: Int = if (delaySeconds <= 0) 0 else delaySeconds
        logger.info(s"Stopping local debugging HTTP server in $delay seconds")
        httpServer.stop(delay)
        state = Stopped
      case Uninitialized =>
        throw new IllegalStateException("Cannot stop a server that has not been initialized")
      case Stopped =>
        throw new IllegalStateException("Cannot stop a server that has already been stopped")
    }
  }
}

/** Handles the root page for the local debugging HTTP server. */
private class DebuggingRootPageHandler(dashboardTitle: String) extends HttpHandler {

  /** Handles the HTTP request. */
  def handle(exchange: HttpExchange): Unit = {
    try {
      val dashboardHtml = generateDashboardHtml()

      exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
      exchange.sendResponseHeaders(200, dashboardHtml.getBytes.length)

      val os: OutputStream = exchange.getResponseBody
      os.write(dashboardHtml.getBytes)
      os.close()
    } catch {
      case e: Exception =>
        val errorHtml =
          s"<html><body><h1>Error generating dashboard</h1><p>${e.getMessage}</p></body></html>"
        exchange.sendResponseHeaders(500, errorHtml.getBytes.length)
        val os: OutputStream = exchange.getResponseBody
        os.write(errorHtml.getBytes)
        os.close()
    }
  }

  /** Generates the HTML for the root page. */
  private def generateDashboardHtml(): String = {
    val titleTag: TypedTag[String] = tag("title")(dashboardTitle)
    val styleTag: TypedTag[String] = tag("style")(
      """|body { font-family: Arial, sans-serif; margin: 40px; }
         |h1 { color: #333; }
         |.links { margin: 30px 0; }
         |.link {
         |  display: block;
         |  margin: 15px 0;
         |  padding: 10px;
         |  background: #f0f0f0;
         |  border: 1px solid #ccc;
         |  text-decoration: none;
         |  color: #333;
         |}
         |.link:hover { background: #e0e0e0; }
         |.time { margin-top: 30px; font-size: 18px; }
         |""".stripMargin
    )

    val pageContent = html(
      head(
        titleTag,
        styleTag,
        meta(charset := "UTF-8")
      ),
      body(
        h1("Dicer Debugging Dashboard"),
        div(cls := "links")(
          a(href := DebugStringServletRegistry.DEBUG_URL_PATH, cls := "link")("Admin Debug"),
          a(href := "/tracez", cls := "link")("TraceZ - View traces"),
          a(href := "/statsz", cls := "link")("StatsZ - View statistics"),
          a(href := "/rpcz", cls := "link")("RpcZ - View RPC stats")
        )
      )
    )

    "<!DOCTYPE html>\n" + pageContent.render
  }

}
