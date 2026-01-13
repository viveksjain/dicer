package com.databricks.common.web

import com.databricks.instrumentation.DebugHttpServer
import com.databricks.logging.ConsoleLogging

import java.util.concurrent.CountDownLatch

/**
 * Open source version of internal InfoService. Starts [[DebugHttpServer]] and also creates a
 * non-daemon thread to keep JVM alive.
 */
object InfoService extends ConsoleLogging {

  override def loggerName: String = getClass.getName

  /**
   * Latch to keep the JVM alive through a non-daemon thread. A shutdown hook decrements this
   * to stop the background thread.
   */
  private val keepAliveLatch = new CountDownLatch(1)

  /** Starts the InfoService on the given port. */
  def start(port: Int): Unit = {
    logger.info(s"InfoService starting on port $port")

    DebugHttpServer.start(port)

    // Register shutdown hook to release the latch on JVM exit
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("InfoService shutting down")
      keepAliveLatch.countDown()
    }, "InfoService-ShutdownHook"))

    // Create a non-daemon thread that keeps the JVM alive
    val thread = new Thread(() => {
      try {
        keepAliveLatch.await()
      } catch {
        case _: InterruptedException =>
          logger.info("InfoService keep-alive thread interrupted")
      }
    }, "InfoService-KeepAlive")
    thread.setDaemon(false)
    thread.start()
  }
}
