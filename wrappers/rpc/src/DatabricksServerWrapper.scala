package com.databricks.rpc

import com.databricks.common.util.Lock
import java.util.concurrent.{CompletableFuture, CompletionStage, ExecutorService}
import java.util.concurrent.locks.ReentrantLock
import io.grpc.Server

/** Wrapper around an io.grpc.Server that provides compatible functionality with internal code. */
class DatabricksServerWrapper(server: Server, executor: ExecutorService) {

  /** The lock guarding the state of the server, to make starting it thread-safe. */
  private val lock = new ReentrantLock()

  /** Whether the server has been started, guarded by the lock. */
  private var isStarted = false

  /** Starts the server and waits until it is fully started up. */
  def start(): Unit = Lock.withLock(lock) {
    if (isStarted) {
      throw new IllegalStateException("Server already started, must be stopped to start")
    }
    isStarted = true

    server.start()
  }

  /** Stops the server. */
  def stop(): Unit = Lock.withLock(lock) {
    if (isStarted) {
      server.shutdownNow()
      isStarted = false
      executor.shutdownNow()
    }
  }

  /**
   * Stops the server and returns an immediately completed CompletionStage.
   *
   * This method exists to maintain compatibility with internal code where the underlying server
   * supports asynchronous shutdown.
   */
  def stopAsync(): CompletionStage[Void] = Lock.withLock(lock) {
    stop()
    CompletableFuture.completedFuture(null)
  }

  /** Returns the primary HTTPS port that this server is listening to. */
  def activePort(): Int = Lock.withLock(lock) {
    server.getPort
  }
}
