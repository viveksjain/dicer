package com.databricks.dicer.client

import java.time.Instant
import scala.concurrent.ExecutionContext

import com.databricks.dicer.common.{ClientType, Generation}

/**
 * Client-specific proto logger that provides convenience methods for Dicer client logging.
 * Logging is disabled in this implementation.
 */
private[client] trait DicerClientProtoLogger {

  /**
   * Convenience method to log assignment propagation latency.
   *
   * @param generation the assignment generation.
   * @param currentTime the current time when this assignment was received.
   */
  def logAssignmentPropagationLatency(generation: Generation, currentTime: Instant): Unit

  /**
   * Gets the current sampling fraction.
   *
   * @return the current sampling fraction.
   */
  def getKeySampleFraction: Double

  /**
   * Updates the sampling fraction.
   *
   * @param newFraction the new sampling fraction.
   */
  def setKeySampleFraction(newFraction: Double): Unit
}

private[client] object DicerClientProtoLogger {

  /**
   * Creates a new [[DicerClientProtoLogger]] instance. Always returns a no-op logger.
   *
   * @param clientType the type of client (Clerk or Slicelet).
   * @param subscriberDebugName the debug name of the subscriber.
   * @param keySampleFraction the fraction of keys to sample.
   * @param executor the execution context.
   */
  def create(
      clientType: ClientType,
      subscriberDebugName: String,
      keySampleFraction: Double,
      executor: ExecutionContext): DicerClientProtoLogger = {
    NoopDicerClientProtoLogger
  }

  /** No-op implementation that does not perform any logging. */
  private object NoopDicerClientProtoLogger extends DicerClientProtoLogger {
    override def logAssignmentPropagationLatency(
        generation: Generation,
        currentTime: Instant): Unit = {
      // No-op: do nothing
      ()
    }

    /** No-op implementation that always returns 0.0. */
    override def getKeySampleFraction: Double = 0.0

    /** No-op implementation that does nothing. */
    override def setKeySampleFraction(newFraction: Double): Unit = ()
  }
}
