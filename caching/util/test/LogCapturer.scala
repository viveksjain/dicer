package com.databricks.caching.util

import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.{GuardedBy, ThreadSafe}
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.{LogEvent, Logger, LoggerContext}
import com.databricks.caching.util.LogCapturer.CapturedLogEvent
import com.databricks.caching.util.Lock.withLock

/** Companion object to provide a factory method for creating a LogCapturer. */
object LogCapturer {

  /** Simple immutable copy of LogEvent data for testing */
  case class CapturedLogEvent(loggerName: String, formattedMessage: String, level: Level)

  /**
   * Capture log events matching `captureRegex` in a scoped block.
   *
   * @param captureRegex the regex to match log messages against. Only log events that match this
   *                     regex will be captured.
   * @param loggerPackageOpt the optional package name of the logger to capture logs from. If not
   *                         provided, defaults to the root logger.
   *
   * Example usage:
   *
   * LogCapturer.withCapturer(
   *   new Regex("Some log message"),
   *   Some("com.databricks.logging.structured.RequestActivityLog")
   * ) { capturer => {
   *   // Note that we perform the operation under test within the LogCapturer.withCapturer block
   *   // so that the capturer is created *before* executing the log-generating operation.
   *   performOperationWhichGeneratesSomeLogMessage()
   *   capturer.waitForEvent()
   * }
   */
  def withCapturer(captureRegex: Regex, loggerPackageOpt: Option[String] = None)(
      thunk: LogCapturer => Unit): Unit = {
    val loggerPackage: String = loggerPackageOpt.getOrElse("")
    val capturer = new LogCapturer(captureRegex, loggerPackage)
    try {
      thunk(capturer)
    } finally {
      capturer.detach()
    }
  }
}

/**
 * An abstraction to capture log output and later allow it to be examined. The capturer starts and
 * attaches itself to the logger associated with the loggerPackage when it is created. Call
 * [[detach()]] to stop capturing logs.
 */
@ThreadSafe
class LogCapturer private (captureRegex: Regex, loggerPackage: String) {
  // Trigger PrefixLogger initialization.
  // This may override the logger associated with `loggerPackage`, so we need it to happen before
  // attaching our custom appender.
  PrefixLogger.create(getClass, "")

  /** The appender that keeps track of the events. */
  private val logAppender = new LogAppender

  /** The lock to protect the state in this abstraction. */
  private val lock = new ReentrantLock

  /** Tracks the log events that have been logged and match `captureRegex`. */
  @GuardedBy("lock")
  private val events = new ArrayBuffer[CapturedLogEvent]

  // Start the appender BEFORE adding it to the logger. The appender will throw an exception
  // if a log event is appended to it before it is started.
  logAppender.start()
  LogManager.getLogger(loggerPackage).asInstanceOf[Logger].addAppender(logAppender)
  LogManager.getContext(false).asInstanceOf[LoggerContext].updateLoggers()

  /** Returns a (shallow) copy of all captured log events. */
  def getCapturedEvents: Vector[CapturedLogEvent] = withLock(lock) {
    events.clone().toVector
  }

  /** Waits until some log event matching [[captureRegex]] is captured. */
  def waitForEvent(): Unit = {
    AssertionWaiter(s"log event matching $captureRegex").await {
      withLock(lock) {
        assert(events.nonEmpty)
      }
    }
  }

  /** Detaches the log appender from the logger. */
  private def detach(): Unit = {
    LogManager.getLogger(loggerPackage).asInstanceOf[Logger].removeAppender(logAppender)
    LogManager.getContext(false).asInstanceOf[LoggerContext].updateLoggers()

    // DO NOT stop the appender. Even after the appender has been removed from the logger, it
    // may still be in use. The appender will throw an exception if a log event is appended to it
    // after it has been stopped.
  }

  /** An appender that keeps track of the log events matching a regex. */
  private class LogAppender
      extends AbstractAppender("LogCapture.LogAppender", null, null, false, Property.EMPTY_ARRAY) {

    /** Given a log event, adds it to `events` if it matches `captureRegex`. */
    override def append(event: LogEvent): Unit = withLock(lock) {
      if (captureRegex.findFirstIn(event.getMessage.getFormattedMessage).isDefined) {
        // Create an immutable copy to avoid issues with LogEvent object reuse
        val capturedEvent = CapturedLogEvent(
          loggerName = Option(event.getLoggerName).getOrElse(""),
          formattedMessage = event.getMessage.getFormattedMessage,
          level = event.getLevel
        )
        events.append(capturedEvent)
      }
    }
  }
}
