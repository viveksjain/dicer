package com.databricks.caching.util

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer

import io.grpc.Status

import com.databricks.caching.util.Lock.withLock

/**
 * This abstraction implements a [[StreamCallback]] that logs all the values and errors in an
 * internal buffer that a piece of test code can examine. It also provides methods to wait for
 * certain conditions to be true in the log before those methods return. `sec` is the
 * [[SequentialExecutionContext]] that is used for the [[StreamCallback]] - it is not the context
 * on which this abstraction's synchronous methods run.
 */
class LoggingStreamCallback[T](sec: SequentialExecutionContext) extends StreamCallback[T](sec) {
  import LoggingStreamCallback.describe

  private final val logger = PrefixLogger.create(this.getClass, "")

  /** Lock to guard the internal state. */
  private val lock = new ReentrantLock()

  /** The buffer that tracks all values and errors. */
  private val buffer = new ArrayBuffer[StatusOr[T]]

  /** Creates a new [[LoggingStreamCallback]] that shares the same execution context as `that`. */
  def this(that: LoggingStreamCallback[T]) {
    this(that.getExecutionContext)
  }

  override def onSuccess(value: T): Unit = withLock(lock) {
    logger.info(s"onSuccess in LoggingStreamCallback: ${describe(value)} at ${buffer.size}")
    // Just add the value to the buffer.
    buffer.append(StatusOr.success(value))
  }

  override def onFailure(status: Status): Unit = withLock(lock) {
    logger.info(s"onFailure in LoggingStreamCallback: $status at ${buffer.size}")
    // Just add the failure value to the buffer.
    buffer.append(StatusOr.error(status))
  }

  /** Returns the number of elements in the current buffer. */
  def numElements: Int = withLock(lock) { buffer.size }

  /** Returns a copy of the log. */
  def getLog: Vector[StatusOr[T]] = withLock(lock) { buffer.toVector }

  /**
   * Waits until `pred` is true on any value (not error) element that is in the buffer with an index
   * `fromElem` or higher. Callers typically use this method in conjunction with [[numElements]].
   * For example, if the number of elements is 2 (i.e., there are elements 0 and 1), the caller may
   * call `waitForPredicate` with a predicate and 2 stating that the predicate must be satisfied on
   * any element added after these two elements.
   *
   * Note: WaitForPredicate is used in this special case as it waits for exactly one predicate.
   * WaitForPredicate is less error prone and easier to understand than the generalized
   * waitForAssertions method.
   */
  def waitForPredicate(pred: T => Boolean, fromElem: Int): Unit = {
    AssertionWaiter(s"LogStream wait for value satisfying predicate from $fromElem").await {
      withLock(lock) {
        var result = false
        for (i <- fromElem until buffer.length) {
          if (buffer(i).isOk && pred(buffer(i).get)) {
            result = true
          }
        }
        assert(result)
      }
    }
  }

  /**
   * Waits until the log has the `status` error in an element starting at index `fromElem` (it only
   * compares the code and not the rest of the status type). For more details about the element
   * index and how to use this, see the specs of [[waitForPredicate()]].
   */
  def waitForStatus(status: Status, fromElem: Int): Unit = {
    AssertionWaiter(s"LogStream wait for $status from $fromElem").await {
      withLock(lock) {
        var result = false
        for (i <- fromElem until buffer.length) {
          if (!buffer(i).isOk && buffer(i).status.getCode == status.getCode) {
            result = true
          }
        }
        assert(result)
      }
    }
  }

  /** The context used by the [[LoggingStreamCallback]] that this class implements. */
  private def getExecutionContext: SequentialExecutionContext = sec
}
object LoggingStreamCallback {

  /**
   * Describes `value` for logging purposes. Truncates `toString` descriptions longer than 100
   * characters to avoid spamming test logs.
   */
  private def describe[T](value: T): String = {
    val description = value.toString
    if (description.length > 100) {
      s"${description.substring(0, 100)} ..."
    } else {
      description
    }
  }
}
