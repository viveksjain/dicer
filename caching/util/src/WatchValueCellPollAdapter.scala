package com.databricks.caching.util

import javax.annotation.concurrent.ThreadSafe
import io.grpc.Status

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
 * An abstraction for implementing a [[WatchValueCell]] on top of a producer that only supports
 * polling for determining when the underlying value has changed.
 *
 * @param initialValue          The initial value of the transformed type.
 * @param poller                A code function that polls the watched value.
 * @param transform             The code function that transforms the raw value to the parsed one.
 * @param pollInterval          The finite duration of the interval between each value poll.
 * @param blockingWorkExecutor  A named executor to execute blocking work.
 * @param sec                   A sequential execution context to protect the state.
 *
 * @tparam T                The type for the raw value.
 * @tparam R                The type for the parsed value.
 *
 * The producer start to polls the raw value every `pollInterval` using `poller` at startup.
 * Once it gets the value, it applies `transform` to get the parsed value. The periodical poll
 * will be canceled if `cancel()` is called.
 * The consumer can register its callback by calling the `watch()` function.
 */
@ThreadSafe
sealed class WatchValueCellPollAdapter[T, R](
    initialValue: R,
    poller: () => T,
    transform: T => R,
    pollInterval: FiniteDuration,
    blockingWorkExecutor: ExecutionContext,
    sec: SequentialExecutionContext)
    extends WatchValueCell.Consumer[R]
    with Cancellable {

  /** The cell to watch the value. */
  private val cell = new WatchValueCell[R]
  cell.setValue(initialValue)

  /** The runnable poller that polls the value and updates `cell`. */
  private val pollerRunnable: Runnable = () => {
    sec.assertCurrentContext()

    // Since the initial value is set, the latest value must be non-empty.
    val latestValue: R =
      cell.getLatestValueOpt.getOrElse(throw new AssertionError("value must be defined"))

    blockingWorkExecutor.execute(() => {
      val newValueRaw: T = poller()
      val newValue: R = transform(newValueRaw)

      // Only update the value when the value gets changed.
      if (latestValue != newValue) {
        cell.setValue(newValue)
      }
    })
  }

  /** The poller that periodically polls value that starts at startup. */
  private val periodicPoller: Cancellable =
    sec.scheduleRepeating("periodical poller", pollInterval, pollerRunnable)

  override def watch(callback: ValueStreamCallback[R]): Cancellable = {
    cell.watch(callback)
  }

  override def watch(callback: StreamCallback[R]): Cancellable = {
    cell.watch(callback)
  }

  override def cancel(reason: Status = Status.CANCELLED): Unit = {
    periodicPoller.cancel(reason)
  }

  override def getLatestValueOpt: Option[R] = cell.getLatestValueOpt

  override def getStatus: Status = cell.getStatus
}
