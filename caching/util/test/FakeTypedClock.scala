package com.databricks.caching.util

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.ThreadSafe

import scala.concurrent.duration.FiniteDuration

import com.databricks.caching.util.FakeTypedClock.AdvanceCallback
import com.databricks.threading.FakeClock

/**
 * A fake clock for unit tests that does not advance automatically. Instead, it is manually advanced
 * using [[advanceBy()]]. Supports registering callbacks that are synchronously invoked when time
 * advances.
 *
 * Tests relying on the generic Databricks [[FakeClock]] can use the clock returned by
 * [[asFakeDatabricksClock]].
 */
@ThreadSafe
final class FakeTypedClock extends TypedClock {

  /**
   * Callbacks that are invoked when the clock is advanced. Using an atomic reference to support
   * efficient copy-on-write (we don't want low-level concurrency APIs to be contentious).
   */
  private val callbacksRef = new AtomicReference(Seq.empty[AdvanceCallback])

  private val fakeDatabricksClock = new FakeClock {

    /**
     * Override of [[FakeClock.advance()]] that is called by all "advancers" of the fake clock and
     * that is responsible for calling all registered callbacks. Other advance methods are:
     *
     *  * [[FakeTypedClock.advanceBy()]]
     *  * [[FakeClock.advanceTo()]]
     *
     * This is a somewhat brittle approach as other advance overloads could be added to
     * [[FakeClock]] or the implementation of [[FakeClock.advanceTo()]] could be modified so that it
     * no longer calls [[FakeClock.advance()]]. It's unclear how the former case could be handled,
     * but for the latter case we rely on tests to identify the breakage rather than making this
     * fake clock implementation more complicated.
     */
    override def advance(time: FiniteDuration): Unit = {
      super.advance(time)
      for (callback <- callbacksRef.get()) {
        callback()
      }
    }
  }

  private val startTime: Instant = fakeDatabricksClock.currentTimeInstant()

  private val startNanoTime: Long = fakeDatabricksClock.nanoTime()

  override def tickerTime(): TickerTime =
    TickerTime.ofNanos(fakeDatabricksClock.nanoTime() - startNanoTime)

  override def instant(): Instant = {
    // We don't just use `fakeDatabricksClock.currentTimeInstant()` here, because the instants it
    // returns are truncated to millisecond precision, but we have test cases that depend on finer
    // grained time deltas than that.
    startTime.plusNanos(fakeDatabricksClock.nanoTime() - startNanoTime)
  }

  /**
   * REQUIRES: `duration` is non-negative.
   *
   * Advances the fake clock by the given duration and synchronously invokes all registered
   * callbacks.
   */
  def advanceBy(duration: FiniteDuration): Unit = fakeDatabricksClock.advance(duration)

  /** Arranges for `callback` to be called whenever time advances. */
  def registerCallback(callback: AdvanceCallback): Unit = {
    callbacksRef.accumulateAndGet(Seq(callback), _ ++ _)
  }

  /**
   * Returns a [[FakeClock]] that is synchronized with this clock: advancing the returned clock
   * using [[FakeClock.advance()]] or [[FakeClock.advanceTo()]] will also advance this clock, and
   * result in synchronous invocation of all registered callbacks.
   *
   * Ordering of callbacks relative to thread wakeups for [[FakeClock.sleep()]] and
   * [[FakeClock.sleepTo()]] is not guaranteed.
   */
  def asFakeDatabricksClock: FakeClock = fakeDatabricksClock
}
object FakeTypedClock {

  /** Callback type for registered callbacks. */
  type AdvanceCallback = () => Unit
}
