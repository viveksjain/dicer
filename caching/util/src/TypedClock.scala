package com.databricks.caching.util

import java.time.Instant

/** Alternative to [[Clock]] to provide typed ticker and wall-clock times.
 *
 * Background: Ticker times and wall-clock times are two different types of time (tickers are only
 * useful for measuring differences in time from measurements in the same process, whereas
 * wall-clock times tell, well, wall-clock time, but typically have lower resolution than tickers do
 * and so are not preferred for measuring time differences). The problem is that [[Clock]] returns
 * the same Long type for both ticker times and wall-clock times, even though these are semantically
 * very different things. [[TypedClock]] instead returns [[TickerTime]] for ticker times, and
 * [[Instant]] for wall-clock times.
 * Additionally, [[Clock]] can choose an arbitrary origin for ticker times, which forces the caller
 * to consider potential issues if the chosen origin is too far away. This could lead to concerns
 * about ticker time overflow or underflow during computations. In contrast, [[TypedClock]]
 * guarantees that the ticker time starts at 0 — meaning its origin is the moment the [[TypedClock]]
 * is created. As a result, it would only overflow after `Long.MaxValue` nanoseconds (~292 years),
 * which is impossible in practice.
 */
trait TypedClock {

  /**
   * Returns current ticker time. Has better precision than [[instant()]], and better accuracy when
   * measuring short time durations in-process, but the returned times are relative to an origin and
   * can't be related to ticker times from other processes.
   *
   * Not monotonic.
   * Its origin is selected to be the time when the [[TypedClock]] is created.
   *
   * See [[System.nanoTime()]].
   */
  def tickerTime(): TickerTime

  /**
   * Returns the current system time. Has only millisecond precision, but unlike [[tickerTime()]],
   * the returned values track system time, which loosely tracks the time on other machines.
   *
   * Not monotonic.
   *
   * See [[System.currentTimeMillis()]].
   */
  def instant(): Instant
}

/** The system clock. */
object RealtimeTypedClock extends TypedClock {

  /** It is used to ensure that the ticker time origin is [[RealtimeTypedClock]]'s creation time. */
  private val startNanoTime: Long = System.nanoTime()

  override def tickerTime(): TickerTime = TickerTime.ofNanos(System.nanoTime() - startNanoTime)
  override def instant(): Instant = Instant.ofEpochMilli(System.currentTimeMillis())
}
