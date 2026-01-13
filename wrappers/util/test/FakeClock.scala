package com.databricks.threading

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.ThreadSafe

import scala.concurrent.duration.FiniteDuration

/** Provides a controllable clock that can be advanced manually in tests. */
@ThreadSafe
class FakeClock {

  /** Start time for the fake clock (wall clock time). */
  private val startInstant: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  /** Current nano time offset from start (for ticker time). */
  private val nanoTimeOffset = new AtomicLong(0L)

  /** Advances the clock by the given duration. */
  def advance(duration: FiniteDuration): Unit = {
    nanoTimeOffset.addAndGet(duration.toNanos)
  }

  /**
   * Advances the clock to the specified duration from the start. Internally calls advance() with
   * the appropriate delta.
   */
  @throws[IllegalArgumentException]("if duration is negative")
  def advanceTo(duration: FiniteDuration): Unit = {
    val targetOffset = duration.toNanos
    require(targetOffset >= 0, "duration must be non-negative")
    val currentOffset = nanoTimeOffset.get()
    if (targetOffset > currentOffset) {
      val deltaNanos = targetOffset - currentOffset
      advance(scala.concurrent.duration.Duration.fromNanos(deltaNanos))
    }
    // If targetOffset == currentOffset, do nothing.
  }

  /** Gets the current wall-clock time as an Instant. */
  def currentTimeInstant(): Instant = {
    startInstant.plusNanos(nanoTimeOffset.get())
  }

  /** Gets the current wall-clock time in milliseconds since epoch. */
  def currentTimeMillis(): Long = {
    currentTimeInstant().toEpochMilli
  }

  /** Gets the current nano time (for ticker measurements). */
  def nanoTime(): Long = {
    nanoTimeOffset.get()
  }
}
