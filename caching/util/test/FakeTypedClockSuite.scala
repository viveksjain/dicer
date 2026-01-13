package com.databricks.caching.util

import java.time.Instant

import scala.util.Random
import scala.collection.mutable
import scala.concurrent.duration._

import com.databricks.testing.DatabricksTest

class FakeTypedClockSuite extends DatabricksTest {

  test("Ticker time starts at 0") {
    // Test plan: verify that the ticker time starts at 0 when the `FakeTypedClock`
    // is created, and it can be correctly retrieved when the clock is advanced.
    for (_ <- 0 until 20) {
      val clock = new FakeTypedClock()
      assert(clock.tickerTime().nanos == 0)
      var randomPositiveDuration: FiniteDuration = Duration.Zero
      // In case `randomPositiveDuration.nanos` is `Long.MinValue`.
      while (randomPositiveDuration <= Duration.Zero) {
        randomPositiveDuration = Duration.fromNanos(Random.nextLong().abs)
      }
      clock.advanceBy(randomPositiveDuration)
      assert(clock.tickerTime().nanos == randomPositiveDuration.toNanos)
    }
    // Test the edge case of advancing `Long.MaxValue` nanoseconds.
    val clock = new FakeTypedClock()
    clock.advanceBy(Long.MaxValue.nanoseconds)
    assert(clock.tickerTime().nanos == Long.MaxValue)
  }

  test("Callbacks") {
    // Test plan: verify that registered callbacks are triggered exactly once and synchronously when
    // a FakeTypedClock is advanced using any of the supported mechanisms (`advanceBy`,
    // `asFakeDatabricksClock.advance`, `asFakeDatabricksClock.advanceTo`). When the callback is
    // made, callbacks should observe the "advanced" time using any of the supported mechanisms
    // (`tickerTime`, `instant`, `asFakeDatabricksClock.currentTimeMillis`,
    // `asFakeDatabricksClock.nanoTime`). Registers two callbacks to verify that both are called.
    val clock = new FakeTypedClock()
    val startNanoTime: Long = clock.asFakeDatabricksClock.nanoTime()

    // Record of the "now" values reported by the fake clock using all accessors.
    case class CallbackLogEntry(
        tickerTime: TickerTime,
        instant: Instant,
        currentTimeMillis: Long,
        nanoTime: Long) {
      def this() =
        this(
          tickerTime = clock.tickerTime(),
          instant = clock.instant(),
          currentTimeMillis = clock.asFakeDatabricksClock.currentTimeMillis(),
          nanoTime = clock.asFakeDatabricksClock.nanoTime() - startNanoTime
        )
    }
    // Register callbacks.
    val log1 = mutable.ArrayBuffer.empty[CallbackLogEntry]
    val log2 = mutable.ArrayBuffer.empty[CallbackLogEntry]
    clock.registerCallback(() => log1 += new CallbackLogEntry())
    clock.registerCallback(() => log2 += new CallbackLogEntry())

    // Advance clock repeatedly and accumulate expected log entries.
    val expectedEntries = mutable.ArrayBuffer.empty[CallbackLogEntry]

    // Define a helper function to create expected log entries given an offset from the start time.
    val startTickerTime: TickerTime = clock.tickerTime()
    val startInstant: Instant = clock.instant()
    def createExpectedLogEntry(offset: FiniteDuration): CallbackLogEntry = {
      CallbackLogEntry(
        tickerTime = startTickerTime + offset,
        instant = startInstant.plusNanos(offset.toNanos),
        currentTimeMillis = startInstant.plusNanos(offset.toNanos).toEpochMilli,
        nanoTime = (startTickerTime + offset).nanos
      )
    }

    // Advance using `advanceBy`.
    clock.advanceBy(1.second)
    expectedEntries += createExpectedLogEntry(offset = 1.second)
    assert(log1 == expectedEntries)
    assert(log2 == expectedEntries)

    // Advance using `asFakeDatabricksClock.advance`.
    clock.asFakeDatabricksClock.advance(1.second)
    expectedEntries += createExpectedLogEntry(offset = 2.seconds)
    assert(log1 == expectedEntries)
    assert(log2 == expectedEntries)

    // Advance using `asFakeDatabricksClock.advanceTo`.
    clock.asFakeDatabricksClock.advanceTo(3.seconds)
    expectedEntries += createExpectedLogEntry(offset = 3.seconds)
    assert(log1 == expectedEntries)
    assert(log2 == expectedEntries)
  }
}
