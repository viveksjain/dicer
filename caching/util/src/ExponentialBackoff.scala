package com.databricks.caching.util

import scala.concurrent.duration._
import scala.util.Random

/**
 * REQUIRES: 0 < minRetryDelay <= maxRetryDelay < 1 day
 *
 * <p>A class that tracks and returns exponential backoff duration with some jitter on every
 * backoff. Sample usage:
 * {{{
 *   val backoff = new ExponentialBackoff(
 *     new Random,
 *     2.seconds,
 *     1.minute
 *   )
 *   backoff.nextDelay()
 * }}}
 *
 * @param random        The random number generator for some jitter
 * @param minRetryDelay The minimum delay (pre-jitter) to be returned when `nextDelay` is called
 * @param maxRetryDelay The maximum delay (pre-jitter) to be returned when `nextDelay` is called.
 *                      After this value is reached, `nextDelay` returns this same value until
 *                      `reset`.
 */
class ExponentialBackoff(
    random: Random,
    minRetryDelay: FiniteDuration,
    maxRetryDelay: FiniteDuration) {
  require(Duration.Zero < minRetryDelay)
  require(minRetryDelay < maxRetryDelay)
  require(maxRetryDelay < 1.day)

  /**
   * The percent around the generated value that we randomize the delay to avoid lockstep changes to
   * the delay (say) for multiple distributed clients, e.g., a 40% jitter means that the returned
   * value is 20% above or below the generated value.
   */
  private val jitterRatio = 0.4

  /**
   * The current delay (for determining the next delay). 0 means that the delay has been reset.
   * While this could be computed from [[attempts]], this is tracked independently so that each call
   * to [[nextDelay()]] is O(1).
   */
  private var delayWithoutJitter: FiniteDuration = Duration.Zero

  /**
   * Tracks the number of calls to [[nextDelay()]] made since creation of the object, or the last
   * call to [[reset()]].
   */
  private var attempts: Int = 0

  def nextDelay(): FiniteDuration = {
    attempts += 1
    delayWithoutJitter = clamp(delayWithoutJitter * 2)

    // Add/subtract a random delay of jitterRatio relative to the new value. Given a value between
    // 0 and 1, subtract 0.5 so that we split jitter across 0, e.g., if the number is 0 and
    // jitter percent is 40%, the delay is -0.5*0.4, i.e, 20% lower than the computed value.
    val actualJitterRatio: Double = (random.nextDouble() - 0.5) * jitterRatio

    // Now add the jitter relative to the computed value.
    val delay: Duration = (1 + actualJitterRatio) * delayWithoutJitter
    delay match {
      case delay: FiniteDuration => delay
      case _ => throw new AssertionError(s"unexpected over/under flow: $delay")
    }
  }

  def reset(): Unit = {
    attempts = 0
    delayWithoutJitter = Duration.Zero
  }

  /**
   * Returns the number of calls to [[nextDelay()]] made since creation of the object, or the last
   * call to [[reset()]].
   */
  def getAttempts: Int = attempts

  /** Clamps `delta` to a value in `[minRetryDelay, maxRetryDelay]`. */
  private def clamp(delay: FiniteDuration): FiniteDuration =
    if (delay < minRetryDelay) minRetryDelay
    else if (maxRetryDelay < delay) maxRetryDelay
    else delay

  object forTest {

    /** Returns whether this object has some backoff applied. */
    def isInBackoff: Boolean = {
      delayWithoutJitter.toNanos != 0
    }
  }
}
