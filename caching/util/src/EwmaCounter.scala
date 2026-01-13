package com.databricks.caching.util

import java.time.{Duration, Instant}
import javax.annotation.concurrent.NotThreadSafe

import com.databricks.caching.util.EwmaCounter.{EwmaAccumulator, Value}

/**
 * Exponentially weighted moving average (EWMA) counter.
 *
 * A counter whose incremented values decay exponentially over time. The counter is defined over a
 * time window (see [[Value]]), and the value of the counter at any point in time is the weighted
 * sum of all inputs. The weight of each input decays exponentially as new inputs accumulate. To
 * simplify reasoning, the counter advances in one second increments: every second, the aggregate
 * rate over the previous second is added to an underlying [[EwmaAccumulator]]. This simplification
 * allows us to use a straightforward time-weighted average, but also means that the counter does
 * not reflect measurements from the latest one-second window.
 *
 * See <internal link> for more details on this abstraction.
 *
 * Not thread-safe.
 *
 * @param startTime The low end of the time window (inclusive) tracked for this counter.
 * @param halfLifeSeconds The half-life of the counter, in seconds. The weight of an input will be
 *                        halved after this many seconds.
 * @param seedOpt When defined, the counter uses this seed value as the historical value for the
 *                counter. Otherwise, the counter assumes that there is no history.
 */
@NotThreadSafe
class EwmaCounter(startTime: Instant, halfLifeSeconds: Int, seedOpt: Option[Double] = None) {
  require(halfLifeSeconds > 0, "halfLifeSeconds must be positive")

  /**
   * Accumulator incorporating all rate measurements passed to [[incrementBy]] prior to
   * [[startTime]] + [[currentSecondLow]]. (All subsequent measurements are staged in
   * [[currentSecondCount]].)
   */
  private[this] val accumulator = {
    // Compute decay such that the weight of an input at time t is half that of an input at time t +
    // halfLifeSeconds. See derivation of $\alpha$ in <internal link>.
    val alpha: Double = 1.0 - Math.pow(2, -1.0 / halfLifeSeconds)
    new EwmaAccumulator(alpha, seedOpt)
  }

  /**
   * The second for which a rate value is currently accumulating. For example, if [[startTime]] is
   * Epoch + 10.2 seconds and [[currentSecondLow]] is 3, [[currentSecondCount]] includes the sum of
   * all values passed to [[incrementBy]] between Epoch + 13.2 seconds (inclusive) and Epoch + 14.2
   * seconds (exclusive).
   */
  private[this] var currentSecondLow: Int = 0

  /** Total count so far in the 1-second window starting at [[startTime]] + [[currentSecondLow]]. */
  private[this] var currentSecondCount: Long = 0

  /**
   * Increments the rate by `value` as of `now`. It's ok if `now` is less than some previously
   * reported timestamp because of a non-monotonic clock: the accumulator uses the highest seen
   * timestamp seen so far internally.
   *
   * Q: Why not use the system ticker rather than the system clock, since the ticker is more likely
   * to be monotonic?
   *
   * A: Measurements are being shared across machines, whose system tickers will not track. For
   * example, Dicer's Assigner uses counter measurements to track load, and needs to relate load
   * reported by multiple machines.
   *
   * Q: Won't non-monotonic and unsteady clocks result in incorrect measurements, with illustory
   * spikes and dips?
   *
   * A: Yes! This is one of the motivations for using a low-pass filtering algorithm like EWMA.
   */
  def incrementBy(now: Instant, value: Int): Unit = {
    advanceTo(now)
    currentSecondCount += value
  }

  /**
   * Returns the value of the counter as of `now` rounded down to the nearest second since
   * [[startTime]]. See remarks in [[incrementBy()]] about non-monotonic timestamps (tl;dr: they're
   * allowed).
   */
  def value(now: Instant): Value = {
    advanceTo(now)
    Value(startTime, startTime.plusSeconds(currentSecondLow), accumulator.value)
  }

  /**
   * Internal helper that advances [[currentSecondLow]] to `now` and accumulates measurements staged
   * in [[currentSecondCount]].
   */
  private def advanceTo(now: Instant): Unit = {
    val second: Int = Duration.between(startTime, now).getSeconds.toInt
    if (second > currentSecondLow) {
      // Append value for the current second.
      accumulator.appendValue(currentSecondCount)
      currentSecondCount = 0
      currentSecondLow += 1

      // Append zero values to catch up to the current second. Even when there are no new inputs,
      // previously recorded values must continue to decay.
      if (currentSecondLow < second) {
        accumulator.appendZeroes(second - currentSecondLow)
        currentSecondLow = second
      }
    }
  }
}
object EwmaCounter {
  case class Value(windowLowInclusive: Instant, windowHighExclusive: Instant, weightedValue: Double)

  /**
   * Implementation of discrete EWMA or simple exponential smoothing, where the weight of each input
   * decays exponentially as new inputs accumulate. This variation of EWMA is described in detail at
   * <internal link>.
   *
   * @param alpha The weight coefficient for each input.
   *
   * Not thread-safe.
   */
  @NotThreadSafe
  private[util] class EwmaAccumulator(alpha: Double, seedOpt: Option[Double]) {
    require(alpha > 0.0)
    require(alpha < 1.0)

    /** The coefficient applied to the "history" on each input. */
    private val decay: Double = 1.0 - alpha

    /**
     * The latest value for $s_t$ as described at <internal link>. This is the
     * numerator for the weighted average. When a seed is provided, the weighted sum is initialized
     * to that seed value, and its weight is initialized to 1.0 (see [[totalWeight]]). Otherwise,
     * both the weighted sum and the total weight are initialized to zero.
     */
    private[this] var weightedSum: Double = seedOpt match {
      case Some(seed: Double) => seed
      case None => 0.0
    }

    /**
     * The latest value for $w_t$ as described at <internal link>. This is the
     * denominator for the weighted average. See remarks on [[weightedSum]] about initialization.
     */
    private[this] var totalWeight: Double = seedOpt match {
      case Some(_) => 1.0
      case None => 0.0
    }

    /** Accumulates an input to the running weighted-average. */
    def appendValue(value: Long): Unit = {
      // See <internal link> for derivations.
      weightedSum = alpha * value + decay * weightedSum
      // When totalWeight is 1.0 (expected when a seed is provided, or when the accumulator has
      // converged because of limited fp precision), there's no need to update it:
      //    alpha + decay * 1.0 = alpha + (1 - alpha) = 1.0
      if (totalWeight != 1.0) {
        // In theory, `totalWeight` should asymptotically approach 1.0 from below, but in practice,
        // accumulating fp errors may cause an overshoot. We use `.min(1.0)` to force convergence
        // and prevent instability in that case.
        totalWeight = (alpha + decay * totalWeight).min(1.0)
      }
    }

    /**
     * REQUIRES: `n` is positive.
     *
     * Appends `n` zeroes, equivalent to calling `appendValue(0)` `n` times.
     */
    def appendZeroes(n: Int): Unit = {
      require(n > 0, "n must be positive")
      // Figure out how much the current values will decay over n steps.
      val decayNTimes: Double = Math.pow(decay, n)
      weightedSum = decayNTimes * weightedSum
      // See remarks in `appendValue` about updating `totalWeight` (or not updating it when it has
      // already converged to `totalWeight == 1.0`).
      if (totalWeight != 1.0) {
        // Update `totalWeight` using a closed-form formula matching the result of repeatedly
        // (`n` times) setting `totalWeight = alpha + decay * totalWeight`.
        // If we represent the value of `totalWeight` after `n` iterations as `T(n)`, we have:
        //   T(0) = w (w is shorthand for the original value of `totalWeight`)
        //   T(n) = decay * T(n - 1) + alpha
        // This is similar to the example discussed in some class notes at
        // https://courses.engr.illinois.edu/cs173/fa2011/lectures/recursive-definition.pdf. Its
        // closed form solution is:
        //   T(n) = decay^n * w + alpha * sum_{i = 0}^{n - 1} (decay ^ i)
        //        = decay^n * w + alpha * (1 - decay^n) / (1 - decay)
        //        = decay^n * w + alpha * (1 - decay^n) / alpha
        //        = decay^n * w + 1 - decay^n
        //        = decay^n * (w - 1) + 1
        // Notes:
        //  - `decayNTimes` is `decay^n` in the above formula.
        //  - See remarks in `appendValue` about using `.min(1.0)` to prevent instability.
        totalWeight = (decayNTimes * (totalWeight - 1) + 1).min(1.0)
      }
    }

    /** The latest value for $m_t$. */
    def value: Double = if (totalWeight == 0) 0 else weightedSum / totalWeight
  }
}
