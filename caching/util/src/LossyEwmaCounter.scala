package com.databricks.caching.util

import java.time.Instant
import java.util
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.NotThreadSafe

import scala.collection.mutable

import com.databricks.caching.util.AssertMacros.iassert
import com.databricks.caching.util.LossyEwmaCounter.Config

/**
 * A counter that estimates the time-weighted contributions of "hot keys". The "contribution" of a
 * key is the exponentially-weighted moving average (EWMA) of all values recorded for the key
 * divided by the EWMA of all values recorded for all keys.
 *
 * A hot key is one with a contribution greater than [[Config.support]]. For example,
 * if a key accounts for 10% of the total traffic, and `support` is 0.05, then the key is hot.
 *
 * To avoid tracking for an unbounded number of keys, some error is permitted by this "lossy"
 * abstraction. The contribution for a key may be underestimated by as much as [[Config.error]]. To
 * avoid missing (potentially) hot keys, the [[getHotKeys()]] method may have false positives, but
 * the true contribution of any returned keys will never be less than `support - error`.
 *
 * Recent values are given more weight than older values. Every second, the weight assigned to a
 * value decreases by a factor of [[Config.decay]].
 *
 * The motivating use case for this counter is hot key detection in the Dicer auto-sharder (see
 * <internal link>), which aims to identify, measure, and (through sharding assignments) isolate
 * hot keys that account for a significant fraction of the load offered to a service.
 *
 * See <internal link> for a more formal description of the concepts introduced above
 * and for important background on the implementation (particularly scaling of time-weighted sums
 * and performance considerations).
 *
 * @param config configuration parameters for the counter.
 * @tparam K the type of keys that are being counted.
 */
@NotThreadSafe
class LossyEwmaCounter[K](val config: Config) {
  import LossyEwmaCounter.EstimatedSum

  /**
   * The alpha value, which is the weight of all measurements in [[baseEpoch]] (see
   * <internal link> for background on alpha). It is equal to 1-decay. As
   * mentioned in the class docs, any value would be "correct", but scaling all time-weighted sums
   * by thus value means that the internal weighted-sum estimates are also exponentially-weighted
   * moving averages of the rate relative to [[baseEpoch]], which makes it easier to reason about
   * the scale of [[totalSum]].
   */
  private val alpha: Double = 1.0 - config.decay

  /**
   * INVARIANT: includes all keys that have true time-weighted sums greater than
   * [[sumErrorThreshold]]. This means that any entry may only be removed from the collection when
   * its maximum possible time-weighted sum is less than or equal to [[sumErrorThreshold]].
   *
   * Time-weighted estimated sums of values reported to [[incrementBy()]] per key. Samples recorded
   * before a key was last removed from the collection do not contribute to the estimate, but must
   * account for less than [[sumErrorThreshold]], per the invariant described above.
   */
  private val estimatedSums = new util.HashMap[K, EstimatedSum]()

  /**
   * Time-weighted sum of all values reported to [[incrementBy()]]. See [[estimatedSums]] for the
   * definition of the time-weighted sum. When [[baseEpoch]] is the same as [[currentEpoch]] (which
   * is true after [[scan()]] is performed), the total sum is also equal to the exponentially
   * weighted moving average (EWMA) of the rate of increments across all epochs.
   */
  private var totalSum: Double = 0.0

  /**
   * INVARIANT: `currentEpoch >= baseEpoch`
   *
   * The (monotonically increasing) epoch. This is the latest epoch based on timestamps supplied to
   * [[incrementBy()]].
   */
  private var currentEpoch: Long = 0

  /**
   * INVARIANT: `baseEpoch <= currentEpoch`
   *
   * The base, or "zero", epoch relative to which decay is computed. When the base epoch advances,
   * all estimated sums decay by [[Config.decay]] for each elapsed epoch.
   */
  private var baseEpoch: Long = 0

  /**
   * INVARIANT: `amplification == alpha * decay^{baseEpoch - currentEpoch}`
   *
   * Memoized factor by which values reported to [[incrementBy]] are amplified before being added
   * to [[estimatedSums]] and [[totalSum]]. This factor is updated whenever [[baseEpoch]] or
   * [[currentEpoch]] is updated. This is an inverse decay factor that allows the counter
   * implementation to defer rescaling of values after the epoch advances until the next [[scan()]]
   * operation.
   */
  private var amplification: Double = alpha

  /**
   * When the size of [[estimatedSums]] reaches this threshold, a [[scan()]] should be performed.
   * After a scan, a new value is chosen such that the amortized costs of [[incrementBy]] is O(1)
   * and the number of elements in [[estimatedSums]] remains within a constant factor of the optimal
   * number of elements. See remarks in [[scan()]] for more details. Per discussion at
   * <internal link>, we expect 7/error elements in steady state, so we start with
   * a somewhat lower threshold for the first scan. The choice of initial value is somewhat
   * arbitrary and has no effect on the amortized cost.
   */
  private var estimatedSumsSizeScanThreshold: Int = (1 / config.error).ceil.toInt.max(1)

  /**
   * The true time-weighted sum threshold below which an entry may be discarded because it's within
   * the acceptable error bounds for the lossy count algorithm. Used both when deciding whether an
   * entry can be evicted (during a scan) and to determine the maximum error associated with an
   * entry when it is added to [[estimatedSums]] (when an untracked key is encountered in
   * [[incrementBy()]]).
   */
  @SuppressWarnings(Array("EnforceMethodOrder"))
  private def sumErrorThreshold: Double = totalSum * config.error

  /**
   * Increments the counter for `key` by `value` as of `now`. It's ok if `now` is less than some
   * previously reported timestamp because of a non-monotonic clock: the accumulator uses the
   * highest seen timestamp seen so far internally. Negative or zero values are ignored.
   *
   * Q: Why not use the system ticker rather than the system clock, since the ticker is more likely
   * to be monotonic?
   *
   * A: See the reasoning given in [[EwmaCounter.incrementBy]], which also applies here. We are also
   * using the same value of `now` for tracking Slicelet load in both [[EwmaCounter]] and this
   * class, so we use Instant in both places to avoid any skew between them.
   */
  def incrementBy(now: Instant, key: K, value: Int): Unit = {
    if (value <= 0) {
      return // Ignore non-positive values.
    }
    advanceEpoch(now)

    // Increment the total sum while accounting for amplification and protecting against overflow.
    totalSum = {
      val nextTotalSum: Double = totalSum + amplification * value
      if (nextTotalSum.isInfinity) {
        // Base epoch rescaling due to `totalSum` overflow
        // -----------------------------------------------
        // If the total time-weighted sum overflows, we need to rescale all estimates and reset the
        // amplification to `alpha`, which is a side-effect of the `scan()` method. This code path
        // complicates our amortized complexity analysis for `incrementBy()`, but fortunately it is
        // rarely reached in practice:
        //
        // `totalSum` is the exponentially-weighted moving average rate multiplied by
        // `amplification`. Since the maximum value that can be reported to `incrementBy()` is
        // `Int.MaxValue` (~2^31), and one million calls a second is a reasonable upper bound
        // (~2^20), let's assume that `totalSum` is at most `2^51 * amplification`. Assuming that
        // decay is chosen such that the half-life is 2 minutes, then `amplification` will ramp from
        // `alpha` to `Double.MaxValue` over more than `log2(Double.MaxValue / 2^51) * 2 minutes`,
        // which is over 32 hours. (Note this analysis is extremely pessimistic since it assumes
        // that scans are never being performed in `advanceEpoch` or due to the normal
        // `estimatedSumsSizeScanThreshold` enforcement.)
        LossyEwmaCounter.totalSumOverflowCounter.incrementAndGet()
        scan(hotKeysBuilderOpt = None)
        totalSum + amplification * value
      } else {
        nextTotalSum
      }
    }
    // Get or create the estimated sum for the key.
    val estimate: EstimatedSum = estimatedSums
      .computeIfAbsent(
        key,
        _ => {
          // Determine the maximum possible error for the new estimate. If the key has never been
          // seen before, then the actual error is 0, but if the key was previously tracked by the
          // counter, it could only have been removed when its true time-weighted sum was less than
          // `sumErrorThreshold` (unscaled).
          new EstimatedSum(maxError = sumErrorThreshold)
        }
      )
    // Since per-key sums must be <= totalSum, we don't need to check for overflow here.
    estimate.minSum += amplification * value
    if (estimatedSums.size() >= estimatedSumsSizeScanThreshold) {
      // Perform a scan whenever we reach the `estimatedSumsSizeScanThreshold`.
      scan(hotKeysBuilderOpt = None)
    }
  }

  /**
   * Returns estimated contributions for all keys that may have a true contribution of at least
   * `support`. Note that the contribution is defined as the ''fraction'' of the total that is
   * attributed to each key. For example, if a key accounts for 10% of the total EWMA traffic, its
   * contribution is 0.1 regardless of the underlying time-weighted value.
   *
   * Guarantees:
   *  - The estimated contribution may be an underestimate by up to `error` (but is never an
   *    overestimate).
   *  - Any key that has contributed more than `support` will be included in the result.
   *  - Any key that has contributed less than `support - error` will not be included in the result.
   * Thus, the minimum possible value returned for any key is `support - error`.
   */
  def getHotKeys(): Map[K, Double] = {
    val builder: HotKeysBuilder = Map.newBuilder[K, Double]
    scan(Some(builder))
    builder.result()
  }

  /**
   * Alias for the map builder type used to populate the [[getHotKeys]] result during
   * [[scan()]].
   */
  private type HotKeysBuilder = mutable.Builder[(K, Double), Map[K, Double]]

  /**
   * DANGER DANGER: The implementation _must not_ depend on [[amplification]] which may transiently
   * have an incorrect value when this method is called from [[advanceEpoch()]] when the next
   * amplification value would otherwise overflow. After this method is called, [[amplification]]
   * invariants will be restored (see post-conditions).
   *
   * POST-CONDITIONS:
   *  - [[baseEpoch]] is set to [[currentEpoch]], and all weighted sums are rescaled as needed.
   *    This implies that the [[amplification]] is reset to [[alpha]], since the amplification is
   *    equal to `alpha * decay^{baseEpoch - currentEpoch}`
   *  - Keys whose maximum possible time-weighted sums are less than or equal to
   *    [[sumErrorThreshold]] are removed from the counter. Reiterating the fundamental invariant
   *    for the counter, this is safe to do because, if the key is subsequently added back to the
   *    collection, the maximum error in its estimated time-weighted sum will be at least as large
   *    as its maximum possible time-weighted sum (unscaled) at the time of its removal.
   *
   * Scans all keys in [[estimatedSums]] and opportunistically does bookkeeping (see post-
   * conditions). Optionally populates hot keys discovered during the scan.
   */
  private def scan(hotKeysBuilderOpt: Option[HotKeysBuilder]): Unit = {
    // During the scan, we rescale all estimates such that the `baseEpoch` catches up to
    // `currentEpoch`. Determine the total decay to apply. If we are advancing `baseEpoch`, we must
    // also reset `amplification` to `alpha` since there is no "inverse" decay when `baseEpoch` and
    // `currentEpoch` are equal.
    val decayNTimes: Double = if (currentEpoch > baseEpoch) {
      val elapsedEpochs: Long = currentEpoch - baseEpoch
      baseEpoch = currentEpoch
      amplification = alpha
      math.pow(config.decay, elapsedEpochs)
    } else {
      // No adjustment is required if `currentEpoch == baseEpoch`.
      iassert(
        currentEpoch == baseEpoch,
        s"Base epoch is not trailing current epoch? " +
        s"currentEpoch=$currentEpoch, baseEpoch=$baseEpoch"
      )
      1.0 // decay ^ 0
    }
    // Decay the total sum, which is the denominator for the key contribution estimates.
    totalSum *= decayNTimes

    // Iterate over all estimated sums. Rescale them and discard them if their maximum possible
    // time-weighted sum is less than or equal to the current error threshold. This is safe to do
    // because if the key were to be added back to the counter later, the (unscaled) error threshold
    // at that time will be at least as large as the current error threshold.
    val it = estimatedSums.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val key: K = entry.getKey
      val estimate: EstimatedSum = entry.getValue

      // Apply decay to the estimated sum and maximum error so that they are consistent with the
      // current `baseEpoch`.
      estimate.minSum *= decayNTimes
      estimate.maxError *= decayNTimes
      if (hotKeysBuilderOpt.isDefined && (estimate.maxSum / totalSum) > config.support) {
        // If the maximum possible contribution for the current key is above the configured support,
        // include it in the hot keys result. Note that while we filter based on the maximum
        // possible contribution, we report the minimum possible contribution, since our contract
        // allows only underestimating, not overestimating.
        hotKeysBuilderOpt.get += ((key, estimate.minSum / totalSum))
      } else if (estimate.maxSum <= sumErrorThreshold) {
        // If the key's maximum possible sum is not more than the error threshold, it can be
        // removed.
        it.remove()
      }
    }
    // Reset the size threshold at which a new scan should be triggered. We allow the number of
    // tracked keys to double before the next scan, which ensures that the amortized cost of
    // `incrementBy` is O(1). It will take at least `n` calls before the size doubles (this is the
    // worst case where every call introduces a new key), which means that for each call to
    // `incrementBy`, we need to scan at most 2 entries in the next call to `scan`.
    estimatedSumsSizeScanThreshold = (estimatedSums.size() * 2).max(1)

    // Assert post-conditions.
    iassert(currentEpoch == baseEpoch, s"currentEpoch=$currentEpoch, baseEpoch=$baseEpoch")
    iassert(amplification == alpha, s"amplification=$amplification")
  }

  /**
   * Advances [[currentEpoch]] when `now` is part of a later epoch. When the epoch advances, the
   * [[amplification]] factor is typically increased to account for deferred time-weighted decays
   * that logically occur at the epoch boundary.
   */
  private def advanceEpoch(now: Instant): Unit = {
    // The epoch number is the number of seconds that have elapsed since `startTime`.
    val epoch: Long = (now.toEpochMilli - config.startTime.toEpochMilli) / 1000

    // If the epoch has advanced, we need to adjust the amplification factor (and potentially
    // rescale existing time-weighted sums if the amplification factor is overflowing). If the epoch
    // is the same we do nothing. If the epoch has regressed, it means that `now` is not
    // monotonically increasing, and, per the `incrementBy` spec, we do nothing (to ensure
    // monotonicity of `currentEpoch`).
    val elapsedEpochs: Long = epoch - currentEpoch
    if (elapsedEpochs > 0) {
      // First, try to increase the amplification factor, because doing so does not require us to
      // scan all entries.
      val nextAmplification: Double = if (elapsedEpochs == 1) {
        // Optimize for the common case.
        amplification / config.decay
      } else {
        amplification * math.pow(config.decay, -elapsedEpochs)
      }
      currentEpoch = epoch
      if (nextAmplification.isInfinity) {
        // Base epoch rescaling due to amplification overflow
        // --------------------------------------------------
        // If the amplification factor overflows, we need to rescale all values and reset the
        // amplification to alpha, which is a side-effect  of `scan()`. Note that the `scan` method
        // does not depend on the value of the `amplification` field, so we can safely scan before
        // restoring invariants for that field.
        //
        // We fortunately rarely hit this code path. Fortunate because the amortized complexity
        // analysis for [[incrementBy()]] depends on scans occurring only in response to size
        // thresholds being exceeded, not epoch boundaries. Rare because for a typical decay factor
        // with (say) a half-life of 2 minutes, the amplification factor will only overflow after
        // `log2(Double.MaxValue / alpha) * 2 minutes > log2(Double.MaxValue) * 2 minutes` which is
        // greater than 34 hours. If no scan is triggered for other reasons during that period, it
        // suggests that there are very few keys being added to the collection, and the cost of the
        // scan is likely to be negligible anyway.
        LossyEwmaCounter.amplificationOverflowCounter.incrementAndGet()
        scan(hotKeysBuilderOpt = None)
      } else {
        amplification = nextAmplification
      }
    }
  }

  object forTest {

    /**
     * Number of keys currently tracked by the counter. May be less than the number of distinct keys
     * that have been added to the counter, because keys that contribute less than the configured
     * error are removed during scans.
     */
    def numKeys: Int = estimatedSums.size
  }
}
object LossyEwmaCounter {

  /**
   * REQUIRES: 0 < support < 1
   * REQUIRES: 0 < error < 1
   * REQUIRES: error << support (i.e. error is at least an order of magnitude smaller than support)
   * REQUIRES: halfLifeSeconds > 0
   *
   * Configuration for the counter.
   *
   * @param startTime the time at which epoch 0 starts
   * @param support   the minimum ratio of the total time-weighted contribution that a key must
   *                  account for to be considered a hot key.
   * @param error     the maximum error in the estimated contribution for a key.
   * @param halfLifeSeconds The half-life of the counter, in seconds. The weight of an input will be
   *                        halved after this many seconds.
   */
  case class Config(startTime: Instant, support: Double, error: Double, halfLifeSeconds: Double) {
    require(support > 0 && support < 1, "support must be in (0, 1)")
    require(error > 0 && error < 1, "error must be in (0, 1)")
    require(error <= support / 10, "error must be significantly less than support")
    require(halfLifeSeconds > 0, "half life must be positive")

    // Compute decay such that the weight of an input at time t is half that of an input at time t +
    // halfLifeSeconds. See derivation of $\alpha$ in <internal link>, decay is
    // 1-alpha.
    val decay: Double = Math.pow(2, -1.0 / halfLifeSeconds)
  }

  /**
   * Time-weighted sum estimate for a key.
   *
   * @param maxError the maximum possible error in the estimate of the weighted sum for the current
   *                 key.
   */
  private class EstimatedSum(var maxError: Double) {

    /**
     * Inclusive lower bound on the true time-weighted sum of the current key. May be an
     * underestimate by up to [[maxError]]. Never an overestimate.
     */
    var minSum: Double = 0.0

    /** Inclusive upper bound on the true time-weighted sum of the current key. */
    def maxSum: Double = minSum + maxError
  }

  /**
   * Counts the number of times [[LossyEwmaCounter.amplification]] overflowed, requiring a scan of
   * all entries to advance the base epoch and rescale all estimates.
   */
  private val amplificationOverflowCounter = new AtomicLong(0)

  /**
   * Counts the number of times [[LossyEwmaCounter.totalSum]] overflowed, requiring a scan of all
   * entries to advance the base epoch and rescale all estimates.
   */
  private val totalSumOverflowCounter = new AtomicLong(0)

  private[util] object forTestCompanionObject {

    def getAmplificationOverflowCount: Long = amplificationOverflowCounter.get()

    def getTotalSumOverflowCount: Long = totalSumOverflowCounter.get()
  }
}
