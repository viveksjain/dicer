package com.databricks.caching.util

import java.time.Instant
import javax.annotation.concurrent.NotThreadSafe

import scala.collection.mutable

//
// This file contains simple versions of the Lossy Count algorithm and contract that are used for
// design docs and as reference implementations for tests. See <internal link> for background.
//

/**
 * A counter that estimates per-key counts in a stream and reports "hot keys" that account for more
 * than some fraction ([[LossyCounter.Config.support]]) of the total count. The estimated counts for
 * keys may be underestimates of the true counts by at most
 * [[LossyCounter.Config.error]] * [[LossyCounter#total]].
 *
 * @tparam K the type of keys that are being counted.
 */
@NotThreadSafe
trait LossyCounter[K] {

  /** The configuration of the counter. */
  val config: LossyCounter.Config

  /** Increments the count for `k` and `total`. */
  def increment(key: K): Unit

  /** Returns the total number of [[increment]] calls. */
  def total: Long

  /**
   * Returns all possible hot keys and their estimated counts, possibly including some false
   * positives. A hot key is a key satisfying the `support` requirement: `count > support * total`.
   * Guarantees:
   *  - No false negatives: all hot keys are returned.
   *  - False positives are possible, but only within `error` for keys with
   *    `count >= (support - error) * total`
   *  - Estimated counts may be less than the true count by at most `error * total`, but are never
   *    overestimates.
   */
  def getHotKeys(): Map[K, Long]
}
object LossyCounter {

  /**
   * REQUIRES: 0 < support < 1
   * REQUIRES: 0 < error < 1
   * REQUIRES: error << support (error is at least an order of magnitude less than support)
   *
   * Configuration for the counter.
   *
   * @param support the ratio of [[LossyCounter.increment()]] calls to [[LossyCounter.total]] that a
   *                key must have to be considered a hot key.
   * @param error   the maximum relative error in the estimated count for a key.
   */
  case class Config(support: Double, error: Double) {
    require(support > 0 && support < 1, "support must be in (0, 1)")
    require(error > 0 && error < 1, "error must be in (0, 1)")
    require(error <= support / 10, "error must be significantly less than support")
  }
}

/**
 * Implementation of [[LossyCounter]] that tracks O(total) entries and has no error in the key
 * counts it tracks. Used for illustrative purposes and as a reference implementation for tests. The
 * implementation intentionally includes _all_ false positives permitted by the contract in
 * [[LossyCounter.getHotKeys()]] so that the results can be used to verify that any false
 * positives returned by a real implementation are within the configured error bounds.
 */
@NotThreadSafe
class InefficientLossyCounter[K](val config: LossyCounter.Config) extends LossyCounter[K] {
  private var totalCount: Long = 0L
  private val counts = mutable.Map.empty[K, Long].withDefaultValue(0L)

  override def increment(key: K): Unit = {
    totalCount += 1
    counts(key) += 1
  }

  override def total: Long = totalCount

  override def getHotKeys(): Map[K, Long] = {
    // Return all the false positives we're permitted to return based on the `error`.
    val countThreshold: Long = ((config.support - config.error) * total).floor.toLong
    counts.filter {
      case (_: K, count: Long) => count >= countThreshold
    }.toMap
  }

  /**
   * REQUIRES: `this.config` is the same as `counter.config` (otherwise, `this` is not a meaningful
   * reference).
   *
   * Checks that the hot key synopses returned by `counter` are consistent with the true counts
   * tracked in this reference implementation. (Because this implementation never drops state for
   * any keys, it knows the true counts for each key.) This allows us to check whether the actual
   * implementation satisfies its obligations:
   *
   *  - It returns all true hot keys (i.e., those with `count > support * total`).
   *  - While false negatives are permitted, the true counts of all keys must be within the
   *    configured error (i.e., keys with `count >= (support - error) * total`).
   *  - The estimated counts must be within `[trueCount * (1 - error), trueCount]`
   *    (underestimates by up to `error` are permitted, overestimates are not permitted).
   */
  def checkHotKeys(counter: LossyCounter[K]): Unit = {
    import org.scalatest.Assertions._ // use test assertions for better error messages
    assert(counter.config == this.config)
    assert(total == counter.total)

    val hotKeys: Map[K, Long] = counter.getHotKeys()
    val referenceHotKeys: Map[K, Long] = this.getHotKeys()

    // Compute the permitted absolute error based on the current total.
    val countError: Long = (total * config.error).ceil.toLong
    for (entry <- hotKeys) {
      val (key, estimatedCount): (K, Long) = entry
      assert(
        referenceHotKeys.contains(key),
        s"key $key is not a possible hot key (estimatedCount=$estimatedCount)"
      )
      val trueCount: Long = referenceHotKeys(key)

      // The lossy counter is allowed to underestimate the count by as much as `error`...
      assert(estimatedCount >= trueCount - countError)

      // ...but it is not permitted to overestimate.
      assert(estimatedCount <= trueCount)
    }
    // Verify that the implementation doesn't have any false negatives.
    val countThreshold: Long = (total * config.support).ceil.toLong
    for (entry <- referenceHotKeys) {
      val (key, trueCount): (K, Long) = entry
      if (trueCount >= countThreshold) {
        assert(
          hotKeys.contains(key),
          s"missing hot key $key (trueCount=$trueCount)"
        )
      }
    }
  }
}

/**
 * Classic presentation of the Lossy Count algorithm without any memory optimizations. Worst-case,
 * this implementation uses O(log(n)) space, where n is the number of calls to [[increment()]] so
 * far. In practice, the algorithm tends to use less space than probabilistic alternatives that have
 * constant space requirements but permit false negatives
 * (https://www.vldb.org/conf/2002/S10P03.pdf).
 */
@NotThreadSafe
class LossyCounterImpl[K](val config: LossyCounter.Config) extends LossyCounter[K] {
  import LossyCounterImpl.EstimatedCount

  /**
   * The bucket size is chosen such that a single sample accounts for no more than the error
   * "budget" for a bucket. See remarks on [[errorThreshold]].
   */
  private val bucketSize = (1 / config.error).ceil.toInt

  /**
   * Estimated counts for keys.
   *
   * INVARIANT: contains entries for all keys that potentially have count > [[errorThreshold]].
   */
  private val estimatedCounts = mutable.Map.empty[K, EstimatedCount]

  /** The total number of calls to [[increment]]. */
  private var totalCount: Long = 0L

  /**
   * The true count threshold below which an entry may be discarded because it's within the
   * acceptable error bounds for the lossy count algorithm. Used both when deciding whether an entry
   * must be retained and to determine the maximum error associated with an entry when it is added
   * to [[estimatedCounts]].
   */
  private def errorThreshold: Long = totalCount / bucketSize

  override def increment(key: K): Unit = {
    val estimate: EstimatedCount = estimatedCounts
      .getOrElseUpdate(
        key, {
          // Determine the maximum possible error for the new entry. If the key has never been seen
          // before, then the actual error is 0, but if the key was previously tracked by the
          // counter, it could only have been removed when its true count was less than
          // `errorThreshold`. Since `errorThreshold` is monotonic, we know that the maximum
          // possible true count at the time the (hypothetical) entry was removed is
          // `errorThreshold - 1`, and that becomes the `maxError` for the new entry.
          new EstimatedCount(maxError = errorThreshold)
        }
      )
    estimate.minCount += 1
    totalCount += 1
    if (totalCount % bucketSize == 0) {
      // Retain only entries with maxCount > errorThreshold. It would be correct to run this check
      // on every increment, but there's no need: the `errorThreshold` only increases at bucket
      // boundaries.
      estimatedCounts.retain {
        case (_: K, value: EstimatedCount) => value.maxCount > errorThreshold
      }
    }
  }

  override def total: Long = totalCount

  override def getHotKeys: Map[K, Long] = {
    // Compute the minimum true count that is above the current support threshold.
    val countThreshold: Long = (config.support * total).ceil.toLong

    // Return all entries whose maximum possible true count is at or above the threshold.
    val hotKeys = Map.newBuilder[K, Long]
    for (entry <- estimatedCounts) {
      val (key, estimate): (K, EstimatedCount) = entry
      if (estimate.maxCount >= countThreshold) {
        // Return the minimum rather than maximum count to satisfy the "no over-count" guarantee.
        // (The choice to prefer under-counts to over-counts in the contract is arbitrary.)
        hotKeys += ((key, estimate.minCount))
      }
    }
    hotKeys.result()
  }

  /** The number of keys tracked by the counter. */
  def size: Int = estimatedCounts.size
}
object LossyCounterImpl {

  /**
   * Count estimate for a key.
   *
   * @param maxError the maximum possible error in the estimate of the count for the current key.
   */
  private class EstimatedCount(val maxError: Long) {

    /**
     * Inclusive lower bound on the true count of the current key. May be an underestimate by up to
     * [[maxError]]. Never an overestimate.
     */
    var minCount: Long = 0L

    /** Inclusive upper bound on the true count of the current key. */
    def maxCount: Long = minCount + maxError
  }
}

/**
 * Implementation of the [[LossyEwmaCounter]] contract that tracks O(total) entries and has
 * no error in the time-weighted sums it tracks. Used for illustrative purposes and as a reference
 * implementation for tests. The implementation intentionally includes _all_ false positives
 * permitted by the contract in [[LossyEwmaCounter.getHotKeys()]] so that the results can be
 * used to verify that any false positives returned by a real implementation are within the
 * configured error bounds.
 */
@NotThreadSafe
class InefficientLossyEwmaCounter[K](val config: LossyEwmaCounter.Config) {

  /** True weighted sums for each key passed to [[incrementBy]]. */
  private val sums = mutable.Map.empty[K, Double].withDefaultValue(0.0)

  /**
   * The total weighted sum of all [[incrementBy]] calls, where all samples in [[currentEpoch]] have
   * weight 1 and weights decay by [[LossyEwmaCounter.Config.decay]] every epoch.
   */
  private var totalSum: Double = 0.0

  /** The highest epoch observed so far. */
  private var currentEpoch: Long = 0

  /** See [[LossyEwmaCounter.incrementBy()]]. */
  def incrementBy(now: Instant, key: K, value: Int): Unit = {
    require(value > 0, s"value must be positive: $value")

    // The epoch number is the number of seconds that have elapsed since `startTime`.
    val epoch: Long = (now.toEpochMilli - config.startTime.toEpochMilli) / 1000
    if (epoch > currentEpoch) {
      // Decay all existing counts if the epoch has advanced.
      val elapsedEpochs: Long = epoch - currentEpoch
      currentEpoch = epoch
      val decayNTimes: Double = math.pow(config.decay, elapsedEpochs)
      sums.transform {
        case (_: K, sum: Double) => sum * decayNTimes
      }
      totalSum *= decayNTimes
    }
    sums(key) += value
    totalSum += value
  }

  /**
   * Returns all keys that could be returned by a correct implementation of
   * [[LossyEwmaCounter]], i.e., all keys whose time-weighed contribution is greater than or
   * equal to `support - error - tolerance`, where `tolerance` allows tests to declare an absolute
   * tolerance for floating point error. Returns the true contributions of all keys. Note that keys
   * with contribution greater than `support` *must* be returned by a correct implementation (others
   * are optional). See [[LossyEwmaCounter.getHotKeys()]].
   */
  def getHotKeys(tolerance: Double = 0): Map[K, Double] = {
    val builder = Map.newBuilder[K, Double]
    for (entry <- sums) yield {
      val (key, timeWeightedCount): (K, Double) = entry
      // Compute the key contribution, which is the weighted sum for the key divided by the total
      // weighted sum across all keys.
      val contribution: Double = timeWeightedCount / totalSum

      // Any key that has contributed at least `support - error` can be returned.
      if (contribution >= config.support - config.error - tolerance) {
        builder += ((key, contribution))
      }
    }
    builder.result()
  }

  /**
   * REQUIRES: `this.config` is the same as `counter.config` (otherwise, `this` is not a meaningful
   *           reference).
   *
   * Checks that the hot key synopses returned by `counter` are consistent with the true
   * contributions tracked in this reference implementation. (Because this implementation never
   * drops state for any keys, it knows the true contribution of each key.) This allows us to check
   * whether the actual implementation satisfies its obligations:
   *
   *  - It returns all true hot keys (i.e., those with `trueContribution > support`).
   *  - While false negatives are permitted, the true contributions of those keys must be within the
   *    configured error and tolerance (i.e., keys with `trueContribution >= support - error`).
   *  - The estimated contributions must be within `[trueContribution - error, trueContribution]`
   *    (underestimates by up to `error` are permitted, overestimates are not permitted).
   *
   * For all comparisons, `tolerance` is used to allow for some absolute floating point error. For
   * some test cases, the tolerance may be 0 (e.g., when using "well-behaved" decay values like
   * 0.5 or 0.25).
   */
  def checkHotKeys(counter: LossyEwmaCounter[K], tolerance: Double = 0.0): Unit = {
    import org.scalatest.Assertions._ // use test assertions for better error messages
    assert(counter.config == this.config)
    val hotKeys: Map[K, Double] = counter.getHotKeys
    val referenceHotKeys: Map[K, Double] = this.getHotKeys(tolerance)

    // The reference implementation returns all hot keys (i.e., those with
    // `contribution >= support - error - tolerance`), and the actual implementation is permitted to
    // return any of these. The actual implementation is also required to return all true hot keys
    // (i.e., those with `contribution > support + tolerance`).
    for (entry <- hotKeys) {
      val (key, estimatedContribution): (K, Double) = entry
      assert(
        referenceHotKeys.contains(key),
        s"key $key is not a possible hot key (estimatedContribution=$estimatedContribution)"
      )
      val trueContribution: Double = referenceHotKeys(key)

      // The lossy counter is allowed to underestimate the contribution by as much as `error`...
      assert(estimatedContribution >= trueContribution - counter.config.error - tolerance)

      // ...but it is not permitted to overestimate.
      assert(estimatedContribution <= trueContribution + tolerance)
    }
    // Verify that the implementation doesn't have any false negatives.
    for (entry <- referenceHotKeys) {
      val (key, trueContribution): (K, Double) = entry
      if (trueContribution > counter.config.support + tolerance) {
        assert(
          hotKeys.contains(key),
          s"missing hot key $key (trueContribution=$trueContribution)"
        )
      }
    }
  }

  /** Returns the number of distinct keys observed by the counter. */
  def numKeys: Int = sums.size
}
