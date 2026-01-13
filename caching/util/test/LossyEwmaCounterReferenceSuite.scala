package com.databricks.caching.util

import org.apache.commons.math3.distribution.ZipfDistribution
import org.apache.commons.math3.random.Well19937c

import com.databricks.testing.DatabricksTest

class LossyEwmaCounterReferenceSuite extends DatabricksTest {

  /**
   * A wrapper around a [[LossyCounterImpl]] instance and an [[InefficientLossyCounter]] instance.
   * The [[InefficientLossyCounter]] is used as a reference implementation to verify that the
   * behaviors of the [[LossyCounterImpl]] are correct. After modifications to the counters via
   * [[increment()]] and [[incrementBy()]], validates the hot keys returned by the counter.
   */
  private class LossyCounterWrapper[K](override val config: LossyCounter.Config)
      extends LossyCounter[K] {

    /** Counter under test. */
    private val counter = new LossyCounterImpl[K](config)

    /** Reference implementation of the counter. */
    private val reference = new InefficientLossyCounter[K](config)

    override def increment(key: K): Unit = {
      counter.increment(key)
      reference.increment(key)
      reference.checkHotKeys(counter)
    }

    /** Increments `key` `n` times. */
    def incrementBy(key: K, n: Int): Unit = {
      for (_ <- 0 until n) {
        counter.increment(key)
        reference.increment(key)
      }
      reference.checkHotKeys(counter)
    }

    override def total: Long = {
      val value: Long = counter.total
      assert(value == reference.total)
      value
    }

    override def getHotKeys(): Map[K, Long] = {
      counter.getHotKeys()
    }

    /** Returns [[LossyCounterImpl.size]]. */
    def size: Int = counter.size
  }

  test("Hello, Lossy Count") {
    // Test plan: supply a sequence of inputs to [[LossyCounter]] intended to demonstrate the
    // following behaviors:
    //  - The right hot keys are emitted (no false negatives).
    //  - Keys are evicted (or not) at the expected thresholds. We can indirectly observe that
    //    eviction occurred when there is an underestimate, and that eviction did not occur when
    //    the estimate is exact.
    //  - Allow for false positives when a key is within the configured `error` threshold of the
    //    configured `support`.
    // Uses a [[LossyCounterWrapper]] to automatically validate expected behavior relative to a
    // reference implementation.

    val config = LossyCounter.Config(support = 0.1, error = 0.01)
    val counter = new LossyCounterWrapper[String](config)

    counter.incrementBy("a", 100)
    counter.incrementBy("b", 900)
    assert(counter.getHotKeys() == Map("a" -> 100L, "b" -> 900L))

    // After another 9K increments, "a" will be evicted because 100 / total=10K is at the error
    // threshold. "b" is not evicted because it's still above the error threshold, but it is no
    // longer a hot key.
    counter.incrementBy("c", 9000)
    assert(counter.getHotKeys() == Map("c" -> 9000L))

    // Now add back "a" with enough support that it should be in the hot keys result. But it will be
    // under-counted because it was previously evicted.
    counter.incrementBy("a", 2000)
    assert(counter.getHotKeys() == Map("a" -> 2000L, "c" -> 9000L))

    // Verify that "b" was not previously evicted by incrementing its count and verifying that
    // its count is not underestimated when it's a hot key again.
    counter.incrementBy("b", 6000)
    assert(counter.getHotKeys() == Map("a" -> 2000L, "b" -> 6900L, "c" -> 9000L))

    // Introduce a new key that does not have enough support to be a hot key but is just within the
    // configured error threshold. This new key is an (expected) false positive.
    assert(counter.total == 18000L)
    counter.incrementBy("d", 1900)
    counter.incrementBy("e", 100) // filler to get to 20K total
    assert(counter.getHotKeys() == Map("a" -> 2000L, "b" -> 6900L, "c" -> 9000L, "d" -> 1900L))
  }

  test("Before first scan") {
    // Test plan: internally, LossyCounterImpl performs a scan after every ceil(1 / error)
    // increments. Verify that the counter internally retains all keys until the first scan is
    // performed and that `getHotKeys` returns all hot keys after the first scan (there is
    // perfect information, so there should be no false positives). Advance beyond the bucket
    // boundary and verify that the counter only retains keys with counts above the error threshold.

    val counter =
      new LossyCounterWrapper[String](LossyCounter.Config(support = 0.4, error = 0.0201))

    // Expect 50 = ceil(1 / error) increments before the first scan. Required support for hot keys
    // after 49 increments is 19.6 = 49 * support, so keys with 20 or more increments should be
    // returned by `getHotKeys`.
    counter.incrementBy("a", 19)
    counter.increment("b")
    counter.incrementBy("c", 20)
    counter.incrementBy("d", 9)
    assert(counter.size == 4, "all keys should be retained")
    assert(
      counter.getHotKeys() == Map("c" -> 20L),
      "only key with 20+ increments should be returned"
    )
    // Increment to trigger eviction of "b".
    counter.increment("a")
    assert(counter.size == 3, "'b' should be evicted")
    assert(
      counter.getHotKeys() == Map("a" -> 20L, "c" -> 20L),
      "only keys with 20+ increments should be returned"
    )
  }

  test("Randomized") {
    // Test plan: supply a sequence of inputs drawn from a Zipf distribution to `LossyCounterImpl`
    // and the reference `InefficientLossyCounter`. Verify that the hot keys reported by the
    // counter under test are consistent with those identified by the reference implementation.
    // Verify that the counter under test retains at most `maxRetainedKeys` keys, to ensure that
    // eviction is effective in practice.

    val rng = new Well19937c(42)
    case class TestCase(
        description: String,
        support: Double,
        numKeys: Int,
        zipfianExponent: Double,
        maxRetainedKeys: Int)
    val testCases = Seq(
      TestCase(
        "10% support, 2000 keys",
        support = 0.1,
        numKeys = 2000,
        zipfianExponent = 2.0,
        maxRetainedKeys = 20
      ),
      TestCase(
        "10% support, 10 equalish keys",
        support = 0.1,
        numKeys = 10,
        zipfianExponent = 1.01,
        maxRetainedKeys = 10 // no eviction since all keys will remain above error threshold
      ),
      TestCase(
        "20% support, 100 keys",
        support = 0.2,
        numKeys = 100,
        zipfianExponent = 1.8,
        maxRetainedKeys = 10
      )
    )
    for (testCase <- testCases) {
      val config = LossyCounter.Config(support = testCase.support, error = testCase.support / 10)
      val counter = new LossyCounterWrapper[Int](config)

      // Increment the counter with a sequence of keys drawn from a Zipfian distribution. Rely on
      // the  internal consistency validation performed by [[LossyCounterWrapper]] rather than
      // explicitly checking results (since we're dependent on the reference implementation in this
      // randomized test case).
      val zipfDistribution = new ZipfDistribution(rng, testCase.numKeys, testCase.zipfianExponent)
      for (_ <- 0 until 200000) {
        val key: Int = zipfDistribution.sample()
        counter.increment(key)
      }
      assert(
        counter.size <= testCase.maxRetainedKeys,
        s"Too many retained keys: ${testCase.description}"
      )
    }
  }
}
