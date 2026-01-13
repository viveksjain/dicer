package com.databricks.caching.util

import com.databricks.testing.DatabricksTest
import com.databricks.threading.NamedExecutor
import com.databricks.caching.util.TestUtils.TestName
import io.grpc.Status
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.collection.mutable
import scala.collection.JavaConverters._

class WatchValueCellPollAdapterSuite extends DatabricksTest with TestName {

  /**
   * A major use case of the WatchValueCellPollAdapter class is dynamic configuration via SAFE.
   * A SAFE batch flag produces a value typed Map[String, String], which can be polled by calling
   * `batchFlag.getSubFlagValues()`. An example value is:
   * {{{
   *   "databricks.softstore.config.namespaces.foo" => "$foo_config_value_in_proto_string."
   *   "databricks.softstore.config.namespaces.bar" => "$bar_config_value_in_proto_string."
   * }}}
   * The value is of raw type and the desired parsed type is Map[String, NamespaceConfig] where the
   * keys are namespace names (e.g. "foo", "bar"), and the values are NamespaceConfig case class
   * instances which include memory limit, rate limit, and other configured values.
   *
   * In this suite, we define two classes below to simulate the process of watching the SAFE batch
   * flag value. The raw value type is Map[String, String] with key in the format of "raw-*" and
   * value in the format of "raw-value-*".
   * The parsed value type is Map[String, ParsedValue] with key in the format of "parsed-*" and the
   * value in the format of "parsed-value-*"
   */
  /** Type alias for better readability. */
  private type RawValueType = Map[String, String]
  private type ParsedValueMap = Map[String, ParsedValue]

  /** A case class for the parsed value. */
  private case class ParsedValue(value: String) {
    require(value.startsWith("raw-"))
  }

  /** A class that mock the SAFE batch flag. */
  private class MockSafeBatchFlag(initialValue: RawValueType) {
    private val flagValue = new ConcurrentHashMap[String, String]()
    for ((key, value) <- initialValue) {
      flagValue.put(key, value)
    }

    /** Get the most recent value of the map. */
    def getCurrentValue: RawValueType = {
      flagValue.asScala.toMap
    }

    /** Update the value for the map. */
    def updateValue(key: String, value: String): Unit = {
      flagValue.put(key, value)
    }
  }

  /** A subscriber class for test that watch the map value. */
  private class TestSubscriber(index: Int) {
    // Guarded by subscriberSec.
    private var latestKeyValueMap: StatusOr[ParsedValueMap] = StatusOr.success(Map.empty)

    private val subscriberSec =
      SequentialExecutionContext.createWithDedicatedPool(s"subscriber-sec-$index")

    val valueStreamCallback: ValueStreamCallback[ParsedValueMap] =
      new ValueStreamCallback[ParsedValueMap](subscriberSec) {

        override protected def onSuccess(value: ParsedValueMap): Unit = {
          subscriberSec.assertCurrentContext()
          latestKeyValueMap = StatusOr.success(value)
        }
      }

    val streamCallback: StreamCallback[ParsedValueMap] =
      new StreamCallback[ParsedValueMap](subscriberSec) {

        override protected def onSuccess(value: ParsedValueMap): Unit = {
          subscriberSec.assertCurrentContext()
          latestKeyValueMap = StatusOr.success(value)
        }

        override protected def onFailure(status: Status): Unit = {
          subscriberSec.assertCurrentContext()
          latestKeyValueMap = StatusOr.error(status)
        }
      }

    def getLatestKeyValueMap: ParsedValueMap = {
      Await
        .result(
          subscriberSec.call {
            latestKeyValueMap
          },
          Duration.Inf
        )
        .get
    }
  }

  /**
   * Transform `rawValue` to a parsed value. If the rawValue is malformed, returns the
   * `lastParsedValue`. This is to prevent from SAFE creating a bad value.
   */
  private def transformation(rawValue: RawValueType): ParsedValueMap = {
    val resultMap = mutable.Map[String, ParsedValue]()
    for ((rawKey, rawValue) <- rawValue) {
      if (!rawKey.startsWith("raw-") || !rawValue.startsWith("raw-")) {
        return INITIAL_MAP_VALUE_PARSED
      }
      val parsedKey = "parsed-" + rawKey.substring("raw-".length)
      val parsedValue = ParsedValue(rawValue)
      resultMap.put(parsedKey, parsedValue)
    }
    resultMap.toMap
  }

  /** The initial value for the KV map in raw value type. */
  private val INITIAL_MAP_VALUE_RAW: RawValueType = Map(
    "raw-key-1" -> "raw-value-1",
    "raw-key-2" -> "raw-value-2",
    "raw-key-3" -> "raw-value-3"
  )

  /** The initial value for the KV map in parsed value type. */
  private val INITIAL_MAP_VALUE_PARSED: ParsedValueMap = Map(
    "parsed-key-1" -> ParsedValue("raw-value-1"),
    "parsed-key-2" -> ParsedValue("raw-value-2"),
    "parsed-key-3" -> ParsedValue("raw-value-3")
  )

  /** The interval between each poll. */
  private val TEST_POLL_INTERVAL: FiniteDuration = 100.milliseconds

  /** A sequential execution context for test. */
  private val sec = SequentialExecutionContext.createWithDedicatedPool("test-sec")

  test("Test WatchValueCellPollAdapterSuite with a single subscriber") {
    // Test plan:
    // 1. Create a WatchValueCellPollAdapter where the pollerThunk is pollCurrentValue.
    //    The periodical poll will start implicitly after startup.
    // 2. Add a subscriber to watch the batch flag.
    // 3. Update the value for the batch flag with a valid value.
    // 4. Wait for a sufficient amount of time to ensure the callback has been executed, and
    //    verify that the subscriber has received the latest value.
    // 5. Update the value for the batch flag with an invalid value.
    // 6. Wait for a sufficient amount of time to ensure the callback has been executed, and
    //    verify that the subscriber's value hasn't been changed.
    val mockSafeBatchFlag = new MockSafeBatchFlag(INITIAL_MAP_VALUE_RAW)

    val watchValueCellAdapter = new WatchValueCellPollAdapter[RawValueType, ParsedValueMap](
      INITIAL_MAP_VALUE_PARSED,
      () => mockSafeBatchFlag.getCurrentValue,
      transformation,
      TEST_POLL_INTERVAL,
      NamedExecutor.create(name = s"ec-$getSafeName", maxThreads = 1),
      sec
    )

    val subscriber = new TestSubscriber(0)
    watchValueCellAdapter.watch(subscriber.valueStreamCallback)
    assert(watchValueCellAdapter.getStatus == Status.OK) // Status is always OK.

    // Before any updates, the key value map should return the initial value.
    AssertionWaiter("Single-subscriber-assertion-0").await {
      assert(subscriber.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED)
    }

    // Update the flag value with a valid value.
    mockSafeBatchFlag.updateValue("raw-key-2", "raw-value-2-1")

    // Wait for consumer to catch the value update and verify the parsed value is fresh.
    val expectedParsedValue: ParsedValueMap = Map[String, ParsedValue](
      "parsed-key-1" -> ParsedValue("raw-value-1"),
      "parsed-key-2" -> ParsedValue("raw-value-2-1"),
      "parsed-key-3" -> ParsedValue("raw-value-3")
    )
    AssertionWaiter("Single-subscriber-assertion-1").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(expectedParsedValue))
      assert(subscriber.getLatestKeyValueMap == expectedParsedValue)
    }

    // Update the flag value with an INVALID value.
    mockSafeBatchFlag.updateValue("raw-key-2", "invalid-value-2-1")

    // Wait for consumer to catch the value update and verify the parsed value is restored to the
    // static fallback.
    AssertionWaiter("Single-subscriber-assertion-2").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(INITIAL_MAP_VALUE_PARSED))
      assert(subscriber.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED)
    }
    watchValueCellAdapter.cancel()
    assert(watchValueCellAdapter.getStatus == Status.OK) // Status is always OK.
  }

  test("Test WatchValueCellPollAdapterSuite with multiple subscribers") {
    // Test plan: similar to the single subscriber unit test, just add more subscribers to verify
    // things still work well when there are multiple subscribers.
    val mockSafeBatchFlag = new MockSafeBatchFlag(INITIAL_MAP_VALUE_RAW)

    val watchValueCellAdapter = new WatchValueCellPollAdapter[RawValueType, ParsedValueMap](
      INITIAL_MAP_VALUE_PARSED,
      () => mockSafeBatchFlag.getCurrentValue,
      transformation,
      TEST_POLL_INTERVAL,
      NamedExecutor.create(name = s"ec-$getSafeName", maxThreads = 1),
      sec
    )

    val subscriber0 = new TestSubscriber(0)
    val subscriber1 = new TestSubscriber(1)
    val subscriber2 = new TestSubscriber(2)

    // Two subscribers start to watch the value.
    watchValueCellAdapter.watch(subscriber0.valueStreamCallback)
    watchValueCellAdapter.watch(subscriber1.valueStreamCallback)
    assert(watchValueCellAdapter.getStatus == Status.OK) // Status is always OK.

    // Before any updates, the key value map should return the initial value for the watched
    // subscribers.
    AssertionWaiter("Multiple-subscriber-assertion-0").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(INITIAL_MAP_VALUE_PARSED))
      assert(
        subscriber0.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED &&
        subscriber1.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED &&
        subscriber2.getLatestKeyValueMap != INITIAL_MAP_VALUE_PARSED
      )
    }

    // Update the flag value with a valid value.
    mockSafeBatchFlag.updateValue("raw-key-2", "raw-value-2-1")

    // Wait for consumer to catch the value update and verify the parsed value is fresh.
    val expectedParsedValue: ParsedValueMap = Map[String, ParsedValue](
      "parsed-key-1" -> ParsedValue("raw-value-1"),
      "parsed-key-2" -> ParsedValue("raw-value-2-1"),
      "parsed-key-3" -> ParsedValue("raw-value-3")
    )
    AssertionWaiter("Multiple-subscriber-assertion-1").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(expectedParsedValue))
      assert(
        subscriber0.getLatestKeyValueMap == expectedParsedValue &&
        subscriber1.getLatestKeyValueMap == expectedParsedValue
      )
    }

    // Register another subscriber.
    watchValueCellAdapter.watch(subscriber2.valueStreamCallback)
    // Do some further updates for the batch flag.
    mockSafeBatchFlag.updateValue("raw-key-3", "raw-value-3-1")
    mockSafeBatchFlag.updateValue("raw-key-1", "raw-value-1-1")

    // Wait for consumer to catch the value update and verify the parsed value is fresh.
    val expectedParsedValue2: ParsedValueMap = Map[String, ParsedValue](
      "parsed-key-1" -> ParsedValue("raw-value-1-1"),
      "parsed-key-2" -> ParsedValue("raw-value-2-1"),
      "parsed-key-3" -> ParsedValue("raw-value-3-1")
    )
    AssertionWaiter("Multiple-subscriber-assertion-2").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(expectedParsedValue2))
      assert(
        subscriber0.getLatestKeyValueMap == expectedParsedValue2 &&
        subscriber1.getLatestKeyValueMap == expectedParsedValue2 &&
        subscriber2.getLatestKeyValueMap == expectedParsedValue2
      )
    }

    // Update the flag value with an INVALID value.
    mockSafeBatchFlag.updateValue("raw-key-2", "invalid-value-2-1")

    // Wait for consumer to catch the value update and verify the parsed value is restored to the
    // static fallback.
    AssertionWaiter("Multiple-subscriber-assertion-3").await {
      assert(
        subscriber0.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED &&
        subscriber1.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED &&
        subscriber2.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED
      )
    }
    watchValueCellAdapter.cancel()
    assert(watchValueCellAdapter.getStatus == Status.OK) // Status is always OK.
  }

  test("Test watch as StreamCallback") {
    // Test plan: verify that a subscriber receives the latest value when the adapter is watched
    // by the subscriber's callback as a StreamCallback.
    val mockSafeBatchFlag = new MockSafeBatchFlag(INITIAL_MAP_VALUE_RAW)

    val watchValueCellAdapter = new WatchValueCellPollAdapter[RawValueType, ParsedValueMap](
      INITIAL_MAP_VALUE_PARSED,
      () => mockSafeBatchFlag.getCurrentValue,
      transformation,
      TEST_POLL_INTERVAL,
      NamedExecutor.create(name = s"ec-$getSafeName", maxThreads = 1),
      sec
    )

    val subscriber = new TestSubscriber(0)
    watchValueCellAdapter.watch(subscriber.streamCallback)
    assert(watchValueCellAdapter.getStatus == Status.OK) // Status is always OK.

    // Before any updates, the key value map should return the initial value.
    AssertionWaiter("Single-subscriber-assertion-0").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(INITIAL_MAP_VALUE_PARSED))
      assert(subscriber.getLatestKeyValueMap == INITIAL_MAP_VALUE_PARSED)
    }

    // Update the flag value with a valid value.
    mockSafeBatchFlag.updateValue("raw-key-2", "raw-value-2-1")

    // Wait for consumer to catch the value update and verify the parsed value is fresh.
    val expectedParsedValue: ParsedValueMap = Map[String, ParsedValue](
      "parsed-key-1" -> ParsedValue("raw-value-1"),
      "parsed-key-2" -> ParsedValue("raw-value-2-1"),
      "parsed-key-3" -> ParsedValue("raw-value-3")
    )
    AssertionWaiter("Single-subscriber-assertion-1").await {
      assert(watchValueCellAdapter.getLatestValueOpt == Some(expectedParsedValue))
      assert(subscriber.getLatestKeyValueMap == expectedParsedValue)
    }
  }
}
