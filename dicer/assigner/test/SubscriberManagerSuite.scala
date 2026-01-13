package com.databricks.dicer.assigner
import com.databricks.caching.util.{FakeSequentialExecutionContextPool, FakeTypedClock, TestUtils}
import com.databricks.testing.DatabricksTest

import scala.concurrent.duration._
import scala.concurrent.{Future}
import com.databricks.dicer.common.{ClerkSubscriberSlicezData, SliceletSubscriberSlicezData}
import com.databricks.dicer.external.Target
import com.databricks.caching.util.TestUtils

class SubscriberManagerSuite extends DatabricksTest {
  test("The slicez data is empty when there is no subscriber") {
    // Test Plan: Create a `SubscriberManager` without subscribers and verify that the `slicez`
    // data is empty. This case can only be tested by directly calling
    // `SubscriberManager.getSlicezData()` because, in the assigner,
    // `SubscriberManager.getSlicezData()` is invoked with a target only after a watch request is
    // received, at which point subscribers exist. Cases involving subscribers are in
    // `SlicezSuite.scala`.

    val fakeClock = new FakeTypedClock()
    val secPool: FakeSequentialExecutionContextPool =
      FakeSequentialExecutionContextPool.create("subscriber-manager-suite", 1, fakeClock)
    val subscriberManager = new SubscriberManager(
      secPool,
      getSuggestedClerkRpcTimeoutFn = () => 1.second,
      suggestedSliceletRpcTimeout = 1.second
    )

    val sliceDataFut: Future[(Seq[SliceletSubscriberSlicezData], Seq[ClerkSubscriberSlicezData])] =
      subscriberManager.getSlicezData(Target("test-target"))

    val sliceData: (Seq[SliceletSubscriberSlicezData], Seq[ClerkSubscriberSlicezData]) =
      TestUtils.awaitResult(sliceDataFut, Duration.Inf)
    assertResult(sliceData)((Seq.empty, Seq.empty))
  }
}
