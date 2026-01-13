package com.databricks.dicer.client

import java.util.Random

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

import com.databricks.rpc.RequestHeaders
import io.prometheus.client.CollectorRegistry

import com.databricks.caching.util.TestUtils
import com.databricks.caching.util.TestUtils.{TestName, shamefullyAwaitForNonEventInAsyncTest}
import com.databricks.caching.util.{
  AssertionWaiter,
  FakeSequentialExecutionContextPool,
  FakeTypedClock,
  MetricUtils
}
import com.databricks.dicer.common.TargetHelper.TargetOps
import com.databricks.dicer.common.TestSliceUtils._
import com.databricks.dicer.common.{
  Assignment,
  AssignmentMetricsSource,
  ClientRequest,
  ClientType,
  InternalDicerTestEnvironment,
  ProposedSliceAssignment
}
import com.databricks.dicer.external.{Clerk, ResourceAddress, SliceKey, Slicelet, Target}
import com.databricks.dicer.friend.SliceMap
import com.databricks.testing.DatabricksTest
import com.databricks.threading.NamedExecutor

class ClerkImplSuite extends DatabricksTest with TestName {

  private val fakeClock = new FakeTypedClock()

  /** A Dicer test environment where the assigner uses `fakeClock`. */
  private val testEnv: InternalDicerTestEnvironment = InternalDicerTestEnvironment.create(
    secPool =
      FakeSequentialExecutionContextPool.create("clerk-impl-suite", numThreads = 8, fakeClock)
  )

  override def afterAll(): Unit = {
    testEnv.stop()
  }

  /** Creates a clerk that connects to the given Slicelet for assignments. */
  private def createClerk(slicelet: Slicelet): Clerk[ResourceAddress] = {
    testEnv.createClerk(slicelet)
  }

  /** Creates a clerk that directly connects to the Assigner for assignments. */
  private def createDirectClerk(target: Target): Clerk[ResourceAddress] = {
    testEnv.createDirectClerk(target, initialAssignerIndex = 0)
  }

  /**
   * Verifies that the assigner stops receiving watch requests once the clerk is stopped. After
   * advancing the fake clock far into the future, this function checks the latest watch request
   * recorded in the test assigner to determine if a new request was received.
   */
  private def verifyEventuallyNoWatchRequestsReceivedAfterStop(target: Target): Unit = {
    val latestReceivedWatchRequestOpt: Option[(RequestHeaders, ClientRequest)] =
      testEnv.testAssigner.getLatestClerkWatchRequest(target)
    // Advance the clock by 1 day, so the assigner will respond to any hanging watch requests and
    // the clerk can send the next request if it is still active.
    fakeClock.advanceBy(1.day)
    // Sleep for some time to allow the assigner time to respond and the clerk to send the next
    // watch request if it remains active. Note that sleeping is highly discouraged in tests, but
    // here we are checking for a *non-event*, and in such cases we have no other option than to
    // wait a little time to leave room for the undesired event to occur.
    shamefullyAwaitForNonEventInAsyncTest()
    assert(
      testEnv.testAssigner.getLatestClerkWatchRequest(target) == latestReceivedWatchRequestOpt
    )
  }

  test("Methods called after stop don't throw") {
    // Test plan: Verify that all methods called after `ClerkImpl.stop` don't throw for both a
    // regular Clerk and a direct Clerk.
    val target = Target(getSafeName)
    val slicelet: Slicelet =
      testEnv.createSlicelet(target).start(selfPort = 1234, listenerOpt = None)
    val regularClerk: Clerk[ResourceAddress] = createClerk(slicelet)
    val directClerk: Clerk[ResourceAddress] = createDirectClerk(target)

    // Setup: Stop both `ClerkImpl`s.
    regularClerk.impl.stop()
    directClerk.impl.stop()

    // Verify: All methods invoked after stopping don't throw.
    regularClerk.impl.ready
    regularClerk.impl.getStubForKey(SliceKey.MIN)
    regularClerk.impl.stop()
    directClerk.impl.ready
    directClerk.impl.getStubForKey(SliceKey.MIN)
    directClerk.impl.stop()

    // Cleanup: Stop the Slicelet.
    slicelet.forTest.stop()
  }

  test("ClerkImpl.stop") {
    // Test plan: Verify that `ClerkImpl.stop` removes the clerk info from `ClientSlicez` and that
    // the `ClerkImpl` neither sends watch requests nor incorporates new assignments after being
    // stopped.
    val target = Target(getSafeName)

    // Returns the latest generation number for the `target` with `AssignmentMetricsSource.Clerk`.
    def getLatestGenerationNumber: Double = {
      MetricUtils.getMetricValue(
        CollectorRegistry.defaultRegistry,
        "dicer_assignment_latest_generation_number",
        Map(
          "targetCluster" -> target.getTargetClusterLabel,
          "targetName" -> target.getTargetNameLabel,
          "source" -> AssignmentMetricsSource.Clerk.toString
        )
      )
    }

    // Returns the number of active `SliceLookup`s for the `target` with `ClientType.Clerk`.
    def getNumActiveSliceLookups: Long = {
      MetricUtils
        .getMetricValue(
          CollectorRegistry.defaultRegistry,
          metric = "dicer_client_num_active_slice_lookups",
          Map(
            "targetCluster" -> target.getTargetClusterLabel,
            "targetName" -> target.getTargetNameLabel,
            "clientType" -> ClientType.Clerk.toString
          )
        )
        .toLong
    }

    // Setup: Set and freeze an initial assignment to the assigner.
    val proposal: SliceMap[ProposedSliceAssignment] = sampleProposal()
    val initialAssignment: Assignment =
      TestUtils.awaitResult(testEnv.setAndFreezeAssignment(target, proposal), Duration.Inf)

    val initialNumActiveSliceLookup: Long = getNumActiveSliceLookups
    // Setup: Create a Clerk that connects directly to the assigner to send watch requests, because
    // the type of watch server backing a particular address is unlikely to affect the clerk's
    // stopping behavior, and the assigner is easier to control and verify than a Slicelet. (Note:
    // currently, creating a Slicelet with `fakeClock` in `testEnv` is not supported. Also, Slicelet
    // does not have an equivalent method to `getLatestClerkWatchRequest` for verification).
    val clerk: Clerk[ResourceAddress] = createDirectClerk(target)
    // Setup: Wait for the clerk to start and receive the first response.
    AssertionWaiter("Wait for the clerk to start and receive the first response").await {
      assert(getNumActiveSliceLookups == (initialNumActiveSliceLookup + 1))
      assert(clerk.impl.forTest.getLatestAssignmentOpt.contains(initialAssignment))
    }
    val numClientSlicezDataForTargetBeforeStop: Int =
      TestUtils.awaitResult(ClientSlicez.forTest.getData, Duration.Inf).count {
        clientSlicezData: ClientTargetSlicezData =>
          clientSlicezData.target == target
      }

    // Setup: Stop the clerk. Also wait for the SliceLookup to be cancelled.
    clerk.impl.stop()
    AssertionWaiter("Wait for the lookup metric to be decremented").await {
      assert(getNumActiveSliceLookups == initialNumActiveSliceLookup)
    }

    // Verify: ClientSlicez should unregister the clerk after stopping the clerk.
    AssertionWaiter("Wait for ClientSlicez to unregister the clerk").await {
      val numClientSlicezDataForTargetAfterStop: Int =
        TestUtils.awaitResult(ClientSlicez.forTest.getData, Duration.Inf).count {
          clientSlicezData: ClientTargetSlicezData =>
            clientSlicezData.target == target
        }
      assert(numClientSlicezDataForTargetAfterStop == (numClientSlicezDataForTargetBeforeStop - 1))
    }

    // Setup: Record the clerk's current assignment and the latest generation number.
    val initialAssignmentOpt: Option[Assignment] = clerk.impl.forTest.getLatestAssignmentOpt
    val initialLatestGenerationNumber: Double = getLatestGenerationNumber

    // Setup: Set and freeze a new assignment after the clerk is stopped.
    val newAssignment: Assignment =
      TestUtils.awaitResult(testEnv.setAndFreezeAssignment(target, proposal), Duration.Inf)
    assert(newAssignment != initialAssignment)

    // Verify: Since there may be a delay between when the clerk sends a request and when the
    // assigner receives it, a hanging request sent before `ClerkImpl.stop` might not have been
    // received. `AssertionWaiter` is used to prevent flakiness in these cases. (Note: if the clerk
    // isn't stopped, even if this test passes by chance, it will eventually fail after multiple
    // runs).
    AssertionWaiter("Wait for the clerk to be fully stopped").await {
      verifyEventuallyNoWatchRequestsReceivedAfterStop(target)
    }

    // Verify: Even if the assigner has a new assignment, the clerk's latest assignment and
    // generation number should stay unchanged because it has been stopped.
    assert(clerk.impl.forTest.getLatestAssignmentOpt == initialAssignmentOpt)
    assert(getLatestGenerationNumber == initialLatestGenerationNumber)
  }

  test("Multi-thread ClerkImpl.stop") {
    // Test plan: Verify that ClerkImpl.stop is thread-safe when invoked concurrently with other
    // clerk methods. Verify it as follows:
    // - Create a clerk and start concurrent operations from multiple threads, where one performs
    //   ClerkImpl.stop() and the remaining operations randomly invoke stop(), ready(), or
    //   getStubForKey().
    // - After all concurrent operations complete, set a new assignment and verify the clerk is
    //   fully stopped by confirming it neither sends new watch requests to the assigner nor
    //   incorporates the new assignment.

    val target = Target(getSafeName)

    // Setup: Create a Clerk that connects directly to the assigner to send watch requests, because
    // the type of watch server backing a particular address is unlikely to affect the clerk's
    // stopping behavior, and the assigner is easier to control and verify than a Slicelet. (Note:
    // currently, creating a Slicelet with `fakeClock` in `testEnv` is not supported. Also, Slicelet
    // does not have an equivalent method to `getLatestClerkWatchRequest` for verification).
    val clerk: Clerk[ResourceAddress] = createDirectClerk(target)

    val numOps: Int = 20
    val ec: ExecutionContext = NamedExecutor.create(getSafeName, 4)
    val random = new Random()
    // Setup: Randomly select one operation that will serve as the stop operation.
    val stopOpIndex: Int = random.nextInt(numOps)

    val futs: Seq[Future[Unit]] =
      for (i: Int <- 0 until numOps) yield {
        Future {
          if (i == stopOpIndex) {
            // If it is the chosen stop operation index, call `ClerkImpl.stop`.
            clerk.impl.stop()
          } else {
            // Otherwise, randomly invoke a method on the clerk.
            random.nextInt(3) match {
              case 0 => clerk.impl.stop()
              case 1 => clerk.impl.ready
              case 2 => clerk.impl.getStubForKey(SliceKey.MIN)
            }
          }
          ()
        }(ec)
      }
    // Setup: Wait for all tasks to finish.
    val fut: Future[Seq[Unit]] = Future.sequence(futs)(implicitly, ec)
    Await.result(fut, Duration.Inf)

    val proposal: SliceMap[ProposedSliceAssignment] = sampleProposal()

    // Verify: The clerk should stop sending new watch requests and incorporating new assignments
    // after being stopped.
    AssertionWaiter("Wait for the clerk to be fully stopped").await {
      // Setup: Set and freeze a new assignment after the clerk is stopped.
      val newAssignment: Assignment =
        Await.result(testEnv.setAndFreezeAssignment(target, proposal), Duration.Inf)
      verifyEventuallyNoWatchRequestsReceivedAfterStop(target)
      assert(!clerk.impl.forTest.getLatestAssignmentOpt.contains(newAssignment))
    }
  }
}
