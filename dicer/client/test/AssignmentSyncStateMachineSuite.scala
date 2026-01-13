package com.databricks.dicer.client

import java.net.URI
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future}
import scala.concurrent.duration._

import io.grpc.Status

import com.databricks.caching.util.TestUtils
import com.databricks.caching.util.TestUtils.TestName
import com.databricks.caching.util.{
  AssertionWaiter,
  FakeSequentialExecutionContext,
  FakeTypedClock,
  StateMachineDriver,
  TestStateMachineDriver,
  TickerTime
}
import com.databricks.dicer.client.AssignmentSyncStateMachine.{DriverAction, Event}
import com.databricks.dicer.common.{
  Assignment,
  AssignmentConsistencyMode,
  ClerkData,
  ClientRequest,
  ClientResponse,
  ClientType,
  Generation,
  ProposedAssignment,
  Redirect,
  SyncAssignmentState,
  TestSliceUtils
}
import com.databricks.dicer.common.TestSliceUtils._
import com.databricks.dicer.external.{Slice, Target}
import com.databricks.rpc.tls.TLSOptionsMigration
import com.databricks.rpc.testing.TestSslArguments
import com.databricks.testing.DatabricksTest

import java.time.Instant
import java.util.Random

class AssignmentSyncStateMachineSuite extends DatabricksTest with TestName {

  /** A type representing the assignment sync state machine driver for readability. */
  private type AssignmentSyncStateMachineDriver =
    StateMachineDriver[Event, DriverAction, AssignmentSyncStateMachine]

  /** SEC used for the AssignmentSyncStateMachine. */
  private val sec = FakeSequentialExecutionContext.create("AssignmentSyncStateMachineSuite")

  /** The target to use for the current test, derived from the test name. */
  private def target: Target = Target(getSafeName)

  /**
   * Creates the client configuration for a AssignmentSyncStateMachine, with test SSL parameters.
   */
  private def createInternalClientConfig(): InternalClientConfig = {
    InternalClientConfig(
      ClientType.Clerk,
      subscriberDebugName = s"test-clerk",
      watchAddress = URI.create("fake-address"),
      tlsOptionsOpt = TLSOptionsMigration.convert(TestSslArguments.clientSslArgs),
      target,
      watchStubCacheTime = 10.seconds,
      watchFromDataPlane = false,
      rejectWatchRequestsOnFatalTargetMismatch = true,
      assignmentLatencySampleFraction = 0
    )
  }

  test("StateMachine deadline exceeded") {
    // Test plan: Verify that if the AssignmentSyncStateMachine doesn't get a response by the
    // deadline, it will hedge and retry a new request. Do this by first setting up a redirect,
    // verifying that a request is sent to the redirected address, and then simulate a timeout of
    // the redirected request. After the expected deadline, we should retry using the default
    // address. Then, if we later gets back a successful response to the original request, it will
    // be incorporated into the state machine.

    val config: InternalClientConfig = createInternalClientConfig()
    // Track all actions sent by AssignmentSyncStateMachine. Protected by `sec`.
    val receivedActions = ArrayBuffer[DriverAction]()
    def recordAction(action: DriverAction): Unit = {
      sec.assertCurrentContext()
      receivedActions += action
    }
    val driver =
      new AssignmentSyncStateMachineDriver(
        sec,
        new AssignmentSyncStateMachine(config, new Random),
        recordAction
      )
    sec.run {
      driver.start()
    }

    var expectedNumActions = 1
    // Get a SendRequest action on driver start.
    val firstRequest: DriverAction.SendRequest =
      AssertionWaiter("Initial SendRequest action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == expectedNumActions)
        receivedActions.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }

    // Inject a response with a redirect. We will test later that after the request times out, we
    // send the next request to the default address. The redirect also makes the test simpler
    // because in `onReadFailure` we don't add a backoff when falling back to the default address.
    val uri = URI.create("fake-redirect")
    val redirect = Redirect(Some(uri))
    val response = ClientResponse(
      SyncAssignmentState.KnownGeneration(Generation.EMPTY),
      config.watchRpcTimeout,
      redirect
    )
    sec.call {
      driver.handleEvent(Event.ReadSuccess(None, firstRequest.opId, response))
    }

    // The state machine should send a request to the redirected address.
    expectedNumActions += 1
    val redirectedRequest: DriverAction.SendRequest =
      AssertionWaiter("Second SendRequest action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == expectedNumActions)
        receivedActions.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }
    assert(redirectedRequest.addressOpt.contains(uri))
    assert(
      redirectedRequest.syncState ==
      SyncAssignmentState.KnownGeneration(Generation.EMPTY)
    )

    // The request deadline is `watchRpcTimeout`. Advancing the clock less
    // than that shouldn't trigger retry.
    sec.getClock.advanceBy(config.watchRpcTimeout - 100.millis)
    TestUtils.awaitResult(sec.call {
      assert(receivedActions.size == expectedNumActions)
    }, Duration.Inf)

    // Advancing beyond the deadline should now create another SendRequest.
    sec.getClock.advanceBy(100.millis)
    expectedNumActions += 1
    val fallbackRequest: DriverAction.SendRequest =
      AssertionWaiter("Retry SendRequest action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == expectedNumActions)
        receivedActions.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }
    // The fallback request should go to the default address.
    assert(fallbackRequest.addressOpt.isEmpty)

    // Now, try to send a successful response to an old request. A successful response with
    // `KnownAssignment` with a newer generation, and a redirect with the same generation, should be
    // incorporated. The new assignment will trigger `DriverAction.UseAssignment`, but we shouldn't
    // try to send another request. When we send the next request though, the redirect should be
    // used.
    val asnGeneration: Generation = 2 ## 6
    val assignment = createAssignment(
      asnGeneration,
      AssignmentConsistencyMode.Affinity,
      Slice.FULL @@ asnGeneration -> Seq("pod0")
    )
    val response2 = ClientResponse(
      SyncAssignmentState.KnownAssignment(assignment),
      config.watchRpcTimeout,
      redirect
    )
    sec.call {
      driver.handleEvent(Event.ReadSuccess(Some(uri), redirectedRequest.opId, response2))
    }
    expectedNumActions += 1
    val useAssignment: DriverAction.UseAssignment =
      AssertionWaiter("UseAssignment action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == expectedNumActions)
        receivedActions.last match {
          case useAssignmentAction: DriverAction.UseAssignment => useAssignmentAction
          case otherAction => fail(s"Expected UseAssignment action, but got $otherAction")
        }
      }
    assert(useAssignment.assignment == assignment)

    // Send failed responses: one with invalid, newer `opId`, the other with an older request. A
    // failed response that doesn't correspond to the last request should be ignored.
    sec.call {
      driver.handleEvent(Event.ReadFailure(fallbackRequest.opId + 10, Status.DEADLINE_EXCEEDED))
      driver.handleEvent(Event.ReadFailure(redirectedRequest.opId, Status.DEADLINE_EXCEEDED))
    }
    TestUtils.awaitResult(sec.call {
      assert(receivedActions.size == expectedNumActions)
    }, Duration.Inf)

    // Inject a response to the latest request. Verify that the new request that is sent uses
    // `asnGeneration` and `redirect.addressOpt`.
    sec.call {
      driver.handleEvent(Event.ReadSuccess(None, fallbackRequest.opId, response))
    }
    expectedNumActions += 1
    val finalRequest: DriverAction.SendRequest =
      AssertionWaiter("Final SendRequest action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == expectedNumActions)
        receivedActions.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }
    assert(
      finalRequest.syncState ==
      SyncAssignmentState.KnownGeneration(asnGeneration)
    )
    assert(finalRequest.addressOpt.contains(uri))
  }

  test("AssignmentSyncStateMachine Event.Cancel") {
    // Test plan: Verify that when the state machine receives a `Cancel` event, it stops generating
    // any actions, as follows:
    // - Use the same SEC to create another `activeDriver` which will not be cancelled, start both
    //   drivers, and wait for both to send the initial watch requests.
    // - Cancel the `driver`.
    // - Trigger a `ReadSuccess` with an empty generation and a redirect for `driver` and then
    //   `activeDriver`. Verify that only the latter generates a new redirect request.
    // - Advance the clock to trigger a timeout and retry of the redirected request for both
    //   drivers. Verify that only the active driver generates a retry request.
    // - Trigger a `ReadSuccess` with a valid assignment. Verify that only the active driver
    //   generates a `UseAssignment` action.
    // - Trigger a `ReadFailure` and invoke `onAdvance` explicitly after backoff. Verify that
    //   only the active driver generates a retry request.

    val config: InternalClientConfig = createInternalClientConfig()
    // Setup: Create a driver which will be stopped. All its received actions are captured in
    // `receivedActions`.
    val receivedActions = ArrayBuffer[DriverAction]()
    def recordAction(action: DriverAction): Unit = {
      sec.assertCurrentContext()
      receivedActions += action
    }
    val driver =
      new AssignmentSyncStateMachineDriver(
        sec,
        new AssignmentSyncStateMachine(config, new Random),
        recordAction
      )
    // Setup: Create another driver which will not be cancelled and will be always active. All its
    // received actions are captured in `receivedActionsForActiveDriver`.
    val receivedActionsForActiveDriver = ArrayBuffer[DriverAction]()
    def recordAction2(action: DriverAction): Unit = {
      sec.assertCurrentContext()
      receivedActionsForActiveDriver += action
    }
    val activeDriver =
      new AssignmentSyncStateMachineDriver(
        sec,
        new AssignmentSyncStateMachine(config, new Random),
        recordAction2
      )

    // Setup: Create a seq to support iterating over both drivers. Since they share the same SEC,
    // `driver` is placed before `activeDriver` to ensure that when the active driver completes an
    // action, the `driver` has already completed the corresponding action. So to test a no-op for
    // the stopped `driver`, we don’t need to call Thread.sleep. (We just need to wait for
    // `activeDriver` to receive the corresponding action.)
    val drivers: Seq[AssignmentSyncStateMachineDriver] = Seq(driver, activeDriver)

    // Setup: Start 2 drivers, and wait for the first watch requests from both.
    sec.run {
      for (driver: AssignmentSyncStateMachineDriver <- drivers) {
        driver.start()
      }
    }
    val firstRequest: DriverAction.SendRequest =
      AssertionWaiter("Initial SendRequest action", ecOpt = Some(sec)).await {
        assert(receivedActions.size == 1)
        assert(receivedActionsForActiveDriver.size == 1)
        // The initial watch request should be the same for both drivers, so we randomly pick one.
        receivedActions.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }

    // Setup: Cancel the `driver`.
    sec.run {
      driver.handleEvent(Event.Cancel)
    }

    // Setup: For both drivers, trigger a `ReadSuccess` with a response with an empty assignment and
    // a redirect.
    sec.run {
      val redirect = Redirect(Some(URI.create("fake-redirect")))
      val response = ClientResponse(
        SyncAssignmentState.KnownGeneration(Generation.EMPTY),
        config.watchRpcTimeout,
        redirect
      )
      for (driver: AssignmentSyncStateMachineDriver <- drivers) {
        sec.call {
          driver.handleEvent(Event.ReadSuccess(None, firstRequest.opId, response))
        }
      }
    }

    // Verify: `driver` should not send any new requests while `activeDriver` should send a second
    // request to the redirected address.
    val redirectedRequest: DriverAction.SendRequest =
      AssertionWaiter("Second SendRequest action").await {
        assert(receivedActionsForActiveDriver.size == 2)
        receivedActionsForActiveDriver.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }
    assert(receivedActions == Seq(firstRequest))

    // Setup: We advance the clock to trigger a timeout and retry of the redirected request.
    sec.getClock.advanceBy(config.watchRpcTimeout)

    // Verify: `driver` should not send any new requests while `activeDriver` should send a retry
    // request.
    val retryRequest: DriverAction.SendRequest =
      AssertionWaiter("Third SendRequest action").await {
        assert(receivedActionsForActiveDriver.size == 3)
        receivedActionsForActiveDriver.last match {
          case sendRequestAction: DriverAction.SendRequest => sendRequestAction
          case otherAction => fail(s"Expected SendRequest action, but got $otherAction")
        }
      }
    assert(receivedActions == Seq(firstRequest))

    // Setup: For both drivers, trigger a `ReadSuccess` with a response which has a valid
    // assignment.
    sec.run {
      val asnGeneration: Generation = 2 ## 6
      val assignment = createAssignment(
        asnGeneration,
        AssignmentConsistencyMode.Affinity,
        Slice.FULL @@ asnGeneration -> Seq("pod0")
      )
      val response2 = ClientResponse(
        SyncAssignmentState.KnownAssignment(assignment),
        config.watchRpcTimeout,
        Redirect.EMPTY
      )
      for (driver: AssignmentSyncStateMachineDriver <- drivers) {
        driver.handleEvent(Event.ReadSuccess(None, redirectedRequest.opId, response2))
      }
    }

    // Verify: `driver` should not generate any actions while `activeDriver` should generate a
    // `UseAssignment` action.
    AssertionWaiter("UseAssignment action").await {
      assert(receivedActionsForActiveDriver.size == 4)
      assert(receivedActionsForActiveDriver.last.isInstanceOf[DriverAction.UseAssignment])
    }
    assert(receivedActions == Seq(firstRequest))

    // Setup: For both drivers, trigger a `ReadFailure`.
    sec.run {
      for (driver: AssignmentSyncStateMachineDriver <- drivers) {
        driver.handleEvent(
          Event.ReadFailure(retryRequest.opId, Status.DEADLINE_EXCEEDED)
        )
      }
    }

    // Verify: `driver` should not send any more requests while `activeDriver` should send a retry
    // request, if we explicitly call `onAdvance` after the backoff period has elapsed.
    val onAdvanceFut: Future[Unit] = sec.call {
      assert(
        driver.forTest.getStateMachine
          .onAdvance(
            sec.getClock.tickerTime() + 1.hour,
            sec.getClock.instant().plusSeconds(60 * 60)
          )
          .actions
          .isEmpty
      )
      assert(
        activeDriver.forTest.getStateMachine
          .onAdvance(
            sec.getClock.tickerTime() + 1.hour,
            sec.getClock.instant().plusSeconds(60 * 60)
          )
          .actions
          .head
          .isInstanceOf[DriverAction.SendRequest]
      )
    }
    TestUtils.awaitResult(onAdvanceFut, Duration.Inf)
  }

  test("Request containing assignment for different target ignored") {
    // Test plan: verify that a new sync state machine does not erroneously incorporate the
    // assignment for a target which it does not own, even if it has no assignment itself.

    // Use fixed timestamps generated by a fake clock to avoid non-deterministic actions requested
    // by the state machine (will hedge requests if too much time elapses before a response).
    val uri1: URI = URI.create("kubernetes-cluster:test-env/cloud1/public/region1/clustertype2/01")
    val target = Target.createKubernetesTarget(uri1, getSafeName)
    val clock = new FakeTypedClock()
    val tickerTime: TickerTime = clock.tickerTime()
    val instant: Instant = clock.instant()

    val config: InternalClientConfig = createInternalClientConfig()
    val testDriver: TestStateMachineDriver[Event, DriverAction] =
      new TestStateMachineDriver(
        new AssignmentSyncStateMachine(config, new Random())
      )

    // Send the initial `onAdvance` call to initialize the state machine.
    testDriver.onAdvance(tickerTime, instant)

    // Send a request containing an assignment for a different target name. Despite not having any
    // assignment, the state machine should not request that the driver incorporate the
    // assignment.
    val fatallyMismatchedTarget = Target(getSuffixedSafeName("other"))
    val assignment1: Assignment = ProposedAssignment(
      predecessorOpt = None,
      TestSliceUtils.createProposal(
        ("" -- ∞) -> Seq("Pod2")
      )
    ).commit(
      isFrozen = false,
      AssignmentConsistencyMode.Affinity,
      3 ## 42
    )

    assert(
      testDriver
        .onEvent(
          tickerTime,
          instant,
          Event.WatchRequest(
            ClientRequest(
              fatallyMismatchedTarget,
              SyncAssignmentState.KnownAssignment(assignment1),
              "another-client",
              5.seconds,
              ClerkData,
              supportsSerializedAssignment = true
            )
          )
        )
        .actions
        .isEmpty
    )

    // On the other hand, sending the same assignment with a matching target should be incorporated.
    assert(
      testDriver
        .onEvent(
          tickerTime,
          instant,
          Event.WatchRequest(
            ClientRequest(
              target,
              SyncAssignmentState.KnownAssignment(assignment1),
              "another-client",
              5.seconds,
              ClerkData,
              supportsSerializedAssignment = true
            )
          )
        )
        .actions == Seq(DriverAction.UseAssignment(assignment1))
    )

    // Sending a newer assignment for a target with the same name but different cluster a (non-fatal
    // mismatch) should also be incorporated.
    val assignment2: Assignment = ProposedAssignment(
      predecessorOpt = None,
      TestSliceUtils.createProposal(
        ("" -- ∞) -> Seq("Pod3")
      )
    ).commit(
      isFrozen = false,
      AssignmentConsistencyMode.Affinity,
      3 ## 43
    )

    val uri2: URI = URI.create("kubernetes-cluster:test-env/cloud1/public/region1/clustertype1/kjfna2")
    val nonFatalMismatchedTarget = Target.createKubernetesTarget(uri2, getSafeName)
    assert(
      testDriver
        .onEvent(
          tickerTime,
          instant,
          Event.WatchRequest(
            ClientRequest(
              nonFatalMismatchedTarget,
              SyncAssignmentState.KnownAssignment(assignment2),
              "another-client",
              5.seconds,
              ClerkData,
              supportsSerializedAssignment = true
            )
          )
        )
        .actions == Seq(DriverAction.UseAssignment(assignment2))
    )
  }
}
