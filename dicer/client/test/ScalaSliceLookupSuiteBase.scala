package com.databricks.dicer.client

import com.databricks.caching.util.TestUtils
import com.databricks.api.proto.dicer.common.ClientRequestP.SliceletDataP
import com.databricks.caching.util.{
  AssertionWaiter,
  Cancellable,
  FakeSequentialExecutionContext,
  FakeTypedClock,
  LoggingStreamCallback,
  MetricUtils,
  SequentialExecutionContext,
  StreamCallback,
  TestUtils
}
import com.databricks.dicer.common.SliceletData.{KeyLoad, SliceLoad}
import com.databricks.dicer.common.{
  Assignment,
  ClientType,
  ClerkData,
  ProposedSliceAssignment,
  SliceletData,
  Squid,
  TestAssigner
}
import com.databricks.dicer.common.TestSliceUtils.{createTestSquid, sampleProposal}
import com.databricks.dicer.external.{Slice, SliceKey}
import com.databricks.dicer.friend.SliceMap
import io.grpc.Status
import io.prometheus.client.CollectorRegistry
import java.time.Instant

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/**
 * Includes the common test cases from [[SliceLookupSuiteBase]] plus test cases that apply to the
 * Scala implementation but not the Rust one.
 */
abstract class ScalaSliceLookupSuiteBase(watchFromDataPlane: Boolean)
    extends SliceLookupSuiteBase(watchFromDataPlane) {

  /** Creates a test DicerClientProtoLogger using the given SEC and config. */
  private def createTestLogger(
      sec: SequentialExecutionContext,
      config: InternalClientConfig): DicerClientProtoLogger = {
    DicerClientProtoLogger.create(
      clientType = config.clientType,
      subscriberDebugName = config.subscriberDebugName,
      keySampleFraction = config.assignmentLatencySampleFraction,
      executor = sec
    )
  }

  override protected def withLookup(
      testAssigner: TestAssigner,
      watchStubCacheTime: FiniteDuration = 20.seconds,
      assignmentLatencySampleFraction: Double = 0)(
      func: (SliceLookupDriver, LoggingStreamCallback[Assignment]) => Unit): Unit = {
    fakeS2SProxy.setFallbackUpstreamPorts(Vector(testAssigner.localUri.getPort))
    val config: InternalClientConfig =
      createInternalClientConfig(
        ClientType.Clerk,
        debugName = "test-clerk",
        portToConnectTo(testAssigner),
        watchStubCacheTime,
        assignmentLatencySampleFraction
      )
    val lookup =
      SliceLookup.createUnstarted(
        sec,
        config,
        () => ClerkData,
        createTestLogger(sec, config),
        serviceBuilderOpt = None
      )
    val callback = new LoggingStreamCallback[Assignment](sec)
    val watchHandle: Cancellable =
      lookup.cellConsumer.watch(new StreamCallback[Assignment](sec) {
        override protected def onFailure(status: Status): Unit = callback.executeOnFailure(status)
        override protected def onSuccess(assignment: Assignment): Unit = {
          callback.executeOnSuccess(assignment)
        }
      })
    lookup.start()
    try {
      func(new ScalaSliceLookupDriver(lookup), callback)
    } finally {
      lookup.cancel()
      watchHandle.cancel(Status.CANCELLED.withDescription("cleaning up after withLookup"))
    }
  }

  override protected def createUnstartedSliceLookup(
      testAssigner: TestAssigner,
      clientType: ClientType,
      watchStubCacheTime: FiniteDuration = 20.seconds,
      sec: SequentialExecutionContext = sec): SliceLookupDriver = {
    val config: InternalClientConfig =
      createInternalClientConfig(
        clientType,
        debugName = "test-clerk",
        portToConnectTo(testAssigner),
        watchStubCacheTime
      )
    val lookup =
      SliceLookup.createUnstarted(
        sec,
        config,
        () => ClerkData,
        createTestLogger(sec, config),
        serviceBuilderOpt = None
      )
    new ScalaSliceLookupDriver(lookup)
  }

  override protected def readPrometheusMetric(
      metricName: String,
      labels: Vector[(String, String)]): Double = {
    MetricUtils.getMetricValue(CollectorRegistry.defaultRegistry, metricName, labels.toMap)
  }

  // This test case is not exercised for Rust, because the Rust implementation currently (as of
  // March 2025) does not have z-pages.
  test("getSlicezData returns appropriate watch address") {
    // Test plan: Verify that the `watchAddress` from `lookup.getSlicezData` works as expected with
    // and without EDS. Without EDS, it should be the address of the Assigner. With EDS, it should
    // indicate EDS is enabled with a `eds://` address. But when the lookup is redirected, it should
    // show the redirected address.
    val assigner1: TestAssigner = multiAssignerTestEnv.testAssigners(0)

    withLookup(assigner1) { (lookup: SliceLookupDriver, _: LoggingStreamCallback[Assignment]) =>
      val data: ClientTargetSlicezData = lookup.getSlicezData
      val scheme: String = if (useSsl) "https" else "http"
      assert(
        data.watchAddress.toString.contains(s"$scheme://localhost:${portToConnectTo(assigner1)}")
      )
    }
  }

  // This test case is not exercised for Rust, because the Rust implementation currently (as of
  // March 2025) does not have z-pages.
  test("ClientTargetSlicezData is correctly generated based on the SliceLookup") {
    // Test plan: Ensure that the values of ClientTargetSlicezData are correctly generated from
    // SliceLookup. To verify this, populate the SliceLookup with clerk and slicelet data with
    // various load information, and check that the generated ClientTargetSlicezData is as
    // expected.
    val kubernetesNamespace: String = "kubernetesNamespace"

    // Use a fake clock so timestamps are deterministic.
    val fakeSec: FakeSequentialExecutionContext =
      FakeSequentialExecutionContext.create(
        s"fake-lookup-context-$getSafeName",
        Some(new FakeTypedClock)
      )

    val clerkDebugName: String = "clerk"
    val slicelet1DebugName: String = "slicelet1"
    val slicelet2DebugName: String = "slicelet2"

    val squid1: Squid = createTestSquid(slicelet1DebugName)
    val squid2: Squid = createTestSquid(slicelet2DebugName)

    // SliceletData for `squid1` with attributed loads and top keys.
    val slicelet1Data = SliceletData(
      squid1,
      SliceletDataP.State.RUNNING,
      kubernetesNamespace,
      attributedLoads = Vector(
        SliceLoad(
          primaryRateLoad = 100.0,
          windowLowInclusive = Instant.EPOCH,
          windowHighExclusive = Instant.EPOCH,
          slice = Slice.FULL,
          topKeys = Seq(KeyLoad(SliceKey.MIN, 100)),
          numReplicas = 1
        )
      ),
      unattributedLoadOpt = None
    )

    // SliceletData for `squid2` with only unattributed load and top keys.
    val slicelet2Data = SliceletData(
      squid2,
      SliceletDataP.State.RUNNING,
      kubernetesNamespace,
      attributedLoads = Vector.empty,
      unattributedLoadOpt = Some(
        SliceLoad(
          primaryRateLoad = 100.0,
          windowLowInclusive = Instant.EPOCH,
          windowHighExclusive = Instant.EPOCH,
          slice = Slice.FULL,
          topKeys = Seq(KeyLoad(SliceKey.MIN, 200)),
          numReplicas = 1
        )
      )
    )

    // Create SliceLookups for the clerk and slicelets.
    val clerkConfig: InternalClientConfig = createInternalClientConfig(
      ClientType.Clerk,
      clerkDebugName,
      singleAssignerTestEnv.getAssignerPort,
      watchStubCacheTime = 20.seconds
    )
    val clerkSliceLookup = SliceLookup.createUnstarted(
      fakeSec,
      clerkConfig,
      () => ClerkData,
      createTestLogger(fakeSec, clerkConfig),
      serviceBuilderOpt = None
    )
    val slicelet1Config: InternalClientConfig =
      clerkConfig.copy(clientType = ClientType.Slicelet, subscriberDebugName = slicelet1DebugName)
    val slicelet1SliceLookup = SliceLookup.createUnstarted(
      fakeSec,
      slicelet1Config,
      () => slicelet1Data,
      createTestLogger(fakeSec, slicelet1Config),
      serviceBuilderOpt = None
    )
    val slicelet2Config: InternalClientConfig =
      clerkConfig.copy(clientType = ClientType.Slicelet, subscriberDebugName = slicelet2DebugName)
    val slicelet2SliceLookup = SliceLookup.createUnstarted(
      fakeSec,
      slicelet2Config,
      () => slicelet2Data,
      createTestLogger(fakeSec, slicelet2Config),
      serviceBuilderOpt = None
    )
    // Inject the assignments into the slicelet2's SliceLookup.
    val proposal: SliceMap[ProposedSliceAssignment] = sampleProposal()
    val assignment: Assignment =
      TestUtils.awaitResult(
        singleAssignerTestEnv.setAndFreezeAssignment(target, proposal),
        Duration.Inf
      )
    slicelet2SliceLookup.forTest.injectAssignment(assignment)

    // Verify that the generated ClientTargetSlicezData is as expected.
    val clerkSlicezData: ClientTargetSlicezData =
      TestUtils.awaitResult(clerkSliceLookup.getSlicezData, Duration.Inf)
    val slicelet1SlicezData: ClientTargetSlicezData =
      TestUtils.awaitResult(slicelet1SliceLookup.getSlicezData, Duration.Inf)
    val slicelet2SlicezData: ClientTargetSlicezData =
      TestUtils.awaitResult(slicelet2SliceLookup.getSlicezData, Duration.Inf)

    val expectedClerkSlicezData = ClientTargetSlicezData(
      target,
      sliceletsData = ArrayBuffer.empty,
      clerksData = ArrayBuffer.empty,
      assignmentOpt = None,
      reportedLoadPerResourceOpt = None,
      reportedLoadPerSliceOpt = None,
      topKeysOpt = None,
      squidOpt = None,
      unattributedLoadBySliceOpt = None,
      subscriberDebugName = clerkDebugName,
      watchAddress = clerkConfig.watchAddress,
      watchAddressUsedSince = fakeSec.getClock.instant(),
      lastSuccessfulHeartbeat = Instant.EPOCH
    )
    val expectedSlicelet1SlicezData: ClientTargetSlicezData =
      expectedClerkSlicezData.copy(
        subscriberDebugName = slicelet1DebugName,
        reportedLoadPerResourceOpt = Some(Map((squid1, 100.0))),
        reportedLoadPerSliceOpt = Some(Map((Slice.FULL, 100.0))),
        unattributedLoadBySliceOpt = Some(Map.empty),
        squidOpt = Some(squid1),
        topKeysOpt = Some(SortedMap((SliceKey.MIN, 100.0)))
      )
    val expectedSlicelet2SlicezData: ClientTargetSlicezData =
      expectedClerkSlicezData.copy(
        assignmentOpt = Some(assignment),
        subscriberDebugName = slicelet2DebugName,
        reportedLoadPerResourceOpt = Some(Map((squid2, 0.0))),
        reportedLoadPerSliceOpt = Some(Map.empty),
        unattributedLoadBySliceOpt = Some(Map((Slice.FULL, 100.0))),
        squidOpt = Some(squid2),
        topKeysOpt = Some(SortedMap((SliceKey.MIN, 200.0)))
      )

    assertResult(expectedClerkSlicezData)(clerkSlicezData)
    assertResult(expectedSlicelet1SlicezData)(slicelet1SlicezData)
    assertResult(expectedSlicelet2SlicezData)(slicelet2SlicezData)
  }

  // This test case is not exercised for Rust, because the Rust implementation currently (as of
  // Oct 2025) does not have z-pages.
  test("SliceLookup ClientSlicez.register") {
    // Test plan: Verify that SliceLookup correctly registers to the ClientSlicez upon start, and
    // unregisters upon cancel. Verify it by creating an unstarted SliceLookup, checking it's not
    // registered, starting it and checking it's registered, then canceling it and checking it's
    // unregistered.

    // Setup: Create an unstarted SliceLookup with `subscriberDebugName`.
    val subscriberDebugName: String = "slice-lookup-client-slicez-register-test"
    val config: InternalClientConfig = createInternalClientConfig(
      ClientType.Clerk,
      debugName = subscriberDebugName,
      portToConnectTo(singleAssignerTestEnv.testAssigner),
      watchStubCacheTime = 20.seconds
    )
    val lookup: SliceLookup = SliceLookup.createUnstarted(
      sec,
      config,
      () => ClerkData,
      createTestLogger(sec, config),
      serviceBuilderOpt = None
    )

    // Verify: Before starting, the lookup should not be registered in ClientSlicez.
    val dataBefore: Seq[ClientTargetSlicezData] =
      TestUtils.awaitResult(ClientSlicez.forTest.getData, Duration.Inf)
    assert(
      !dataBefore.exists((_: ClientTargetSlicezData).subscriberDebugName == subscriberDebugName)
    )

    // Setup: Start the lookup to trigger registration.
    lookup.start()

    // Verify: After starting, the lookup should be registered in ClientSlicez.
    AssertionWaiter("Wait for the lookup to be registered").await {
      val dataAfterStart: Seq[ClientTargetSlicezData] =
        TestUtils.awaitResult(ClientSlicez.forTest.getData, Duration.Inf)
      assert(
        dataAfterStart.exists(
          (_: ClientTargetSlicezData).subscriberDebugName == subscriberDebugName
        )
      )
    }

    // Setup: Cancel the lookup to trigger unregistration.
    lookup.cancel()

    // Verify: After canceling, the lookup should be unregistered from ClientSlicez.
    AssertionWaiter("Wait for the lookup to be unregistered").await {
      val dataAfterCancel: Seq[ClientTargetSlicezData] =
        TestUtils.awaitResult(ClientSlicez.forTest.getData, Duration.Inf)
      assert(
        !dataAfterCancel
          .exists((_: ClientTargetSlicezData).subscriberDebugName == subscriberDebugName)
      )
    }
  }
}
