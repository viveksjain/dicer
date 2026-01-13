package com.databricks.dicer.assigner

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Random
import io.grpc.Status
import com.databricks.caching.util.{
  AssertionWaiter,
  LoggingStreamCallback,
  SequentialExecutionContext,
  StatusOr,
  TestUtils
}
import com.databricks.caching.util.{EtcdTestEnvironment, EtcdClient}
import com.databricks.dicer.common.{
  Assignment,
  AssignmentConsistencyMode,
  Generation,
  Incarnation,
  ProposedAssignment
}
import com.databricks.dicer.external.Target
import com.databricks.caching.util.TestUtils

/** Shadow [[EtcdStore]] should behave like [[InMemoryStore]] so we run the same common tests. */
class ShadowEtcdStoreSuite extends CommonStoreSuiteBase {
  private val etcd = EtcdTestEnvironment.create()

  private val ETCD_NAMESPACE = EtcdClient.KeyNamespace("test-namespace")

  override def afterAll(): Unit = {
    etcd.close()
  }

  override def beforeEach(): Unit = {
    etcd.deleteAll()
  }

  /**
   * Creates an etcd client and store connecting to [[etcd]]. If no incarnation is given, chooses an
   * incarnation that is unique to avoid interference between tests.
   */
  private def createClientAndStore(
      sec: SequentialExecutionContext,
      etcdStoreIncarnation: Incarnation = Incarnation(2)
  ): (EtcdClient, ShadowEtcdStore) = {
    // Create a client with a unique incarnation to avoid interference with other tests.
    val etcdClient: EtcdClient =
      etcd.createEtcdClientAndInitializeStore(EtcdClient.Config(ETCD_NAMESPACE))
    val shadowEtcdStore = new ShadowEtcdStore(
      sec,
      etcdClient,
      etcdStoreIncarnation = etcdStoreIncarnation,
      inMemoryStoreIncarnation = Incarnation.MIN
    )
    (etcdClient, shadowEtcdStore)
  }

  override def createStore(sec: SequentialExecutionContext): ShadowEtcdStore = {
    val (_, store): (EtcdClient, ShadowEtcdStore) = createClientAndStore(sec)
    store
  }

  /** No cleanup of resources for InMemoryStore. */
  override def destroyStore(store: Store): Unit = {
    store match {
      case store: ShadowEtcdStore => TestUtils.awaitResult(store.forTest.cleanup(), Duration.Inf)
      case store: EtcdStore => TestUtils.awaitResult(store.forTest.cleanup(), Duration.Inf)
      case _ => fail("Expected ShadowEtcdStore or EtcdStore")
    }
  }

  test("Write an assignment to the store and expect it be written to Etcd") {
    // Test plan: Perform first write using ShadowEtcdStore for two targets and verify the
    // assignment is visible in EtcdStore eventually. Perform another write with predecessor and
    // verify the assignment is written to EtcdStore eventually.
    val store: ShadowEtcdStore = createStore(pool.createExecutionContext(getSafeName))

    // EtcdStore used under the hood.
    val etcdStore = store.forTest.etcdStore
    val target1 = Target("target1")
    val target2 = Target("target2")

    // Perform a write for target1 and wait for it to be written to ShadowEtcdStore, including the
    // underlying EtcdStore. Note that we wait for the write to target1 to complete before starting
    // the write to target2, otherwise EtcdStore may not update its lower bound on initial
    // assignment incarnations before beginning the write for target2, resulting in a failure.
    // TODO(<internal bug>): Implement retries in EtcdStore to avoid this.
    val proposal1: ProposedAssignment = createProposal(predecessorOpt = None)
    // Use this forTest method to write to the shadow etcd store as we'll need the extra etcd write
    // result for sequencing later.
    val shadowAssignmentWriteResult1: ShadowEtcdStore.ShadowAssignmentWriteResult =
      store.forTest.writeAssignmentToInMemoryAndEtcdStore(
        target1,
        shouldFreeze = false,
        proposal1
      )

    val assignment1: Assignment =
      awaitCommitted(shadowAssignmentWriteResult1.inMemoryStoreWriteResult)
    assert(
      assignment1 == proposal1
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          assignment1.generation
        )
    )
    assert(awaitAssignment(store, target1) == assignment1)

    val watchedEtcdAssignment1: Assignment = awaitAssignment(etcdStore, target1, Generation.EMPTY)
    // Await for the shadow write to etcd store to finish. This is needed because getting an
    // assignment by watching the etcd store doesn't guarantee the previous write has returned and
    // new high version watermark is learned by the etcd store client.
    val committedEtcdAssignment1: Assignment =
      awaitCommitted(shadowAssignmentWriteResult1.etcdStoreWriteResult)
    // Sanity check: The committed etcd assignment should be the same as the watched one.
    assert(committedEtcdAssignment1 == watchedEtcdAssignment1)
    assert(
      watchedEtcdAssignment1 == proposal1
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          watchedEtcdAssignment1.generation
        )
    )

    // Perform a write for target2 and wait for it to be written to ShadowEtcdStore, including the
    // underlying EtcdStore.
    val proposal2: ProposedAssignment = createProposal(predecessorOpt = None)
    val assignment2: Assignment =
      awaitCommitted(
        store.writeAssignment(target2, shouldFreeze = true, proposal2)
      )
    assert(
      assignment2 == proposal2
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          assignment2.generation
        )
    )
    val etcdAssignment2 = awaitAssignment(etcdStore, target2, Generation.EMPTY)
    assert(
      etcdAssignment2 == proposal2
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          etcdAssignment2.generation
        )
    )

    // Perform another write for target1 with assignment1 as predecessor.
    val proposal3: ProposedAssignment =
      createProposal(predecessorOpt = Some(assignment1))
    val assignment3: Assignment =
      awaitCommitted(
        store.writeAssignment(target1, shouldFreeze = false, proposal3)
      )
    assert(
      assignment3 == proposal3
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          assignment3.generation
        )
    )

    // Wait for the new assignment in EtcdStore.
    val etcdAssignment3 = awaitAssignment(etcdStore, target1, watchedEtcdAssignment1.generation)

    // The new assignment should use watchedEtcdAssignment1 as the predecessor.
    assert(
      etcdAssignment3 == proposal3
        .copy(predecessorOpt = Some(watchedEtcdAssignment1))
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          etcdAssignment3.generation
        )
    )
    destroyStore(store)
  }

  test("Inform assignment with existing assignment in EtcdStore") {
    // Test plan: Perform few writes for target, and start ShadowEtcdStore.
    // - Inform assignment and expect it to be visible in underlying InMemoryStore but
    //   not in ShadowEtcdStore.
    val (client, store): (EtcdClient, ShadowEtcdStore) =
      createClientAndStore(pool.createExecutionContext(getSafeName))

    // EtcdStore using same store incarnation as EtcdStore underlying ShadowEtcdStore. Used for
    // writing initial assignment for the target.
    val etcdStore2 =
      EtcdStore.create(
        pool.createExecutionContext(getSafeName),
        client,
        EtcdStoreConfig.create(Incarnation(2)),
        new Random()
      )

    // Underlying EtcdStore for the store.
    val etcdStore1 = store.forTest.etcdStore
    val target = Target("target")

    // Perform a write for target using EtcdStore
    val proposal1: ProposedAssignment = createProposal(predecessorOpt = None)
    val assignment1: Assignment =
      awaitCommitted(
        etcdStore2
          .writeAssignment(target, shouldFreeze = false, proposal1)
      )
    assert(
      assignment1 == proposal1
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          assignment1.generation
        )
    )

    // Perform another write for target using EtcdStore.
    val proposal2: ProposedAssignment =
      createProposal(predecessorOpt = Some(assignment1))
    val assignment2: Assignment =
      awaitCommitted(
        etcdStore2
          .writeAssignment(target, shouldFreeze = true, proposal2)
      )
    assert(
      assignment2 == proposal2
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          assignment2.generation
        )
    )

    // Inform the ShadowEtcdStore about a new assignment from a loose incarnation.
    val proposal3 = createProposal(predecessorOpt = None)
    val assignment3 = proposal3.commit(
      isFrozen = true,
      AssignmentConsistencyMode.Affinity,
      Generation(store.storeIncarnation, Instant.now().toEpochMilli)
    )
    store.informAssignment(target, assignment3)

    // ShadowEtcdStore should learn about assignment3.
    AssertionWaiter("Wait for assignment3 to visible in ShadowEtcdStore").await {
      assert(getLatestKnownAssignment(store, target).contains(assignment3))
    }

    // Underlying EtcdStore should learn about assignment2. assignment3 should be rejected because
    // it belongs to another incarnation.
    AssertionWaiter("Wait for assignment2 to be visible in underlying EtcdStore").await {
      assert(getLatestKnownAssignment(etcdStore1, target).contains(assignment2))
    }

    destroyStore(etcdStore2)
    destroyStore(store)
  }

  test("Write assignment with existing assignment in EtcdStore") {
    // Test plan: Perform few writes for target, and start ShadowEtcdStore.
    // - Establish watch for target then perform another write, expect this write to be visible in
    //   ShadowEtcdStore and underlying EtcdStore.
    val (client, store): (EtcdClient, ShadowEtcdStore) =
      createClientAndStore(pool.createExecutionContext(getSafeName))

    // EtcdStore using same incarnation as EtcdStore underlying ShadowEtcdStore. Used for writing
    // initial assignment for the target.
    val etcdStore =
      EtcdStore.create(
        pool.createExecutionContext(getSafeName),
        client,
        EtcdStoreConfig.create(Incarnation(2)),
        new Random()
      )

    // Underlying EtcdStore for the store.
    val underlyingShadowEtcdStore = store.forTest.etcdStore
    val target = Target("target")

    // Perform a write for target using EtcdStore
    val proposal1: ProposedAssignment = createProposal(predecessorOpt = None)
    val assignment1: Assignment =
      awaitCommitted(
        etcdStore
          .writeAssignment(target, shouldFreeze = false, proposal1)
      )
    assert(
      assignment1 == proposal1
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          assignment1.generation
        )
    )

    // Perform another write for target using EtcdStore.
    val proposal2: ProposedAssignment =
      createProposal(predecessorOpt = Some(assignment1))
    val assignment2: Assignment =
      awaitCommitted(
        etcdStore
          .writeAssignment(target, shouldFreeze = true, proposal2)
      )
    assert(
      assignment2 == proposal2
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          assignment2.generation
        )
    )

    // Perform a write using ShadowEtcdStore and expect it be committed in InMemoryStore but fail
    // in underlying EtcdStore because EtcdStore does not have the latest assignment for the
    // target.
    val proposal3 = createProposal(predecessorOpt = None)
    val assignment3 = awaitCommitted(
      store.writeAssignment(target, shouldFreeze = true, proposal3)
    )

    // Underlying EtcdStore should learn about assignment2. assignment3 should be rejected because
    // it belongs to another incarnation.
    AssertionWaiter("Wait for assignment2 to be visible in underlying EtcdStore").await {
      assert(getLatestKnownAssignment(underlyingShadowEtcdStore, target).contains(assignment2))
    }

    // Establish a watch for the target and expect to learn about assignment4.
    val callback =
      new LoggingStreamCallback[Assignment](pool.createExecutionContext("callback"))
    val cancellable = store.watchAssignments(target, callback)
    val expectedLog = mutable.ArrayBuffer[StatusOr[Assignment]]()
    expectedLog.append(StatusOr.success(assignment3))
    AssertionWaiter("Wait for assignment3").await {
      assert(callback.getLog == expectedLog)
    }

    // Perform a write using ShadowEtcdStore and expect it be committed in InMemoryStore and
    // underlying EtcdStore.
    val proposal4 = createProposal(predecessorOpt = Some(assignment3))
    awaitCommitted(
      store.writeAssignment(target, shouldFreeze = true, proposal4)
    )

    // Latest known assignment in underlying EtcdStore is eventually same as proposal4.
    val etcdAssignment4 = awaitAssignment(underlyingShadowEtcdStore, target, assignment2.generation)
    assert(
      etcdAssignment4 == proposal4
        .copy(predecessorOpt = Some(assignment2))
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          etcdAssignment4.generation
        )
    )

    cancellable.cancel(Status.CANCELLED)

    destroyStore(etcdStore)
    destroyStore(store)
  }

  test("Write assignment with existing assignment in EtcdStore with different incarnation") {
    // Test plan: Verify that ShadowEtcdStore does not reject assignments if the latest known
    // assignment in the underlying EtcdStore has a different incarnation. Verify this by writing
    // assignments to the store with a different incarnation and verifying that the assignments are
    // visible in the store. Then, write an assignment to the store with the same incarnation as the
    // underlying EtcdStore and verify that the assignment is visible in the store.
    val (client, store): (EtcdClient, ShadowEtcdStore) =
      createClientAndStore(pool.createExecutionContext(getSafeName), Incarnation(4))

    // EtcdStore using lower incarnation than EtcdStore underlying ShadowEtcdStore. Used for writing
    // initial assignment for the target.
    val etcdStore =
      EtcdStore.create(
        pool.createExecutionContext(getSafeName),
        client,
        EtcdStoreConfig.create(Incarnation(2)),
        new Random()
      )

    // Underlying EtcdStore for the store.
    val underlyingShadowEtcdStore = store.forTest.etcdStore
    val target = Target("target")

    // Perform a write for target using EtcdStore
    val proposal1: ProposedAssignment = createProposal(predecessorOpt = None)
    val assignment1: Assignment =
      awaitCommitted(
        etcdStore
          .writeAssignment(target, shouldFreeze = false, proposal1)
      )
    assert(
      assignment1 == proposal1
        .commit(
          isFrozen = false,
          AssignmentConsistencyMode.Affinity,
          assignment1.generation
        )
    )

    // Perform a write using ShadowEtcdStore and expect it be committed in InMemoryStore but fail
    // in underlying EtcdStore because the store's high watermark is too low.
    val proposal2 = createProposal(predecessorOpt = None)
    // Note that we use the forTest version of `writeAssignment` so that we can get a Future
    // indicating the write result to the etcd store. We wait for this Future to complete to make
    // sure the high version watermark in etcd is actually increased before we try to write the next
    // assignment.
    val shadowAssignmentWriteResult: ShadowEtcdStore.ShadowAssignmentWriteResult =
      store.forTest.writeAssignmentToInMemoryAndEtcdStore(
        target,
        shouldFreeze = true,
        proposal2
      )
    val assignment2 = awaitCommitted(shadowAssignmentWriteResult.inMemoryStoreWriteResult)
    TestUtils.awaitReady(shadowAssignmentWriteResult.etcdStoreWriteResult, Duration.Inf)

    // Underlying EtcdStore should learn about assignment1. assignment2 should be rejected because
    // it belongs to another incarnation.
    AssertionWaiter("Wait for assignment1 to be visible in underlying EtcdStore").await {
      assert(getLatestKnownAssignment(underlyingShadowEtcdStore, target).contains(assignment1))
    }
    // Establish a watch for the target and expect to learn about assignment2.
    val callback =
      new LoggingStreamCallback[Assignment](pool.createExecutionContext("callback"))
    val cancellable = store.watchAssignments(target, callback)
    val expectedLog = mutable.ArrayBuffer[StatusOr[Assignment]]()
    expectedLog.append(StatusOr.success(assignment2))
    AssertionWaiter("Wait for assignment2").await {
      assert(callback.getLog == expectedLog)
    }

    // Perform a write using ShadowEtcdStore and expect it be committed in InMemoryStore and
    // underlying EtcdStore.
    val proposal3 = createProposal(predecessorOpt = Some(assignment2))
    awaitCommitted(
      store.writeAssignment(target, shouldFreeze = true, proposal3)
    )

    // Latest known assignment in underlying EtcdStore is eventually same as proposal3.
    val etcdAssignment3 = awaitAssignment(underlyingShadowEtcdStore, target, assignment1.generation)
    assert(
      etcdAssignment3 == proposal3
        .copy(predecessorOpt = Some(assignment1))
        .commit(
          isFrozen = true,
          AssignmentConsistencyMode.Affinity,
          etcdAssignment3.generation
        )
    )

    cancellable.cancel(Status.CANCELLED)

    destroyStore(etcdStore)
    destroyStore(store)
  }
}
