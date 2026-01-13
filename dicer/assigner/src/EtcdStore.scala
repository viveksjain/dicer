package com.databricks.dicer.assigner

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Random, Try}

import io.grpc.Status
import io.prometheus.client.Counter

import com.databricks.caching.util.AssertMacros.iassert
import com.databricks.caching.util.{
  CachingErrorCode,
  Cancellable,
  PrefixLogger,
  SequentialExecutionContext,
  Severity,
  StateMachineDriver,
  StreamCallback,
  ValueStreamCallback
}
import com.databricks.dicer.assigner.Store.WriteAssignmentResult
import com.databricks.dicer.common.Assignment.AssignmentValueCell
import com.databricks.dicer.common.TargetHelper.TargetOps
import com.databricks.dicer.common.{Assignment, Incarnation, ProposedAssignment}
import com.databricks.dicer.external.Target
import com.databricks.caching.util.EtcdClient
import com.databricks.caching.util.EtcdClient.{WatchArgs, WriteResponse}

/**
 * REQUIRES: `storeIncarnation` is non-loose.
 * REQUIRES: 0 < minWatchRetryDelay < maxWatchRetryDelay < 5 minutes
 *
 * Configuration for EtcdStore.
 *
 * @param storeIncarnation The store incarnation to use for assignment writes.
 * @param minWatchRetryDelay Minimum retry delay for Etcd watch exponential backoff.
 * @param maxWatchRetryDelay Maximum retry delay for Etcd watch exponential backoff.
 */
case class EtcdStoreConfig(
    storeIncarnation: Incarnation,
    minWatchRetryDelay: FiniteDuration,
    maxWatchRetryDelay: FiniteDuration) {
  require(storeIncarnation.isNonLoose, "Store incarnation must be non-loose")
  require(Duration.Zero < minWatchRetryDelay)
  require(minWatchRetryDelay < maxWatchRetryDelay)
  require(maxWatchRetryDelay < 5.minutes)
}

object EtcdStoreConfig {

  /** Returns a production config for the given `storeIncarnation`. */
  def create(storeIncarnation: Incarnation): EtcdStoreConfig = {
    EtcdStoreConfig(
      storeIncarnation,
      minWatchRetryDelay = 5.seconds,
      maxWatchRetryDelay = 30.seconds
    )
  }
}

/**
 * REQUIRES: The `client` must have an infinite watch duration. (The only cancellations expected by
 * the etcd store are ones it initiates, and in that case the store does not re-establish the
 * watch.)
 *
 * Store that persists assignments to Etcd via [[EtcdClient]]. The assignments in the store are
 * durable and have non-loose incarnation numbers (see [[Incarnation.isNonLoose]]). Supports
 * "watching", "informing" and "writing" assignments to ONLY the store incarnation configured on
 * `client`.
 *
 * The store uses [[sec]] to protect internal state maintained.
 *
 * Implementation note:
 *
 * Writes performed by [[EtcdStore]] are incremental in Etcd i.e. only diff with predecessor is
 * written to Etcd in most cases and we periodically write full assignments when any of the below
 * conditions are satisfied:
 *  - Number of slices in Etcd for the current assignment is greater than a multiple of slices in
 *    the proposal SliceMap.
 *  - The proposal predecessor is ahead of the current slice history in cache ie. our slice history
 *    is stale and we cannot rely upon it.
 *  - The slice history in the cache is unknown.
 *
 * Writing full assignments periodically limits the history size and ensures the watchers for the
 * target can catch up on the latest assignment quickly. For more details, see [[EtcdClient.write]].
 */
class EtcdStore private (
    sec: SequentialExecutionContext,
    client: EtcdClient,
    config: EtcdStoreConfig,
    random: Random)
    extends Store {
  import EtcdStore.Metrics
  import EtcdStoreStateMachine.{DriverAction, EtcdWriteRequest, Event}
  private val logger = PrefixLogger.create(this.getClass, "")

  override val storeIncarnation: Incarnation = config.storeIncarnation

  /**
   * Target to assignment cell mapping. Cell contains the latest assignment from the store state
   * machine. It is updated in response to [[DriverAction.UseAssignment]].
   */
  private val targetCellMap = mutable.Map[Target, AssignmentValueCell]()

  /** Generic driver implementation that drives the [[EtcdStoreStateMachine]]. */
  private val baseDriver =
    new StateMachineDriver[
      EtcdStoreStateMachine.Event,
      EtcdStoreStateMachine.DriverAction,
      EtcdStoreStateMachine](
      sec,
      new EtcdStoreStateMachine(random, config),
      performAction
    )

  /** Handle for EtcdClient watch, used for cancellation. */
  private val targetWatchHandles = mutable.Map[Target, Cancellable]()

  override def watchAssignments(
      target: Target,
      callback: ValueStreamCallback[Assignment]): Cancellable = sec.callCancellable {
    val targetCell = targetCellMap.getOrElseUpdate(target, new AssignmentValueCell)

    // Inform the state machine about the new watch.
    baseDriver.handleEvent(Event.EnsureTargetTracked(target))

    targetCell.watch(callback)
  }

  override def writeAssignment(
      target: Target,
      shouldFreeze: Boolean,
      proposal: ProposedAssignment): Future[WriteAssignmentResult] = sec.flatCall {
    // The predecessor must be either empty or have the same store incarnation as the store.
    val promise = Promise[Store.WriteAssignmentResult]()
    baseDriver.handleEvent(
      Event.WriteRequest(target, promise, shouldFreeze, proposal)
    )
    promise.future
  }

  override def getLatestKnownAssignment(target: Target): Future[Option[Assignment]] =
    sec.call {
      targetCellMap.get(target).flatMap { targetCell: AssignmentValueCell =>
        targetCell.getLatestValueOpt
      }
    }

  /**
   * All assignments distributed from the store [[storeIncarnation]] should have been persisted to
   * the store, so we don't expect to learn anything new from trusted sources. However, in the case
   * when Etcd instance is unavailable or corrupted informAssignment serves as useful mechanism to
   * learn and serve the last known assignment generated by the store.
   */
  override def informAssignment(target: Target, assignment: Assignment): Unit =
    sec.run {
      baseDriver.handleEvent(
        Event.InformAssignmentRequest(target, assignment)
      )
    }

  /** Starts the driver. */
  private def start(): Unit = {
    sec.assertCurrentContext()
    baseDriver.start()
  }

  /** Performs an action requested of this driver by the [[EtcdStoreStateMachine]]. */
  private def performAction(action: DriverAction): Unit = {
    sec.assertCurrentContext()

    action match {
      case DriverAction.UseAssignment(target: Target, assignment: Assignment) =>
        // Update the assignment for distribution. Create the assignment cell if one does not exist
        // it's possible we don't have any watchers yet.
        targetCellMap.getOrElseUpdate(target, new AssignmentValueCell).setValue(assignment)
      case DriverAction.PerformEtcdWrite(target, request) =>
        handlePerformEtcdWrite(
          target,
          request
        )
      case DriverAction.PerformEtcdWatch(target: Target, watchDuration: Duration, pageLimit: Int) =>
        handlePerformEtcdWatch(target, watchDuration, pageLimit)

      case DriverAction.CompletedWrite(
          _: Target,
          promise: Promise[WriteAssignmentResult],
          response: Try[WriteAssignmentResult]
          ) =>
        // Complete the write operation promise.
        promise.complete(response)

      case DriverAction
            .CancelEtcdWatch(target: Target, reason: EtcdStoreStateMachine.StoreErrorReason) =>
        Metrics.incrementNumEtcdStoreCorruptions(target, reason.code)
        logger.alert(
          Severity.CRITICAL,
          CachingErrorCode.ETCD_ASSIGNMENT_STORE_CORRUPTION,
          s"Store corruption for $target due to ${reason.code.toString}."
        )

        // Cancelling the watch for target, will cause a watch failure and the state machine will
        // re-establish watch with backoff. This enables the store to automatically recover from
        // data corruption after manual intervention.

        // If the watch is already cancelled, we may not have a watch handle entry.
        if (targetWatchHandles.contains(target)) {
          targetWatchHandles(target).cancel(reason.status)
        }
    }
  }

  /** Starts [[EtcdClient.watch()]] for the given target. */
  private def handlePerformEtcdWatch(
      target: Target,
      watchDuration: Duration,
      pageLimit: Int): Cancellable = {
    // Create a new watch handle for the target, we expect to create a new watch only if a watch for
    // the target does not already exist OR the previous watch failed.
    iassert(
      !targetWatchHandles.contains(target),
      s"Etcd watch for $target already exists"
    )

    targetWatchHandles.getOrElseUpdate(
      target, {
        client.watch(
          WatchArgs(target.toParseableDescription, watchDuration = watchDuration),
          new StreamCallback[EtcdClient.WatchEvent](sec) {

            /** Handle [[EtcdClient.WatchEvent]] . */
            override protected def onSuccess(value: EtcdClient.WatchEvent): Unit = {
              baseDriver.handleEvent(Event.EtcdWatchEvent(target, value))
            }

            /** Handle terminal failures. */
            override protected def onFailure(status: Status): Unit = {
              targetWatchHandles.remove(target)
              baseDriver.handleEvent(Event.EtcdWatchFailure(target, status))
            }
          }
        )
      }
    )
  }

  /**
   * Performs write operation using [[client]].
   *
   * @param target Target for the assignment write operation.
   * @param request Handle for the store write operation.
   */
  private def handlePerformEtcdWrite(target: Target, request: EtcdWriteRequest): Unit = {
    logger.info(
      s"Writing to etcd: target=$target version=${request.version} " +
      s"previousVersion=${request.previousVersionOpt}."
    )
    val future: Future[WriteResponse] =
      client.write(
        target.toParseableDescription,
        request.version,
        request.value,
        request.previousVersionOpt,
        request.isIncremental
      )

    // Inform the state machine about the response from Etcd for the handle.
    future.onComplete { response =>
      baseDriver.handleEvent(Event.EtcdWriteResponse(target, request, response))
    }(sec)
  }

  object forTest {

    /** Cancel the etcd watch for all targets. */
    def cleanup(): Future[Unit] = sec.call {
      sec.assertCurrentContext()
      for ((target, watchHandle) <- targetWatchHandles) {
        watchHandle.cancel(Status.CANCELLED)
        targetWatchHandles.remove(target)
      }
    }
  }
}

object EtcdStore {

  object Metrics {
    @SuppressWarnings(
      Array(
        "BadMethodCall-PrometheusCounterNamingConvention",
        "reason: Renaming existing prod metric would break dashboards and alerts"
      )
    )
    private val numEtcdStoreCorruptions: Counter = Counter
      .build()
      .name("dicer_assigner_num_etcd_store_corruptions")
      .labelNames("targetCluster", "targetName", "targetInstanceId", "errorCode")
      .help("Number of data corruption incidents in EtcdStore for a target")
      .register()

    def incrementNumEtcdStoreCorruptions(
        target: Target,
        errorCode: EtcdStoreStateMachine.StoreErrorCode.Value): Unit = {
      numEtcdStoreCorruptions
        .labels(
          target.getTargetClusterLabel,
          target.getTargetNameLabel,
          target.getTargetInstanceIdLabel,
          errorCode.toString
        )
        .inc()
    }

    object forTest {
      def getNumEtcdStoreCorruptions(
          target: Target,
          errorCode: EtcdStoreStateMachine.StoreErrorCode.Value): Double = {
        numEtcdStoreCorruptions
          .labels(
            target.getTargetClusterLabel,
            target.getTargetNameLabel,
            target.getTargetInstanceIdLabel,
            errorCode.toString
          )
          .get()
      }
    }
  }

  /**
   * Creates [[EtcdStore]].
   *
   * @param sec [[SequentialExecutionContext]] used to protected the store state.
   * @param client [[EtcdClient]] for performing Etcd store operations
   * @param config Configuration for the store, see [[EtcdStoreConfig]].
   * @param random Random used for jitter backoff in retries.
   */
  def create(
      sec: SequentialExecutionContext,
      client: EtcdClient,
      config: EtcdStoreConfig,
      random: Random): EtcdStore = {
    val driver = new EtcdStore(sec, client, config, random)
    sec.run {
      driver.start()
    }
    driver
  }
}
