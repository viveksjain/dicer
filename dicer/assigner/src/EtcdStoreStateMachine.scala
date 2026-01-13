package com.databricks.dicer.assigner

import java.time.Instant

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

import io.grpc.{Status, StatusException}

import com.databricks.api.proto.dicer.common.DiffAssignmentP
import com.databricks.caching.util.{
  CachingErrorCode,
  ExponentialBackoff,
  PrefixLogger,
  Severity,
  StateMachine,
  StateMachineOutput,
  TickerTime
}
import com.google.protobuf.ByteString
import com.databricks.dicer.assigner.Store.WriteAssignmentResult
import com.databricks.dicer.common.Assignment.DiffUnused.DiffUnused
import com.databricks.dicer.common.TargetHelper.TargetOps
import com.databricks.dicer.common.{
  AssignmentConsistencyMode,
  Assignment,
  DiffAssignmentSliceMap,
  DiffAssignment,
  EtcdClientHelper,
  Generation,
  ProposedAssignment,
  SliceAssignment
}
import com.databricks.dicer.external.Target
import com.databricks.dicer.friend.SliceMap
import com.databricks.dicer.friend.SliceMap.GapEntry
import com.databricks.caching.util.EtcdClient.WriteResponse.KeyState
import com.databricks.caching.util.EtcdClient.{Version, WatchEvent, WriteResponse}
import com.databricks.caching.util.UnixTimeVersion
import scala.concurrent.duration.Duration

/**
 * State machine for [[EtcdStore]]. Maintains the latest assignment for each target based on
 * incoming signals, e.g. write requests, watch events from Etcd.
 *
 * The state machine is exercised only via [[onEvent]] and [[onAdvance]]. The driver, when started,
 * calls [[onAdvance]] with the initial time.
 *
 * @param random Random for exponential backoff jitter.
 * @param config [[EtcdStore]] configuration.
 */
private[assigner] class EtcdStoreStateMachine(random: Random, config: EtcdStoreConfig)
    extends StateMachine[EtcdStoreStateMachine.Event, EtcdStoreStateMachine.DriverAction] {
  import EtcdStoreStateMachine._

  /**
   * An exclusive lower bound on the generation to use for initial assignments. Updated by callbacks
   * from [[EtcdClient]].
   *
   * Since the store may only issue writes for its store incarnation, this is initialized to the
   * minimum value within this `config.storeIncarnation`, and writes maintain the invariant that
   * this generation always increases and stays within the store incarnation.
   */
  private var initialAssignmentGenerationLowerBoundExclusive: Generation =
    Generation(config.storeIncarnation, number = UnixTimeVersion.MIN)

  /** The store state that is specific to target, along side target specific event handlers. */
  private class TargetState(target: Target) {
    private val logger = PrefixLogger.create(getClass, target.getLoggerPrefix)

    /**
     * Latest assignment with slice history for the target. This is learnt from two sources -
     *  - Etcd watch events after receiving the causal event OR
     *  - Assignment write operations.
     */
    private var latestAssignmentHistory = AssignmentSliceHistory.EMPTY

    /** Tracks latest assignment and history, updated only from [[Event.EtcdWatchEvent]]. */
    private var watchState = WatchState.EMPTY

    /**
     * The next time an Etcd watch should be executed by the driver to Etcd. [[TickerTime.MAX]]
     * indicates a watch is already established.
     */
    private var nextWatchTime = TickerTime.MIN

    /**
     * Exponential backoff for watch retries. If a watch on Etcd fails with Status (other than
     * CANCELLED), we request the driver to establish another watch with backoff.
     *
     * A target-specific strategy is preferred over a backoff strategy spanning all targets for
     * simplicity. We also expect failures because of data corruption which may be isolated to
     * target, and target-specific watch retries work better.
     */
    private val watchBackoff =
      new ExponentialBackoff(random, config.minWatchRetryDelay, config.maxWatchRetryDelay)

    /** Handles watch events from Etcd. */
    def onEtcdWatchEvent(event: WatchEvent, outputBuilder: OutputBuilder): Unit = {
      // Reset the watch backoff on causal event. This means we schedule a watch with backoff until
      // we can read a valid assignment from the watch.
      event match {
        case WatchEvent.Causal => watchBackoff.reset()
        case _ =>
      }

      // Update the watch state after applying the event.
      watchState.applyEvent(event) match {
        case Left(value: WatchState) =>
          watchState = value

          // Update the latest assignment from the watch state. Since we also update latest
          // assignment on writes, we should already have the latest assignment in most cases.
          updateLatestAssignmentHistory(watchState.getLatestAssignmentHistory, outputBuilder)
        case Right(reason: StoreErrorReason) =>
          // Cancelling the watch for target, will cause a watch failure and the state machine will
          // re-establish watch with backoff.
          logger.info(s"Cancelling watch, reason: $reason")
          outputBuilder.appendAction(DriverAction.CancelEtcdWatch(target, reason))
      }
    }

    /**
     * Handles write response from Etcd.
     *
     * @param request The original request for the store write operation.
     * @param outputBuilder State machine output builder to build driver actions.
     */
    def onEtcdWriteResponse(
        request: EtcdWriteRequest,
        response: Try[WriteResponse],
        outputBuilder: OutputBuilder): Unit = {
      val assignment = request.assignment
      val predecessorGeneration: Generation = request.previousVersionOpt
        .map { version: Version =>
          EtcdClientHelper.createGenerationFromVersion(version)
        }
        .getOrElse(Generation.EMPTY)
      response match {
        case Success(
            WriteResponse.Committed(newKeyVersionLowerBoundExclusiveOpt: Option[Version])
            ) =>
          // Ask the driver to complete write operation with the committed assignment.
          logger.info(
            s"Write for assignment ${assignment.generation} based on predecessor " +
            s"$predecessorGeneration committed"
          )
          for (newKeyVersionLowerBoundExclusive: Version <- newKeyVersionLowerBoundExclusiveOpt) {
            updateInitialAssignmentGenerationLowerBound(newKeyVersionLowerBoundExclusive)
          }
          outputBuilder.appendAction(
            DriverAction.CompletedWrite(
              target,
              request.promise,
              Success(WriteAssignmentResult.Committed(assignment))
            )
          )

          // Update latest assignment with the assignment written.
          latestAssignmentHistory.applyDiff(request.diffAssignment) match {
            case Left(value: AssignmentSliceHistory) =>
              updateLatestAssignmentHistory(value, outputBuilder)
            case Right(reason: StoreErrorReason) =>
              // Cancelling the watch for target, will cause a watch failure and the state machine
              // will re-establish watch with backoff.
              logger.info(s"Cancelling watch, reason: $reason")
              outputBuilder.appendAction(DriverAction.CancelEtcdWatch(target, reason))
          }
        case Success(WriteResponse.OccFailure(keyState: KeyState)) =>
          val driverAction: DriverAction = keyState match {
            case KeyState
                  .Present(actualVersion: Version, newKeyVersionLowerBoundExclusive: Version) =>
              updateInitialAssignmentGenerationLowerBound(newKeyVersionLowerBoundExclusive)
              val actualGeneration: Generation =
                EtcdClientHelper.createGenerationFromVersion(actualVersion)
              expectStoreAheadOfAssignerKnownGeneration(
                storeGeneration = actualGeneration,
                assignerKnownGeneration = predecessorGeneration
              )
              if (predecessorGeneration == Generation.EMPTY
                && actualGeneration.incarnation < config.storeIncarnation) {
                // By Store's contract, writing with "no-predecessor" should succeed if the key
                // either doesn't exist or its latest version is from a lower incarnation. We've
                // found the second case here, so we don't yet complete the write. Instead, we
                // issue a retry internally with a modified predecessor to overwrite the existing
                // lower incarnation assignment.
                //
                // This could fail again in the same way if there's a concurrent writer in
                // the lower incarnation, but we should win quickly here since we're retrying
                // frequently, which will fence off future writes from that lower incarnation.
                val newWriteRequest: EtcdWriteRequest = request.copy(
                  previousVersionOpt = Some(actualVersion)
                )
                DriverAction.PerformEtcdWrite(target, newWriteRequest)
              } else {
                logger.info(
                  s"Write for assignment ${assignment.generation} based on predecessor " +
                  s"$predecessorGeneration failed OCC checks. Current assignment in the store is " +
                  s"$actualGeneration."
                )
                DriverAction.CompletedWrite(
                  target,
                  request.promise,
                  Success(WriteAssignmentResult.OccFailure(actualGeneration))
                )
              }
            case KeyState.Absent(newKeyVersionLowerBoundExclusive: Version) =>
              // No assignment currently exists in the store. The write may have failed due to
              // either an incorrect expectation about the presence of an assignment in the store,
              // or when the write was indeed for an initial assignment, use of an insufficiently
              // high version to guarantee monotonicity.
              updateInitialAssignmentGenerationLowerBound(newKeyVersionLowerBoundExclusive)
              expectStoreAheadOfAssignerKnownGeneration(
                storeGeneration = Generation.EMPTY,
                assignerKnownGeneration = predecessorGeneration
              )
              if (predecessorGeneration == Generation.EMPTY) {
                // The write was for an initial assignment, and there is indeed no assignment in the
                // store. The write must have failed due to an insufficiently high key version.

                // TODO(<internal bug>): Since choosing generations is the store's responsibility, we
                // should support retries in the store implementation in response to failures due to
                // poorly chosen incarnations.
                DriverAction.CompletedWrite(
                  target,
                  request.promise,
                  Failure(
                    new StatusException(
                      Status.INTERNAL.withDescription(
                        s"Write for initial assignment ${assignment.generation} " +
                        s"failed due to use of an insufficiently high version to guarantee " +
                        s"monotonicity. Initial assignment writes must currently have a" +
                        s" generation of at least $initialAssignmentGenerationLowerBoundExclusive."
                      )
                    )
                  )
                )
              } else {
                // This is unexpected; we've supplied a non-empty predecessor but found nothing in
                // the store. The call to `expectStoreAheadOfAssignerKnownGeneration` will have
                // fired an alert.
                DriverAction.CompletedWrite(
                  target,
                  request.promise,
                  Success(WriteAssignmentResult.OccFailure(Generation.EMPTY))
                )
              }
          }
          outputBuilder.appendAction(driverAction)
        case Failure(throwable) =>
          // Ask the driver to complete write operation with failure.
          logger.warn(s"Write for assignment ${assignment.generation} failed: $throwable")

          outputBuilder.appendAction(
            DriverAction.CompletedWrite(
              target,
              request.promise,
              Failure(throwable)
            )
          )
      }
    }

    /**
     * REQUIRES: Predecessor in the proposal is either empty or has a incarnation equal to the
     * store's configured incarnation.
     *
     * Handles write assignment request from the store and generates
     * [[DriverAction.PerformEtcdWrite]] for the write to be performed on Etcd. The generated Etcd
     * write operation is incremental, except for when the slice history gets long (see
     * [[FULL_ASSIGNMENT_MULTIPLE]]), is stale, or is unknown.
     *
     * @param instant The timestamp when the write was requested.
     * @param opaqueHandle Handle for store write operation, opaque to the state machine.
     * @param shouldFreeze Whether the written assignment should be flagged as frozen, which will
     *                     prevent the assignment generator from overwriting it.
     * @param proposal Assignment proposal for the write operation.
     * @param outputBuilder State machine output builder to build driver actions.
     */
    def onWriteRequest(
        instant: Instant,
        opaqueHandle: Promise[Store.WriteAssignmentResult],
        proposal: ProposedAssignment,
        shouldFreeze: Boolean,
        outputBuilder: OutputBuilder): Unit = {
      // NOTE: If predecessorGeneration < latestAssignment.getGeneration we could fail the write
      // operation earlier here. But the latestAssignment could be stale, and we don't want to fail
      // the write with a stale version in OccFailure. For simplicity we skip the optimization here.

      val predecessorGenerationOpt: Option[Generation] =
        proposal.predecessorOpt.map((_: Assignment).generation)
      for (predecessorGeneration: Generation <- predecessorGenerationOpt)
        if (predecessorGeneration != Generation.EMPTY) {
          require(
            predecessorGeneration.incarnation == config.storeIncarnation,
            s"Predecessor assignment store incarnation " +
            s"${predecessorGeneration.incarnation} does not match configured " +
            s"store incarnation ${config.storeIncarnation}"
          )
        }

      // Choose a generation for the assignment. If a skewed clock pushes the generation number
      // far into the future, this scheme can recover if the store incarnation is increased after
      // the clocks are corrected.
      val lowerBoundGenerationExclusive: Generation = predecessorGenerationOpt match {
        case Some(predecessorGeneration: Generation) => predecessorGeneration
        case None => initialAssignmentGenerationLowerBoundExclusive
      }
      val generation: Generation = Generation.createForCurrentTime(
        config.storeIncarnation,
        instant,
        lowerBoundGenerationExclusive
      )

      val assignment: Assignment =
        proposal.commit(
          isFrozen = shouldFreeze,
          AssignmentConsistencyMode.Affinity,
          generation
        )

      // By default, we write an assignment delta rather than a full assignment (including only
      // slices which have changed from the previous assignment). However, we'll perform a
      // compacting write with a full assignment if at least one of the following is true:
      // 1. The total number of slices stored in etcd is greater than a multiple of slices in the
      //    proposal SliceMap.
      // 2. The total number of assignment entries in etcd exceeds the static threshold.
      // 3. The proposal predecessor is ahead of the current slice history, this indicates our slice
      //    history is stale and we cannot rely upon it.
      // 4. Slice history is unknown, this might happen if we don't have an assignment for the
      //    target or for when we learn about an assignment from a trusted store.
      var fullAssignmentWriteReasons: FullAssignmentWriteReasons = FullAssignmentWriteReasons.NONE
      val totalSliceThreshold: Long = FULL_ASSIGNMENT_MULTIPLE * assignment.sliceMap.entries.size
      latestAssignmentHistory.storedHistorySize match {
        case StoredHistorySize.Known(numSlices: Long, numAssignmentGenerations: Long) =>
          if (numSlices >= totalSliceThreshold) {
            fullAssignmentWriteReasons =
              fullAssignmentWriteReasons.withTotalStoredSlicesAboveThreshold()
          }
          if (numAssignmentGenerations >= MAX_STORED_ASSIGNMENT_ENTRIES) {
            fullAssignmentWriteReasons =
              fullAssignmentWriteReasons.withTotalStoredAssignmentEntriesAboveThreshold()
          }
        case StoredHistorySize.Unknown =>
          fullAssignmentWriteReasons = fullAssignmentWriteReasons.withSliceHistoryUnknown()
      }
      if (predecessorGenerationOpt.exists { predecessorGeneration: Generation =>
          latestAssignmentHistory.generationOrEmpty < predecessorGeneration
        }) {
        fullAssignmentWriteReasons = fullAssignmentWriteReasons.withSliceHistoryStale()
      }
      val shouldWriteFullAssignment = fullAssignmentWriteReasons.shouldWriteFullAssignment()
      if (shouldWriteFullAssignment) {
        logger.info(
          s"Writing full assignment for $generation because $fullAssignmentWriteReasons " +
          s"(Latest history: ${latestAssignmentHistory.generationOrEmpty}, " +
          s"Stored history size: ${latestAssignmentHistory.storedHistorySize}, " +
          s"Stored slice count threshold: $totalSliceThreshold, " +
          s"Stored entries threshold: $MAX_STORED_ASSIGNMENT_ENTRIES, " +
          s"Predecessor: $predecessorGenerationOpt)"
        )
      }

      val diffGeneration: Generation =
        if (shouldWriteFullAssignment) {
          Generation.EMPTY
        } else {
          predecessorGenerationOpt.getOrElse(Generation.EMPTY)
        }

      // Convert the assignment to DiffAssignment and serialize to ByteString.
      val diffAssignment: DiffAssignment = assignment.toDiff(diffGeneration)
      val diffAssignmentBytes: ByteString = ByteString.copyFrom(diffAssignment.toProto.toByteArray)

      outputBuilder.appendAction(
        DriverAction.PerformEtcdWrite(
          target,
          EtcdWriteRequest(
            opaqueHandle,
            diffAssignment,
            assignment,
            version = EtcdClientHelper.getVersionFromNonLooseGeneration(generation),
            diffAssignmentBytes,
            previousVersionOpt = predecessorGenerationOpt.map { predecessorGeneration: Generation =>
              EtcdClientHelper.getVersionFromNonLooseGeneration(predecessorGeneration)
            },
            !shouldWriteFullAssignment
          )
        )
      )
    }

    /** Handles Etcd watch failure from the store. */
    def onEtcdWatchFailure(now: TickerTime, status: Status): Unit = {
      // If the status is not CANCELLED, schedule the next watch with backoff. We don't expect a
      // watch to fail with CANCELLED, since the driver never cancels a watch on target once
      // established. CANCELLED is used only for cleanup in tests.
      if (status != Status.CANCELLED) {
        nextWatchTime = now + watchBackoff.nextDelay()
        logger.warn(s"Retrying because watch failed with terminal error: $status")
      } else {
        logger.info("Not retrying because watch was cancelled")
      }

      // Reset the watch state
      watchState = WatchState.EMPTY
    }

    def onAdvance(now: TickerTime, outputBuilder: OutputBuilder): Unit = {
      // Request a watch, if time is greater than or equal to nextWatchTime.
      if (now >= nextWatchTime) {
        logger.info(s"Requesting watch")
        nextWatchTime = TickerTime.MAX
        outputBuilder.appendAction(
          DriverAction.PerformEtcdWatch(target, WATCH_DURATION, ETCD_WATCH_PAGE_LIMIT)
        )
      } else {
        outputBuilder.ensureAdvanceBy(nextWatchTime)
      }
    }

    /** Handles inform assignment request from the store. */
    def onInformAssignmentRequest(assignment: Assignment, outputBuilder: OutputBuilder): Unit = {
      if (assignment.generation.incarnation == config.storeIncarnation) {
        // Its possible we learn about an assignment that may not be the successor of the current
        // known assignment. So, we cannot be certain about number of slices stored in Etcd for the
        // assignment.
        updateLatestAssignmentHistory(
          AssignmentSliceHistory(Some(assignment), StoredHistorySize.Unknown),
          outputBuilder
        )
      }
    }

    /**
     * Checks that `storeGeneration` is at least `assignerKnownGeneration`, logging at alert
     * level otherwise.
     */
    private def expectStoreAheadOfAssignerKnownGeneration(
        storeGeneration: Generation,
        assignerKnownGeneration: Generation): Unit = {
      // We do not use logger.assert here because we want to fire a sev0 alert but not throw an
      // exception.
      if (storeGeneration < assignerKnownGeneration)
        logger.alert(
          Severity.CRITICAL,
          CachingErrorCode.ASSIGNER_KNOWS_GREATER_ASSIGNMENT_GENERATION_THAN_DURABLE_STORE,
          s"Assigner knows of a later assignment ($assignerKnownGeneration) than the latest " +
          s"assignment in etcd ($storeGeneration). This indicates that either assignment(s) have " +
          s"been lost in etcd, or an assignment was externalized by EtcdStore before it was made " +
          s"durable."
        )
    }

    /** Updates cached knowledge about the lower generation bound for new assignments. */
    private def updateInitialAssignmentGenerationLowerBound(
        newInitialAssignmentVersionLowerBoundExclusive: Version): Unit = {
      val newLowerBoundExclusive: Generation = EtcdClientHelper
        .createGenerationFromVersion(newInitialAssignmentVersionLowerBoundExclusive)
      if (newLowerBoundExclusive.incarnation == config.storeIncarnation) {
        initialAssignmentGenerationLowerBoundExclusive = Ordering
          .ordered[Generation]
          .max(initialAssignmentGenerationLowerBoundExclusive, newLowerBoundExclusive)
      }
    }

    /**
     * Updates the [[latestAssignmentHistory]] to `assignmentHistory` if `assignmentHistory` is more
     * recent and inform the driver.
     */
    private def updateLatestAssignmentHistory(
        assignmentHistory: AssignmentSliceHistory,
        outputBuilder: OutputBuilder): Unit = {
      if (assignmentHistory.generationOrEmpty > latestAssignmentHistory.generationOrEmpty) {
        logger.info(
          s"Updating assignment from ${latestAssignmentHistory.generationOrEmpty} to " +
          s"${assignmentHistory.generationOrEmpty}"
        )

        // Update the latest assignment with slice history.
        latestAssignmentHistory = assignmentHistory

        // Inform the driver about the latest assignment.
        outputBuilder.appendAction(
          DriverAction.UseAssignment(target, latestAssignmentHistory.assignmentOpt.get)
        )
      } else if (assignmentHistory.generationOrEmpty == latestAssignmentHistory.generationOrEmpty &&
        latestAssignmentHistory.storedHistorySize == StoredHistorySize.Unknown &&
        assignmentHistory.storedHistorySize != StoredHistorySize.Unknown) {
        // The incoming assignment is the same as what we already have, except it also contains how
        // many slices are stored in etcd. We prefer this new one since it has more information.
        logger.info(
          s"Restoring assignment slice history for generation " +
          s"${latestAssignmentHistory.generationOrEmpty}"
        )
        latestAssignmentHistory = assignmentHistory
      } else {
        logger.debug(
          s"Ignoring assignment generation ${assignmentHistory.generationOrEmpty}, " +
          s"latest assignment generation: ${latestAssignmentHistory.generationOrEmpty}"
        )
      }
    }
  }

  /** Target specific state in the store. */
  private val targetMap = mutable.Map[Target, TargetState]()

  override protected def onAdvance(
      tickerTime: TickerTime,
      instant: Instant): StateMachineOutput[DriverAction] = {
    onAdvanceInternal(tickerTime, new StateMachineOutput.Builder[DriverAction])
  }

  override protected def onEvent(
      tickerTime: TickerTime,
      instant: Instant,
      event: EtcdStoreStateMachine.Event): StateMachineOutput[DriverAction] = {
    val outputBuilder = new StateMachineOutput.Builder[DriverAction]

    event match {
      case Event.EtcdWatchEvent(target: Target, event: WatchEvent) =>
        // We expect a watch event is received only for an already tracked target.
        getTargetState(target).onEtcdWatchEvent(event, outputBuilder)
      case Event.EtcdWatchFailure(target: Target, status: Status) =>
        // We expect a watch failure is received only for an already tracked target.
        getTargetState(target).onEtcdWatchFailure(tickerTime, status)
      case Event.EtcdWriteResponse(
          target: Target,
          handle: EtcdWriteRequest,
          response: Try[WriteResponse]
          ) =>
        // We expect a write response is received only for an already tracked target.
        getTargetState(target).onEtcdWriteResponse(
          handle,
          response,
          outputBuilder
        )
      case Event.WriteRequest(
          target: Target,
          opaqueHandle: Promise[Store.WriteAssignmentResult],
          shouldFreeze: Boolean,
          proposal: ProposedAssignment
          ) =>
        getOrCreateTargetState(target).onWriteRequest(
          instant,
          opaqueHandle,
          proposal,
          shouldFreeze,
          outputBuilder
        )

      case Event.InformAssignmentRequest(target: Target, assignment: Assignment) =>
        getOrCreateTargetState(target).onInformAssignmentRequest(assignment, outputBuilder)

      case Event.EnsureTargetTracked(target: Target) =>
        getOrCreateTargetState(target)
    }

    onAdvanceInternal(tickerTime, outputBuilder)
  }

  /** Gets or creates [[TargetState]] if one does not exist. */
  private def getOrCreateTargetState(target: Target): TargetState = {
    targetMap.getOrElseUpdate(target, new TargetState(target))
  }

  /**
   * REQUIRES: `target` already exists in [[targetMap]].
   *
   * Gets [[TargetState]] for the given target.
   */
  private def getTargetState(target: Target): TargetState = {
    val targetStateOpt = targetMap.get(target)
    require(targetStateOpt.nonEmpty, s"$target does not exist in the state machine")
    targetStateOpt.get
  }

  private def onAdvanceInternal(
      tickerTime: TickerTime,
      outputBuilder: StateMachineOutput.Builder[DriverAction]): Output = {
    // Advance the state for each target.
    for (targetState <- targetMap.values) {
      targetState.onAdvance(tickerTime, outputBuilder)
    }
    outputBuilder.build()
  }
}

private[assigner] object EtcdStoreStateMachine {
  type Output = StateMachineOutput[DriverAction]
  type OutputBuilder = StateMachineOutput.Builder[DriverAction]

  private val logger = PrefixLogger.create(this.getClass, "")

  /**
   * Request for [[EtcdStore]] to write to etcd.
   *
   * @param promise Promise for the store write operation, opaque to the state machine.
   * @param diffAssignment The diff for the assignment (with current in-memory state) being written
   *                       to Etcd.
   * @param assignment The assignment being written to the store.
   * @param version Version of the assignment to be written.
   * @param value Serialized (diff) assignment value.
   * @param previousVersionOpt The previous assignment generation's [[Version]] for OCC check in
   *                           [[EtcdClient]]. See [[EtcdClient.write()]]
   * @param isIncremental If the assignment value should be written incrementally.
   */
  case class EtcdWriteRequest(
      promise: Promise[Store.WriteAssignmentResult],
      diffAssignment: DiffAssignment,
      assignment: Assignment,
      version: Version,
      value: ByteString,
      previousVersionOpt: Option[Version],
      isIncremental: Boolean)

  /**
   * Actions that the state machine outputs to the driver in response to incoming signals
   * or the passage of time.
   */
  sealed trait DriverAction

  object DriverAction {

    /** Asks the driver to use the assignment as the source of truth for the target. */
    case class UseAssignment(target: Target, assignment: Assignment) extends DriverAction

    /**
     * Asks the driver to complete the store write operation for the handle.
     *
     * @param target Target for the assignment write operation.
     * @param promise Promise for the store write operation.
     * @param response Response of the store write operation.
     */
    case class CompletedWrite(
        target: Target,
        promise: Promise[WriteAssignmentResult],
        response: Try[WriteAssignmentResult])
        extends DriverAction

    /**
     * Asks the driver to perform assignment write on Etcd.
     *
     * @param target Target for the assignment write operation.
     * @param request Request for the store write operation.
     */
    case class PerformEtcdWrite(target: Target, request: EtcdWriteRequest) extends DriverAction

    /**
     * Asks the driver to perform etcd watch `target`'s assignments for `watchDuration` in etcd.
     * The watch protocol is paginated, and `pageLimit` defines the maximum number of assignment
     * entries to include in a single page.
     */
    case class PerformEtcdWatch(target: Target, watchDuration: Duration, pageLimit: Int)
        extends DriverAction

    /**
     * Asks the driver to cancel the watch. A cancellation of watch from the state machine is done
     * if there is data corruption. Reasons are enumerated in [[StoreErrorCode]].
     *
     * When the driver cancels the Etcd watch, we expect to receive a failure notification from
     * [[EtcdClient.watch]] callback to be forwarded to the state machine. The state machine is then
     * responsible for scheduling a retry for the watch using [[DriverAction.PerformEtcdWatch]] with
     * backoff.
     */
    case class CancelEtcdWatch(target: Target, reason: StoreErrorReason) extends DriverAction
  }

  /**
   * Errors encountered when interacting with etcd. All of these indicate a bug in [[EtcdStore]] or
   * data corruption in Etcd and will page SEV0. Use with caution.
   */
  object StoreErrorCode extends Enumeration {
    type StoreErrorCode = Value

    /** The watch event received from Etcd client is not parse-able as [[DiffAssignment]]. */
    val WATCH_VALUE_PARSE_ERROR: Value = Value

    /**
     * The diff from the Etcd watch event, applied to assignment present in state machine is
     * unused. The diff maybe unused for reasons enumerated in [[DiffUnused]], none of these are
     * expected.
     */
    val DIFF_UNUSED_ERROR: Value = Value

    /**
     * When keeping track of slice history, we need to make sure we don't miss any updates i.e. the
     * diff generation in partial SliceMap has the same generation as the current assignment. A
     * mismatch in generations indicates data corruption.
     */
    val DIFF_GENERATION_ERROR: Value = Value
  }

  /** Encapsulates reason for store error with [[Status]]. */
  case class StoreErrorReason(status: Status, code: StoreErrorCode.StoreErrorCode)

  /** Input events to the [[EtcdStoreStateMachine]]. */
  sealed trait Event

  object Event {

    /**
     * Informs the [[EtcdStoreStateMachine]] with a [[WatchEvent]] reported from [[EtcdClient]].
     * Expects the target is already being tracked by the state machine and that we have an
     * active watch i.e. an [[EtcdWatchFailure]] event hasn't been received.
     */
    case class EtcdWatchEvent(target: Target, event: WatchEvent) extends Event

    /**
     * Informs the [[EtcdStoreStateMachine]] about failure from Etcd watch. Expects the target is
     * already being tracked by the state machine and that we have an active watch i.e. an
     * [[EtcdWatchFailure]] event hasn't been received.
     */
    case class EtcdWatchFailure(target: Target, status: Status) extends Event

    /**
     * Informs the [[EtcdStoreStateMachine]] about a write request.
     *
     * @param target Target for the write request,
     * @param opaqueHandle Handle for store write operation, opaque to the state machine.
     * @param shouldFreeze Whether the written assignment should be flagged as frozen, which will
     *                     prevent the assignment generator from overwriting it.
     * @param proposal Assignment proposal for the write operation.
     */
    case class WriteRequest(
        target: Target,
        opaqueHandle: Promise[Store.WriteAssignmentResult],
        shouldFreeze: Boolean,
        proposal: ProposedAssignment)
        extends Event

    /**
     * Inform the [[EtcdStoreStateMachine]] about a write response from [[EtcdClient]]. Expects the
     * target is already being tracked by the state machine.
     *
     * @param target Target for the write request,
     * @param request The original request for the store write operation.
     * @param response Write response from [[EtcdClient]].
     */
    case class EtcdWriteResponse(
        target: Target,
        request: EtcdWriteRequest,
        response: Try[WriteResponse])
        extends Event

    /** Inform the [[EtcdStoreStateMachine]] about a new target that it may want to track. */
    case class EnsureTargetTracked(target: Target) extends Event

    /** Inform the [[EtcdStoreStateMachine]] about an assignment from trusted authority. */
    case class InformAssignmentRequest(target: Target, assignment: Assignment) extends Event
  }

  /**
   * Local knowledge of the total number of slices and assignment generations stored in etcd for a
   * particular assignment history.
   */
  private sealed trait StoredHistorySize

  private object StoredHistorySize {

    /** Number of slices and generations is known. */
    case class Known(numSlices: Long, numAssignmentGenerations: Long) extends StoredHistorySize {
      override def toString: String =
        s"{numSlices=$numSlices, numAssignmentGenerations=$numAssignmentGenerations}"
    }

    /** Number of slices and generations is not known. */
    case object Unknown extends StoredHistorySize
  }

  private object AssignmentSliceHistory {

    /** Empty assignment with no slice history. */
    val EMPTY: AssignmentSliceHistory = AssignmentSliceHistory(None, StoredHistorySize.Unknown)
  }

  /**
   * Assignment alongside knowledge of the current number of assignments and slices in etcd. The
   * tracked sizes are used to determine when a full assignment should be written.
   */
  // Consider refactoring this to decouple the knowledge of the assignment from the stored history
  // size. It's weird that we might reset our knowledge of etcd when we get a new assignment.
  private case class AssignmentSliceHistory(
      assignmentOpt: Option[Assignment],
      storedHistorySize: StoredHistorySize) {

    /** Generation of the assignment, or `EMPTY` if there is no assignment. */
    def generationOrEmpty: Generation = assignmentOpt match {
      case Some(assignment: Assignment) => assignment.generation
      case None => Generation.EMPTY
    }

    /**
     * Computes a new [[AssignmentSliceHistory]] from the current one, after applying the
     * [[DiffAssignment]].
     *
     * If the `diffAssignment` has a more recent generation than [[assignmentOpt]], returns
     * [[AssignmentSliceHistory]] with an updated assignment and slice history size by adding the
     * number of slices in `diffAssignment` to the current history size.
     *
     * Otherwise, returns the current [[AssignmentSliceHistory]].
     *
     * @param diffAssignment [[DiffAssignment]] to be applied.
     */
    def applyDiff(
        diffAssignment: DiffAssignment): Either[AssignmentSliceHistory, StoreErrorReason] = {
      // If current assignment is ahead, we don't need to perform an update.
      if (generationOrEmpty >= diffAssignment.generation) {
        Left(this)
      } else {
        Assignment.fromDiff(assignmentOpt, diffAssignment) match {
          case Left(assignment: Assignment) =>
            diffAssignment.sliceMap match {
              case DiffAssignmentSliceMap
                    .Partial(
                    diffGeneration: Generation,
                    sliceMap: SliceMap[GapEntry[SliceAssignment]]
                    ) =>
                // When keeping track of slice history, we need to make sure we don't miss any
                // updates i.e. the diff generation in partial SliceMap has the same generation as
                // the current assignment. A mismatch in generations indicates data corruption.
                if (generationOrEmpty != diffGeneration) {
                  Right(
                    StoreErrorReason(
                      Status.DATA_LOSS.withDescription(
                        s"The diff generation $diffGeneration does not match assignment " +
                        s"$generationOrEmpty"
                      ),
                      StoreErrorCode.DIFF_GENERATION_ERROR
                    )
                  )
                } else {
                  val newSliceHistorySize: StoredHistorySize = storedHistorySize match {
                    case StoredHistorySize.Known(numSlices: Long, numAssignmentGenerations: Long) =>
                      // Add the size of defined slices in diffs to existing history.
                      StoredHistorySize.Known(
                        numSlices + sliceMap.entries.count(_.isDefined),
                        numAssignmentGenerations + 1
                      )
                    case StoredHistorySize.Unknown =>
                      // Slice history remains unknown without a full SliceMap.
                      storedHistorySize
                  }
                  // Add the size of defined slices in diffs to existing history.
                  Left(
                    AssignmentSliceHistory(Some(assignment), newSliceHistorySize)
                  )
                }
              case DiffAssignmentSliceMap.Full(
                  sliceMap: SliceMap[SliceAssignment]
                  ) =>
                // Assume that the writer of the full assignment compacted the history into a
                // single record and reset both the known slice and generation counts.
                Left(
                  AssignmentSliceHistory(
                    Some(assignment),
                    StoredHistorySize
                      .Known(numSlices = sliceMap.entries.size, numAssignmentGenerations = 1)
                  )
                )
            }
          case Right(diffUnused: DiffUnused) =>
            // Diff is unused, this should not happen and indicates data corruption in etcd.

            logger.error(s"The diff applied to $assignmentOpt is unused. Reason: $diffUnused")
            Right(
              StoreErrorReason(
                Status.DATA_LOSS.withDescription(
                  s"The diff applied to $assignmentOpt is unused. Reason: $diffUnused"
                ),
                StoreErrorCode.DIFF_UNUSED_ERROR
              )
            )
        }
      }
    }
  }

  private object WatchState {

    /** Empty assignment slice history without causal event. */
    val EMPTY: WatchState = WatchState(AssignmentSliceHistory.EMPTY, hasReceivedCausalEvent = false)
  }

  /**
   * Represents latest assignment slice history maintained from [[WatchEvent]].
   *
   * @param assignmentHistory Assignment with slice history.
   * @param hasReceivedCausalEvent If [[EtcdClient.WatchEvent.Causal]] has been received.
   */
  private case class WatchState private (
      private val assignmentHistory: AssignmentSliceHistory,
      private val hasReceivedCausalEvent: Boolean) {

    /**
     * Computes a new [[WatchState]] from the current one, after applying the [[WatchEvent]].
     *
     * @param event [[WatchEvent]] to be applied.
     */
    def applyEvent(event: WatchEvent): Either[WatchState, StoreErrorReason] = {
      event match {
        case WatchEvent.VersionedValue(version: Version, value: ByteString) =>
          logger.debug(s"Watch event: Observed new value with version: $version")

          parseDiffAssignmentSafely(value) match {
            case Left(diffAssignment: DiffAssignment) =>
              // Update the assignment.
              assignmentHistory.applyDiff(diffAssignment) match {
                case Left(newAssignmentHistory) =>
                  Left(this.copy(assignmentHistory = newAssignmentHistory))
                case Right(exception) => Right(exception)
              }
            case Right(error) => Right(error)
          }

        case WatchEvent.Causal =>
          logger.debug(
            s"Watch event: Observed causal signal; we have seen all DiffAssignments in " +
            s"the store since the watch began."
          )
          Left(this.copy(hasReceivedCausalEvent = true))
      }
    }

    /**
     * Returns latest assignment history. If a [[WatchEvent.Causal]] has not been received, this
     * returns [[AssignmentSliceHistory.EMPTY]], to avoid reporting stale assignments.
     */
    def getLatestAssignmentHistory: AssignmentSliceHistory =
      if (hasReceivedCausalEvent) assignmentHistory else AssignmentSliceHistory.EMPTY

    /**
     * Parses [[DiffAssignment]] from specified value or returns [[Status.DATA_LOSS]] on failure.
     */
    private def parseDiffAssignmentSafely(
        value: ByteString): Either[DiffAssignment, StoreErrorReason] = {
      try {
        Left(DiffAssignment.fromProto(DiffAssignmentP.parseFrom(value.toByteArray)))
      } catch {
        case NonFatal(ex) =>
          Right(
            StoreErrorReason(
              Status.DATA_LOSS
                .withDescription("Failed to parse watch event value")
                .withCause(ex),
              StoreErrorCode.WATCH_VALUE_PARSE_ERROR
            )
          )
      }
    }
  }

  /**
   * The reasons for compacting an assignment history and writing a full assignment rather than
   * appending a delta.
   */
  case class FullAssignmentWriteReasons(
      private val totalStoredSlicesAboveThreshold: Boolean,
      private val totalStoredAssignmentEntriesAboveThreshold: Boolean,
      private val sliceHistoryStale: Boolean,
      private val sliceHistoryUnknown: Boolean
  ) {

    /**
     * The total number of slices, and by proxy, the total amount of data stored in etcd is too
     * high.
     */
    def withTotalStoredSlicesAboveThreshold(): FullAssignmentWriteReasons = {
      copy(totalStoredSlicesAboveThreshold = true)
    }

    /**
     * The total number of assignment entries in etcd is too high. Even if the total number of
     * slices is small, each entry has constant-size overheads and read page boundaries are drawn
     * on the number of entries.
     */
    def withTotalStoredAssignmentEntriesAboveThreshold(): FullAssignmentWriteReasons = {
      copy(totalStoredAssignmentEntriesAboveThreshold = true)
    }

    /**
     * The latest known state may be behind what's stored in etcd, so we prefer to compact and
     * write a full assignment instead of extending a potentially too-long history with another
     * delta.
     */
    def withSliceHistoryStale(): FullAssignmentWriteReasons = {
      copy(sliceHistoryStale = true)
    }

    /**
     * The latest known state doesn't include any information about the total number of slices or
     * assignment entries in etcd, so like [[withSliceHistoryStale()]], we prefer to compact and
     * write a full assignment.
     */
    def withSliceHistoryUnknown(): FullAssignmentWriteReasons = {
      copy(sliceHistoryUnknown = true)
    }

    /**
     * Returns whether we have any reason to write a full assignment rather than an incremental
     * delta.
     */
    def shouldWriteFullAssignment(): Boolean = {
      totalStoredSlicesAboveThreshold ||
      totalStoredAssignmentEntriesAboveThreshold ||
      sliceHistoryStale ||
      sliceHistoryUnknown
    }
    override def toString: String = {
      s"(totalStoredSlicesAboveThresold=$totalStoredSlicesAboveThreshold, " +
      s"totalStoredAssignmentEntriesAboveThreshold=$totalStoredAssignmentEntriesAboveThreshold, " +
      s"sliceHistoryStale=$sliceHistoryStale, " +
      s"sliceHistoryUnknown=$sliceHistoryUnknown)"
    }
  }

  object FullAssignmentWriteReasons {

    /**
     * The [[FullAssignmentWriteReasons]] instance which describes non-intent to write a full
     * assignment, i.e. to write an assignment delta.
     */
    val NONE: FullAssignmentWriteReasons = FullAssignmentWriteReasons(
      totalStoredSlicesAboveThreshold = false,
      totalStoredAssignmentEntriesAboveThreshold = false,
      sliceHistoryStale = false,
      sliceHistoryUnknown = false
    )
  }

  /**
   * The duration for watching the target's assignment in etcd.
   *
   * As long as the store exists, we want to reliably watch the target of interest, so we use an
   * infinite duration to avoid automatic cancellation.
   */
  private val WATCH_DURATION = Duration.Inf

  /**
   * If the number of slices in the store are greater than or equal to [[FULL_ASSIGNMENT_MULTIPLE]]
   * times the slices in the current SliceMap then we perform a full assignment.
   *
   * Writing full assignments periodically limits the history size and ensures that watchers for the
   * target can catch up on the latest assignment quickly.
   */
  private val FULL_ASSIGNMENT_MULTIPLE: Long = 5

  /**
   * The maximum number of stored slice entries in etcd before the store will perform a compacting
   * full assignment write.
   *
   * The value here is chosen considering [[ETCD_WATCH_PAGE_LIMIT]] such that reading a target's
   * full assignment history doesn't require too many round trips.
   */
  private val MAX_STORED_ASSIGNMENT_ENTRIES: Int = 50

  /**
   * The maximum number of entries/versions to request in a single etcd watch request.
   */
  private val ETCD_WATCH_PAGE_LIMIT: Int = 10

  private[assigner] object forTest {
    val WATCH_DURATION: Duration = EtcdStoreStateMachine.WATCH_DURATION
    val ETCD_WATCH_PAGE_LIMIT: Int = EtcdStoreStateMachine.ETCD_WATCH_PAGE_LIMIT
  }
}
