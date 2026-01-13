package com.databricks.dicer.assigner

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.{GuardedBy, ThreadSafe}

import scala.collection.mutable

import com.databricks.caching.util.Lock.withLock
import com.databricks.dicer.assigner.KubernetesTargetWatcher.TerminationHandler

/**
 * FakeKubernetesTargetWatcherFactory - A [[KubernetesTargetWatcher.Factory]] that returns fake k8s
 * target watchers and enables simulating pod termination signals.
 */
@ThreadSafe
class FakeKubernetesTargetWatcherFactory extends KubernetesTargetWatcher.Factory {

  /**
   * Lock to protect [[targetWatcherMap]] and operations (start, stop, isStarted) performed on
   * each TargetWatcher.
   */
  private val lock = new ReentrantLock()

  /**
   * Map from KubernetesWatchTarget to KubernetesTargetWatcher. We maintain this to trigger
   * termination handlers for a target.
   */
  @GuardedBy("lock")
  private val targetWatcherMap =
    new mutable.HashMap[KubernetesWatchTarget, FakeKubernetesTargetWatcher]()

  /**
   * Fake [[KubernetesTargetWatcher]] for a specific watch target (see [[KubernetesWatchTarget]]).
   * Bound to a `terminationHandler` for the target. Tracks if the watch has been started.
   *
   * @param terminationHandler Method to be called when we receive termination signals for a pod of
   *                           the given watch target.
   */
  @ThreadSafe
  private class FakeKubernetesTargetWatcher(val terminationHandler: TerminationHandler)
      extends KubernetesTargetWatcher {

    /** If the watcher has been started. */
    @GuardedBy("lock")
    private var isStarted: Boolean = false

    /** Exposed getter for [[isStarted]] */
    def getIsStarted: Boolean = withLock(lock) {
      isStarted
    }

    override def start(): Unit = withLock(lock) {
      isStarted = true
    }

    override def stop(): Unit = withLock(lock) {
      isStarted = false
    }
  }

  override def create(
      kubernetesWatchTarget: KubernetesWatchTarget,
      terminationHandler: TerminationHandler): KubernetesTargetWatcher = withLock(lock) {
    targetWatcherMap.getOrElseUpdate(
      kubernetesWatchTarget,
      new FakeKubernetesTargetWatcher(terminationHandler)
    )
  }

  /**
   * Simulate a termination signal from Kubernetes for the given watch target and podUuid.
   *
   * NOTE: It is OK for termination handlers to be invoked multiple times for the same podUuid.
   */
  def simulateTerminationSignal(
      kubernetesWatchTarget: KubernetesWatchTarget,
      podUuid: UUID): Unit = {
    // Get the termination handler with lock. We invoke the termination handler ONLY after releasing
    // the lock, since we should never invoke callbacks under the lock (and also k8s watchers make
    // no guarantees about serial execution of callbacks).
    val terminationHandler: TerminationHandler = withLock(lock) {
      val targetWatcher: FakeKubernetesTargetWatcher = targetWatcherMap.getOrElse(
        kubernetesWatchTarget,
        throw new IllegalArgumentException(
          s"No watch on $kubernetesWatchTarget has been established"
        )
      )

      // Trigger termination signal only if watcher is running.
      if (!targetWatcher.getIsStarted) {
        throw new IllegalArgumentException(
          s"Watch on $kubernetesWatchTarget has not been started"
        )
      }

      targetWatcher.terminationHandler
    } // lock released

    // NOTE: As documented in the termination handler spec, calls to the handler are not guaranteed
    // to be serialized (the termination handler can be invoked concurrently from multiple threads).
    terminationHandler(podUuid)
  }
}
