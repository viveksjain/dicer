package com.databricks.dicer.assigner
import com.databricks.caching.util.SequentialExecutionContext
import com.databricks.dicer.assigner.EtcdPreferredAssignerStore.{
  PreferredAssignerProposal,
  WriteResult
}
import com.databricks.dicer.assigner.InterposingEtcdPreferredAssignerDriver.ShutdownOption
import com.databricks.dicer.assigner.PreferredAssignerValue.PreferredAssignerWatchCellConsumer
import com.databricks.dicer.common.Incarnation
import com.databricks.caching.util.{EtcdTestEnvironment, EtcdClient, InterposingJetcdWrapper}
import io.grpc.{Status, StatusException}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Random

/** A wrapper over [[EtcdPreferredAssignerStore.Impl]] that allows the test to block writes. */
class InterposingEtcdPreferredAssignerStore private (
    sec: SequentialExecutionContext,
    jetcdWrapper: InterposingJetcdWrapper,
    store: EtcdPreferredAssignerStore.Impl)
    extends EtcdPreferredAssignerStore {

  /** The list of blocked writes, protected by `sec`. */
  private var blockedWritesOpt
      : Option[mutable.ArrayBuffer[DeferredWritePreferredAssignerProposal]] = None

  /** Blocks the [[write()]] method until [[unblockWrites()]] is called. */
  def blockWrites(): Future[Unit] = sec.call {
    blockedWritesOpt match {
      case Some(_) => // Already blocked, do nothing.
      case None =>
        blockedWritesOpt = Some(mutable.ArrayBuffer())
    }
    Future.successful(())
  }

  /** Unblocks the [[write()]] method. */
  def unblockWrites(): Future[Unit] = sec.call {
    blockedWritesOpt match {
      case Some(blockedWrites: mutable.ArrayBuffer[DeferredWritePreferredAssignerProposal]) =>
        // Unblock and run all deferred writes.
        for (blockedWrite <- blockedWrites) { blockedWrite.run() }
        blockedWritesOpt = None
      case None => // Already unblocked, do nothing.
    }
  }

  /** Returns the number of currently blocked writes. */
  def getNumBlockedWrites(): Future[Int] = sec.call {
    blockedWritesOpt match {
      case Some(blockedWrites) => blockedWrites.size
      case None => 0
    }
  }

  override def storeIncarnation: Incarnation = store.storeIncarnation

  override def getPreferredAssignerWatchCell: PreferredAssignerWatchCellConsumer =
    store.getPreferredAssignerWatchCell

  override def write(proposal: PreferredAssignerProposal): Future[WriteResult] = sec.flatCall {
    blockedWritesOpt match {
      case Some(blockedWrites) => // Writes are currently blocked.
        val promise = Promise[WriteResult]()
        blockedWrites.append(DeferredWritePreferredAssignerProposal(proposal, promise))
        promise.future
      case None =>
        store.write(proposal)
    }
  }

  override def informPreferredAssigner(preferredAssigner: PreferredAssignerValue): Unit =
    store.informPreferredAssigner(preferredAssigner)

  /**
   * Represents a [[PreferredAssignerProposal]] write request that is being deferred because the
   * writes are being blocked.
   */
  private case class DeferredWritePreferredAssignerProposal(
      proposal: PreferredAssignerProposal,
      promise: Promise[WriteResult]) {
    def run(): Unit = {
      store.write(proposal).onComplete(promise.complete)(sec)
    }
  }

  /**
   * Stops the store by (best-effort) failing the etcd watches, and blocking the writes if
   * `shutdownOption` is not [[ShutdownOption.ALLOW_ABDICATION]].
   *
   * @note This method is best-effort and may not fail the watches if the initial read hasn't
   *       been completed and the watch hasn't been established yet. But it is guaranteed to block
   *       the writes if `shutdownOption` is not [[ShutdownOption.ALLOW_ABDICATION]].
   */
  def shutdown(shutdownOption: ShutdownOption): Future[Unit] = sec.call {
    jetcdWrapper.startFailingGets(new StatusException(Status.UNAVAILABLE))
    try {
      jetcdWrapper.failCurrentWatch(new StatusException(Status.UNAVAILABLE))
    } catch {
      case _: IllegalArgumentException =>
      // Ignore the exception if the watch is not established.
    }
    shutdownOption match {
      case ShutdownOption.ABRUPT =>
        blockWrites()
      case _ =>
        Future.successful(())
    }
  }
}
object InterposingEtcdPreferredAssignerStore {

  /** Creates an instance of [[InterposingEtcdPreferredAssignerStore]]. */
  def create(
      sec: SequentialExecutionContext,
      storeIncarnation: Incarnation,
      etcd: EtcdTestEnvironment,
      etcdClientConfig: EtcdClient.Config,
      random: Random,
      config: EtcdPreferredAssignerStore.Config): InterposingEtcdPreferredAssignerStore = {
    val (client, jetcdWrapper) =
      etcd.createEtcdClientWithInterposingJetcdWrapper(sec.getClock, etcdClientConfig)
    val store =
      EtcdPreferredAssignerStore.Impl.create(sec, storeIncarnation, client, random, config)
    new InterposingEtcdPreferredAssignerStore(sec, jetcdWrapper, store)
  }

}
