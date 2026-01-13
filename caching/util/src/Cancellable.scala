package com.databricks.caching.util

import io.grpc.Status

/**
 * Allows cancellation of a resource/work. Represents a token that can be used by a caller for
 * best-effort cancellation of pending work.
 */
trait Cancellable {

  /**
   * REQUIRES: `reason` is not Status.OK
   *
   * Attempts to cancel pending work (e.g. for a [[CancellableFuture]]).
   * IMPORTANT: Cancellation is best-effort and asynchronous.
   */
  def cancel(reason: Status = Status.CANCELLED): Unit
}

object Cancellable {

  /** A [[Cancellable]] that does nothing when cancelled. */
  val NO_OP_CANCELLABLE: Cancellable = _ => {}
}
