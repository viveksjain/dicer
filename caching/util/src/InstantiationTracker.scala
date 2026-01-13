package com.databricks.caching.util

import javax.annotation.concurrent.{GuardedBy, ThreadSafe}
import scala.collection.mutable

/**
 * A utility to enforce uniqueness of objects within the process. `T`'s equality semantics
 * define the level at which uniqueness is enforced. In particular, for a given instance
 * `t1`, `enforceAndRecord(t1)` will throw if, within this process, there was a previous invocation
 * `enforceAndRecord(t0)` such that `t0 == t1`.
 *
 * Thread-safe.
 */
@ThreadSafe
class InstantiationTracker[T] private {

  /**
   * The past instantiations by key. We store a stack trace of the original instantiation for
   * debugging.
   */
  @GuardedBy("this")
  private val instantiations: mutable.Map[T, String] = mutable.Map.empty

  /**
   * Records an instantiation associated with `key`. Throws if a previous call to
   * [[enforceAndRecord()]] was made with an equivalent `key`.
   */
  def enforceAndRecord(key: T): Unit = synchronized {
    for (previouslyCreatedLocation: String <- instantiations.get(key)) {
      throw new IllegalStateException(
        s"Instance for $key already exists. Previously created at $previouslyCreatedLocation"
      )
    }

    // Drop the frames for the call to `getStackTrace` and `enforceAndRecord` and indent the
    // original stack trace so that it's distinguished from the trace of the exception that we would
    // throw on a duplicate creation.
    val stackTrace: String =
      Thread.currentThread().getStackTrace.drop(2).mkString("Array(\n    ", "\n    ", ")")

    instantiations.put(key, stackTrace)
  }
}

object InstantiationTracker {

  /** Returns a new [[InstantiationTracker]] for uniqueness key type `T`. */
  def create[T](): InstantiationTracker[T] = new InstantiationTracker[T]()

  /**
   * A convenience object and type alias which specifies that the tracked type should be
   * instantiated at most once per-process.
   */
  object PerProcessSingleton
  type PerProcessSingletonType = PerProcessSingleton.type
}
