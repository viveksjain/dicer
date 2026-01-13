package com.databricks.dicer.assigner.config

import com.databricks.caching.util.{Cancellable, ValueStreamCallback}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
 * A trait for a provider of per-target configs. The interface allows watching for dynamic config
 * updates, but the implementation can also be static, where config stays the same during the
 * lifetime of the provider.
 */
trait TargetConfigProvider {

  /**
   * Starts the config provider. `initialPollTimeout` is the timeout for the initial poll to
   * the dynamic config service. [[TargetConfigProvider.DEFAULT_INITIAL_POLL_TIMEOUT]] can be
   * passed in if there is no specific value for the use case. We make this an explicit argument
   * so that it's clear to the caller that this operation is blocking.
   */
  def startBlocking(initialPollTimeout: FiniteDuration): Unit

  /** Returns whether dynamic config is enabled. */
  def isDynamicConfigEnabled: Boolean

  /**
   * Returns the latest configs for all targets.
   *
   * Note: depending on the implementation, [[startBlocking()]] may be required to be called before
   * this method. For static configuration provider, this always returns the same static
   * configuration.
   */
  def getLatestTargetConfigMap: InternalTargetConfigMap

  /**
   * Watches the config changes, and invoke the `callback` when the config gets updated.
   *
   * Note: depending on the implementation, [[startBlocking()]] may be required to be called before
   * this method. For static configuration provider, this is a no-op.
   */
  def watch(callback: ValueStreamCallback[InternalTargetConfigMap]): Cancellable
}

object TargetConfigProvider {

  /** A short, default timeout for the initial dynamic config value poll at startup. */
  val DEFAULT_INITIAL_POLL_TIMEOUT: FiniteDuration = 5.seconds
}
