package com.databricks.common.util

import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.Logger

import com.databricks.macros.sourcecode.FullName
import org.apache.hadoop.util.{ShutdownHookManager => HadoopShutdownHookManager}

/** Shutdown hook manager built on top of hadoop's ShutdownHookManager. */
object ShutdownHookManager {
  private val logger: Logger = Logger("ShutdownHookManager")

  /** The default priority used for shutdown hooks. Higher priority hooks are run first. */
  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * Adds a shutdown hook with the given priority.
   *
   * @param priority - The shutdown hook's priority. Higher priority hooks are run first.
   * @param hook - The shutdown hook.
   * @return The Runnable hook just added.
   */
  def addShutdownHook[S](priority: Int)(hook: => S)(implicit caller: FullName): Runnable = {
    addShutdownHook(priority, timeoutMillisOpt = None)(hook)
  }

  /**
   * Adds a shutdown hook with the given priority and timeout.
   *
   * @param priority - The shutdown hook's priority. Higher priority hooks are run first.
   * @param timeoutMillisOpt - The shutdown hook timeout in milliseconds.
   * @param hook - The shutdown hook.
   * @return The Runnable hook just added.
   */
  def addShutdownHook[S](priority: Int, timeoutMillisOpt: Option[Long])(hook: => S)(
      implicit caller: FullName): Runnable = {
    logger.info(
      s"Adding shutdown hook with priority=$priority, defined at ${caller.value}" +
      s"${timeoutMillisOpt.map(t => s", timeout=${t}ms").getOrElse("")}"
    )
    val runnable = new Runnable() {
      override def run() {
        logger.info(s"Running shutdown hook defined at ${caller.value}")
        hook
        logger.info(s"Completed shutdown hook defined at ${caller.value}")
      }
    }
    if (timeoutMillisOpt.isDefined) {
      HadoopShutdownHookManager
        .get()
        .addShutdownHook(runnable, priority, timeoutMillisOpt.get, TimeUnit.MILLISECONDS)
    } else {
      HadoopShutdownHookManager.get().addShutdownHook(runnable, priority)
    }
    runnable
  }
}
