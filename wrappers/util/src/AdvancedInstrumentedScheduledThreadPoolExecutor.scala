package com.databricks.util.advanced

import java.util.concurrent.{ScheduledExecutorService, ThreadFactory}
import com.databricks.threading.AbstractInstrumentedScheduledThreadPoolExecutor

/**
 * Open source compatible interface for AdvancedInstrumentedScheduledThreadPoolExecutor factory,
 * which just creates [[AbstractInstrumentedScheduledThreadPoolExecutor]].
 */
object AdvancedInstrumentedScheduledThreadPoolExecutor {

  /**
   * Returns a ScheduledExecutorService with the specified parameters.
   *
   * @param name The name of the thread pool
   * @param numThreads The number of threads to keep in the pool
   * @param factory The thread factory to use for creating new threads
   * @param enableContextPropagation Ignored in OSS
   */
  def create(
      name: String,
      numThreads: Int,
      factory: ThreadFactory,
      enableContextPropagation: Boolean): ScheduledExecutorService = {
    new AbstractInstrumentedScheduledThreadPoolExecutor(
      name,
      numThreads,
      factory,
      enableContextPropagation
    )
  }
}
