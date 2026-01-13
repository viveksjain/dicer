package com.databricks.dicer.friend

import com.databricks.common.util.ShutdownHookManager

object ShutdownHookConstants {

  /**
   * The priority at which to register the Slicelet shutdown hook which transitions the Slicelet
   * into the terminating state.
   *
   * This needs to be exposed to the state migrator so that the state migrator can ensure that its
   * own shutdown hook runs after the Slicelet shutdown hook but before the Armeria one.
   *
   * The shutdown hooks must run in the following order:
   *
   * 1. The Slicelet shutdown hook transitions the Slicelet into the terminating state by informing
   *    Dicer (priority = DEFAULT_SHUTDOWN_PRIORITY + 2).
   * 2. The state migrator shutdown hook runs and waits for all ensuing state transfers to
   *    complete (priority = DEFAULT_SHUTDOWN_PRIORITY + 1).
   * 3. The Armeria shutdown hook runs and implements the RPC server grace period (priority =
   *    DEFAULT_SHUTDOWN_PRIORITY). The Armeria shutdown hook is not aware of open streams so it is
   *    important that the state migrator shutdown hook runs first.
   */
  val SLICELET_TERMINATION_SHUTDOWN_HOOK_PRIORITY: Int =
    ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY + 2
}
