package com.databricks.dicer.client

private[dicer] object Ports {

  /**
   * The TCP port on which Slicelets listen for assignment watch RPCs in production.
   */
  // Implementation note: we should consider removing the Slicelet port from RpcPortConf; we only
  // make it configurable for testing where a Slicelet might listen on any port. However, overriding
  // this port value is otherwise disallowed, and the testing use case could be fulfilled by using a
  // local EdsTestEnvironment and writing EDS entries mapping the relevant EDS address to the
  // Slicelets' assignment server port, so removing the field from RpcPortConf would
  // prevent accidental misconfiguration.
  val SLICELET_ASSIGNMENT_SERVER_PORT: Int = 24510
}
