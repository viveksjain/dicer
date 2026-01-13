package com.databricks.common.status

/**
 * OSS wrapper for ProbeStatuses.
 *
 * Re-exports ProbeStatuses from the ReadinessProbeTracker wrapper to maintain the same import
 * path as the internal version.
 */
object ProbeStatuses {
  val OK_STATUS: com.databricks.rpc.armeria.ProbeStatus =
    com.databricks.rpc.armeria.ProbeStatuses.OK_STATUS
  val NOT_YET_READY_STATUS: com.databricks.rpc.armeria.ProbeStatus =
    com.databricks.rpc.armeria.ProbeStatuses.NOT_YET_READY_STATUS
}
