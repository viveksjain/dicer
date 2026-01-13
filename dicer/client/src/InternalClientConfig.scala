package com.databricks.dicer.client

import java.net.URI

import scala.concurrent.duration._

import com.databricks.dicer.common.ClientType
import com.databricks.dicer.common.WatchServerHelper.{WATCH_RPC_TIMEOUT, validateWatchRpcTimeout}
import com.databricks.dicer.external.Target
import com.databricks.rpc.tls.TLSOptions

/**
 * REQUIRES: `watchRpcTimeout` is at least 1 second
 *
 * Internal config for a Clerk or Slicelet.
 *
 * @param clientType Type of the client, either Clerk or Slicelet
 * @param subscriberDebugName Name of the subscriber passed to the Assigner for debugging purposes
 * @param watchAddress URI to which the client connects to watch assignments
 * @param tlsOptionsOpt TLS options for the gRPC connection
 * @param target Resources for which the assignment is being watcher
 * @param watchStubCacheTime How long to cache the gRPC stub for the watch RPC
 * @param watchFromDataPlane Whether the client is running in the data plane and watching
 *                           assignments from the Dicer Assigner running in the region's general
 *                           cluster
 * @param rejectWatchRequestsOnFatalTargetMismatch See param in [[InternalClientConf]] of the same
 *                                                 name.
 * @param watchRpcTimeout The deadline sent in the client request for each RPC
 * @param minRetryDelay Minimum time to retry a failed RPC call for exponential backoff
 * @param maxRetryDelay Maximum time to retry a failed RPC call for exponential backoff
 * @param assignmentLatencySampleFraction Fraction of assignment propagation latency events to
 *                                        sample for logging.
 *                                        Should be between [0, 1]. 0.0 disables latency logging.
 */
case class InternalClientConfig(
    clientType: ClientType,
    subscriberDebugName: String,
    watchAddress: URI,
    tlsOptionsOpt: Option[TLSOptions],
    target: Target,
    watchStubCacheTime: FiniteDuration,
    watchFromDataPlane: Boolean,
    rejectWatchRequestsOnFatalTargetMismatch: Boolean,
    watchRpcTimeout: FiniteDuration = WATCH_RPC_TIMEOUT,
    minRetryDelay: FiniteDuration = 1.second,
    maxRetryDelay: FiniteDuration = 10.seconds,
    assignmentLatencySampleFraction: Double) {
  validateWatchRpcTimeout(watchRpcTimeout)

  /** Client name to use for the RPC stub. */
  val clientName: String = s"dicer-$clientType-${target.name}"
}

object InternalClientConfig {

  /**
   * Buffer to add to RPC layer deadlines relative to the explicit deadline in the client request.
   * This buffer offsets network latency and other delays in the RPC client and server layers (e.g.,
   * connection establishment, executor delays, etc.).
   *
   * This should have the same value as DEADLINE_BUFFER in slice_lookup_config.rs.
   */
  private[client] final val DEADLINE_BUFFER: FiniteDuration = 10.seconds
}
