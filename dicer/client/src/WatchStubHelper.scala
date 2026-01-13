package com.databricks.dicer.client

import java.net.URI
import java.util.concurrent.TimeUnit

import io.grpc.ChannelCredentials
import io.grpc.Grpc
import io.grpc.InsecureChannelCredentials
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder

import com.databricks.api.proto.dicer.common.AssignmentServiceGrpc
import com.databricks.api.proto.dicer.common.AssignmentServiceGrpc.AssignmentServiceStub
import com.databricks.caching.util.StatusOr
import com.databricks.dicer.common.WatchServerHelper
import com.databricks.rpc.tls.TLSOptions

object WatchStubHelper {

  /**
   * Creates a watch stub for watching assignments from `address`.
   *
   * @note `clientName` is unused but matches the internal implementation signature for
   *  compatibility.
   */
  private[dicer] def createWatchStub(
      clientName: String,
      watchAddress: URI,
      tlsOptionsOpt: Option[TLSOptions],
      watchFromDataPlane: Boolean): AssignmentServiceStub = {
    // Correct the address if it was created without a scheme. For example, if the original string
    // address was "localhost:8080", then we interpret it as "http://localhost:8080" to avoid URI
    // parsing issues ("localhost" as being treated as the scheme).
    val addressString: String = watchAddress.toString
    val normalizedAddress: String =
      if (addressString.contains("://")) addressString else "http://" + addressString

    val correctedUri: URI = new URI(normalizedAddress)

    // Use gRPC's transport-independent TLS API.
    val credentials: ChannelCredentials = tlsOptionsOpt match {
      case Some(tlsOptions) => tlsOptions.channelCredentials()
      case None => InsecureChannelCredentials.create()
    }

    val channelBuilder: ManagedChannelBuilder[_] =
      Grpc
        .newChannelBuilderForAddress(correctedUri.getHost, correctedUri.getPort, credentials)
        .maxInboundMessageSize(WatchServerHelper.MAX_WATCH_MESSAGE_CONTENT_LENGTH_BYTES)
    // Note: Connect timeout is not configurable in gRPC's transport-independent API.
    // Connection establishment uses OS-level TCP defaults (typically 20-120 seconds depending
    // on the OS). This is by design to maintain transport independence. The RPC-level deadline
    // (WATCH_RPC_TIMEOUT) provides fail-fast behavior by timing out the entire call including
    // connection establishment, so clients will fail fast and retry if the server is unreachable.

    val channel: ManagedChannel = channelBuilder.build()
    AssignmentServiceGrpc.stub(channel)
  }

  /**
   * Service-to-service (S2S) proxy routing is not implemented in this version. This method
   * exists for API compatibility and returns the stub unchanged.
   */
  private[client] def createS2SProxyWatchStub(
      baseS2SProxyWatchStub: AssignmentServiceStub,
      address: URI): StatusOr[AssignmentServiceStub] = {
    StatusOr.success(baseS2SProxyWatchStub)
  }
}
