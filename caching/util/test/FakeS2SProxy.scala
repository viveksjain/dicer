package com.databricks.caching.util

import com.databricks.caching.util.Lock.withLock
import com.google.common.io.ByteStreams
import javax.annotation.concurrent.GuardedBy
import io.grpc.{
  CallOptions,
  ClientCall,
  Grpc,
  InsecureChannelCredentials,
  InsecureServerCredentials,
  ManagedChannel,
  Metadata,
  MethodDescriptor,
  Server,
  ServerBuilder,
  ServerCall,
  ServerCallHandler,
  ServerMethodDefinition,
  Status,
  StatusException
}
import io.grpc.MethodDescriptor.MethodType

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.ReentrantLock
import scala.util.Try

/**
 * A substitute for S2S Proxy that can be used in tests.
 *
 * `FakeS2SProxy` is a generic gRPC proxy that extracts the `X-Databricks-Upstream-Host-Port` header
 * from a request if it is present and forwards the request to the specified upstream server, or
 * forwards the request to a fallback localhost port if the header is not present.
 *
 * The implementation is adapted from the example gRPC proxy found here:
 * https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/grpcproxy/GrpcProxy.java
 */
class FakeS2SProxy(server: Server, handler: FakeS2SProxy.Handler) {

  /** Returns the port that the proxy is listening on. */
  def port: Int = server.getPort

  /**
   * Configures the fallback localhost ports to route requests without a
   * `X-Databricks-Upstream-Host-Port` header to.
   *
   * The configured fallback ports are randomly chosen among for each such request.
   */
  def setFallbackUpstreamPorts(ports: Vector[Int]): Unit = handler.setFallbackUpstreamPorts(ports)

  /** Returns the gRPC metadata (i.e. HTTP headers) attached to the most recent proxied request. */
  def latestRequestMetadataOpt: Option[Metadata] = handler.getLatestRequestMetadataOpt

  /** Stops the proxy server. */
  def shutdown(): Unit = server.shutdownNow()
}

object FakeS2SProxy {

  /**
   * The header that the fake S2S Proxy attaches to forwarded requests.
   *
   * This allows the upstream servers in tests to verify whether the request came through S2S Proxy.
   */
  val ADDED_HEADER: String = "X-Dicer-Request-From-FakeS2SProxy"

  /** The header from which S2S Proxy extracts the base64-encoded upstream host-port string. */
  private val UPSTREAM_HOST_PORT_HEADER: String = "X-Databricks-Upstream-Host-Port"

  private class Handler extends ServerCallHandler[Array[Byte], Array[Byte]] {
    private val lock = new ReentrantLock

    @GuardedBy("lock")
    private var fallbackUpstreamPorts: Vector[Int] = Vector()

    @GuardedBy("lock")
    private var latestRequestMetadataOpt: Option[Metadata] = None

    def setFallbackUpstreamPorts(ports: Vector[Int]): Unit = withLock(lock) {
      fallbackUpstreamPorts = ports
    }

    def getLatestRequestMetadataOpt: Option[Metadata] = withLock(lock) {
      latestRequestMetadataOpt
    }

    override def startCall(
        serverCall: ServerCall[Array[Byte], Array[Byte]],
        metadata: Metadata): ServerCall.Listener[Array[Byte]] = withLock(lock) {
      latestRequestMetadataOpt = Some(metadata)

      // Open a gRPC channel to the upstream specified by the header (or the fallback if the header
      // is unspecified). For simplicity, we open a separate channel per incoming request rather
      // than caching the channels.
      val (host, port): (String, Int) = extractUpstreamHostAndPort(metadata)
      val channel: ManagedChannel =
        Grpc.newChannelBuilderForAddress(host, port, InsecureChannelCredentials.create()).build()

      // Initiate a request to the upstream server.
      val clientCall: ClientCall[Array[Byte], Array[Byte]] =
        channel.newCall(serverCall.getMethodDescriptor, CallOptions.DEFAULT)
      val clientCallListener = new ClientCall.Listener[Array[Byte]] {
        override def onClose(status: Status, trailers: Metadata): Unit = {
          serverCall.close(status, trailers)
          channel.shutdownNow()
        }
        override def onHeaders(headers: Metadata): Unit = serverCall.sendHeaders(headers)
        override def onMessage(message: Array[Byte]): Unit = serverCall.sendMessage(message)
      }
      val outboundMetadata = new Metadata
      outboundMetadata.merge(metadata)
      outboundMetadata.put(Metadata.Key.of(ADDED_HEADER, Metadata.ASCII_STRING_MARSHALLER), "true")
      clientCall.start(clientCallListener, outboundMetadata)

      // Disable flow control.
      serverCall.request(Int.MaxValue)
      clientCall.request(Int.MaxValue)

      // Return a listener for the incoming RPC that will forward it to the outgoing call we
      // initiated above.
      new ServerCall.Listener[Array[Byte]] {
        override def onCancel(): Unit = clientCall.cancel("Cancelled", null)
        override def onHalfClose(): Unit = clientCall.halfClose()
        override def onMessage(message: Array[Byte]): Unit = clientCall.sendMessage(message)
      }
    }

    /**
     * Parses and returns the upstream host and port specified by the header, or one of the
     * fallbacks if the header is unspecified.
     */
    @GuardedBy("lock")
    @throws[StatusException]("if the upstream host-port header is malformed")
    private def extractUpstreamHostAndPort(metadata: Metadata): (String, Int) = {
      val hostPortHeader: String =
        metadata.get(
          Metadata.Key.of(UPSTREAM_HOST_PORT_HEADER, Metadata.ASCII_STRING_MARSHALLER)
        )
      if (hostPortHeader == null) {
        // The header was not specified, so randomly choose one of the fallbacks.
        if (fallbackUpstreamPorts.isEmpty) {
          throw Status.UNAVAILABLE
            .withDescription("No fallback upstream ports configured")
            .asException
        }
        val port: Int =
          fallbackUpstreamPorts(ThreadLocalRandom.current.nextInt(fallbackUpstreamPorts.size))
        return ("localhost", port)
      }

      // Parse the header. Note that the real S2S Proxy would route the request to a random pod if
      // it failed to parse the header, but for our testing purposes, we would rather fail fast
      // since that should never happen.
      val hostPortString: String = new String(Base64.getDecoder.decode(hostPortHeader), UTF_8)
      val parts: Array[String] = hostPortString.split(':')
      if (parts.length != 2) {
        throw Status.INVALID_ARGUMENT
          .withDescription("Malformed upstream host-port header")
          .asException
      }
      val host: String = parts(0)
      val port: Int = Try(parts(1).toInt)
        .getOrElse(throw Status.INVALID_ARGUMENT.withDescription("Malformed port").asException)
      (host, port)
    }
  }

  /**
   * A marshaller that acts as a pure identity function for byte strings, since this proxy treats
   * requests and responses as opaque blobs.
   */
  private object ByteMarshaller extends MethodDescriptor.Marshaller[Array[Byte]] {
    override def parse(stream: InputStream): Array[Byte] = ByteStreams.toByteArray(stream)
    override def stream(value: Array[Byte]): InputStream = new ByteArrayInputStream(value)
  }

  /** Creates and starts a fake S2S Proxy server on an arbitrary available port. */
  def createAndStart(): FakeS2SProxy = {
    val handler = new Handler
    val builder: ServerBuilder[_] =
      Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
    builder.fallbackHandlerRegistry((methodName: String, _: String) => {
      ServerMethodDefinition.create(
        MethodDescriptor
          .newBuilder(ByteMarshaller, ByteMarshaller)
          .setFullMethodName(methodName)
          .setType(MethodType.UNKNOWN)
          .build(),
        handler
      )
    })
    new FakeS2SProxy(builder.build().start(), handler)
  }
}
