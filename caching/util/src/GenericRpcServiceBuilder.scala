package com.databricks.caching.util

import scala.collection.mutable.ArrayBuffer

import io.grpc.{Server, ServerBuilder, ServerServiceDefinition}

/**
 * RPC server builder. Open source version has empty parameters because
 * [[io.grpc.ServerBuilder.forPort]] is a static method. Not threadsafe.
 */
class GenericRpcServiceBuilder private () {

  /** The delegate to the underlying builder, immutable once set. */
  private var delegate: Option[ServerBuilder[_]] = None

  /** The services added to the builder */
  private val services: ArrayBuffer[ServerServiceDefinition] = ArrayBuffer.empty

  /**
   * PRECONDITION: Builder must not be initialized more than once.
   *
   * Initializes the builder for the given port.
   */
  def initBuilderForPort(port: Int): Unit = {
    require(delegate.isEmpty, "Builder already initialized")
    delegate = Some(ServerBuilder.forPort(port))
    for (service: ServerServiceDefinition <- services) {
      delegate.get.addService(service)
    }
  }

  /**
   * PRECONDITION: Builder must not be initialized more than once.
   *
   * Initializes the builder with an existing ServerBuilder (e.g., one configured with custom
   * settings).
   */
  def initBuilder(serverBuilder: ServerBuilder[_]): Unit = {
    require(delegate.isEmpty, "Builder already initialized")
    delegate = Some(serverBuilder)
    for (service: ServerServiceDefinition <- services) {
      delegate.get.addService(service)
    }
  }

  /** Adds a service to the builder. */
  def addService(service: ServerServiceDefinition): Unit = {
    services += service
  }

  /** Builds the server. */
  def build(): Server = {
    delegate match {
      case Some(delegate) => delegate.build()
      case None => throw new IllegalStateException("Builder not initialized")
    }
  }
}

object GenericRpcServiceBuilder {

  /** Creates a new GenericRpcServiceBuilder instance. */
  def create(): GenericRpcServiceBuilder = new GenericRpcServiceBuilder()
}
