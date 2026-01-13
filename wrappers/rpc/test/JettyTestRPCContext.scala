package com.databricks.rpc.testing

import com.databricks.rpc.RPCContext

/** Factory for creating a [[JettyTestRPCContextBuilder]]. */
object JettyTestRPCContext {
  def builder(): JettyTestRPCContextBuilder = new JettyTestRPCContextBuilder()
}

/** Builder for creating a [[RPCContext]] with specific parameters, for testing. */
class JettyTestRPCContextBuilder {
  def method(method: String): JettyTestRPCContextBuilder = this
  def uri(uri: String): JettyTestRPCContextBuilder = this
  def build(): RPCContext = new RPCContext()
}
