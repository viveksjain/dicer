package com.databricks.logging

/** Dummy implementation of tracing attribution context. In OSS this is a no-op. */
trait AttributionContextTracing {

  /** Simply executes the block without any attribution context tracking. */
  def withAttributionContext[T](context: AttributionContext)(block: => T): T = {
    block
  }
}
