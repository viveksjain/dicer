package com.databricks.common.instrumentation

/** Exports cache information to metrics. This is a no-op for OSS. */
object SCaffeineCacheInfoExporter {
  def registerCache[T](name: String, cache: T): T = {
    cache
  }
}
