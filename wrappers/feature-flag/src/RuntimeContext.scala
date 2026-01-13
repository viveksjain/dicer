package com.databricks.featureflag.client.utils

/** Runtime context for dynamic flag evaluation, not supported in OSS. */
case class RuntimeContext()

object RuntimeContext {
  val EMPTY: RuntimeContext = RuntimeContext()
}
