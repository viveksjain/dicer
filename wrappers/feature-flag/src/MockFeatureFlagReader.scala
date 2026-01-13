package com.databricks.featureflag.client.experimentation

import com.databricks.featureflag.client.utils.RuntimeContext

/** Provider of a mock FeatureFlagReader for testing, no-op in OSS. */
trait MockFeatureFlagReaderProvider {
  lazy val mockFeatureFlagReader: MockFeatureFlagReader = new MockFeatureFlagReader()
}

/** Mock FeatureFlagReader for testing, no-op in OSS since dynamic config is not supported. */
class MockFeatureFlagReader {

  /** No-op: does nothing in OSS. */
  def setMockValue[T: Manifest](
      flagName: String,
      value: T,
      runtimeCtxOpt: Option[RuntimeContext] = None
  ): Unit = {}

  /** No-op: does nothing in OSS. */
  def clearMockValue(propertyName: String, runtimeCtxOpt: Option[RuntimeContext] = None): Unit = {}

  /** No-op: does nothing in OSS. */
  def clearAllMockValues(): Unit = {}

  /** Always returns None in OSS. */
  def getFeatureFlagValue[T: Manifest](
      flagName: String,
      runtimeContextOpt: Option[RuntimeContext]
  ): Option[T] = None
}
