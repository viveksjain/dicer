package com.databricks.featureflag

import com.databricks.conf.DbConf
import com.databricks.featureflag.client.utils.RuntimeContext

/**
 * The base dynamic conf trait for dynamic feature flags. In OSS, dynamic configuration is not
 * supported, and will always return the default value.
 */
trait BaseDynamicConf extends DbConf {

  /** Dynamic feature flag, not supported in OSS: always returns the default value. */
  class FeatureFlag[T: Manifest](
      val flagName: String,
      defaultValue: T
  ) {

    /** Always returns the default value. */
    def getCurrentValue(): T = defaultValue

    /** Always returns the default value. */
    def getCurrentValue(runtimeContext: RuntimeContext): T = defaultValue
  }

  object FeatureFlag {
    def apply[T: Manifest](flagName: String, defaultValue: T): FeatureFlag[T] = {
      new FeatureFlag(flagName, defaultValue)
    }
  }

  /** A batch of feature flags, always returns empty map in OSS. */
  class BatchFeatureFlag(val flagName: String) {
    def getSubFlagValues(runtimeContext: Option[RuntimeContext] = None): Map[String, String] = {
      Map.empty
    }
  }

  object BatchFeatureFlag {
    def apply(flagName: String): BatchFeatureFlag = new BatchFeatureFlag(flagName)
  }
}
