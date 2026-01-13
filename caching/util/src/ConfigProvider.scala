package com.databricks.caching.util

import com.databricks.conf.trusted.DeploymentModes

/** A trait for objects that can be serialized as JSON strings. */
trait JsonSerializableConfig {
  def toJsonString: String
}

/**
 * A trait providing SAFE configs, including:
 *  - The config targets for dev, staging, and prod.
 *  - The default config for dev, staging, and prod.
 *  - A map of [[ConfigScope]]s to their corresponding overrides for dev, staging, and prod.
 *
 * - Only `DeploymentModes.Development`, `DeploymentModes.Staging` and `DeploymentModes.Production`
 * are considered to be valid modes.
 *
 * @note In the proto, one override can correspond to one or more [[ConfigScope]]s. However, in the
 *       SAFE config tool provider, we do not preserve this property. Instead, we require each
 *       override to correspond to only one [[ConfigScope]], thereby duplicates are removed.
 *       A downside of this approach is that specifying one override with multiple [[ConfigScope]]s
 *       in the proto results in it being broken down into multiple overrides. This leads to a
 *       longer SAFE Jsonnet file content. We prefer this approach since duplications might result
 *       in misconfiguration.
 */
trait SafeConfigProvider[Config <: JsonSerializableConfig] {

  /**
   * REQUIRES: `mode` must be one of the valid modes specified in
   *           [[SafeConfigProvider.SHORT_NAMES_TO_VALID_MODES]].
   *
   * Gets the config targets for the given mode.
   */
  def configTargets(mode: DeploymentModes.Value): Set[String]

  /**
   * REQUIRES: `mode` must be one of the valid modes specified in
   *           [[SafeConfigProvider.SHORT_NAMES_TO_VALID_MODES]].
   * REQUIRES: `configTarget` must exist under  `mode`.
   *
   * Gets the default config for the given mode.
   */
  def getDefaultConfig(mode: DeploymentModes.Value, configTarget: String): Config

  /**
   * REQUIRES: `mode` must be one of the valid modes specified in
   *           [[SafeConfigProvider.SHORT_NAMES_TO_VALID_MODES]].
   * REQUIRES: `configTarget` must exist under  `mode`.
   *
   * Gets the map from [[ConfigScope]]s to their corresponding overridden configs for the given
   * mode.
   */
  def getScopedOverrides(
      mode: DeploymentModes.Value,
      configTarget: String): Map[ConfigScope, Config]

  /**
   * Gets the configs from the targets to their corresponding configs which will be used to canary
   * production config changes.
   */
  def getProductionCanaryConfigs: Map[String, Config]

  /** Returns the canary config scope. */
  def canaryConfigScope: ConfigScope

}

/** Object that includes mappings and sets related to deployment modes. */
object SafeConfigProvider {
  val SHORT_NAMES_TO_VALID_MODES: Map[String, DeploymentModes.Value] = Map(
    SafeConfigUtil.DEV_MODE_SHORT_NAME -> DeploymentModes.Development,
    SafeConfigUtil.STAGING_MODE_SHORT_NAME -> DeploymentModes.Staging,
    SafeConfigUtil.PROD_MODE_SHORT_NAME -> DeploymentModes.Production
  )

  val VALID_MODES: Set[DeploymentModes.Value] = SHORT_NAMES_TO_VALID_MODES.values.toSet
}
