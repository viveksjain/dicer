package com.databricks.featureflag.client

import scala.annotation.StaticAnnotation

/**
 * The annotation holds metadata of the feature flag, must be paired with the flag definition.
 *
 * @param team name of the team that owns this feature flag
 * @param description description of the feature flag
 */
class FeatureFlagDefinition(team: String, description: String = "") extends StaticAnnotation
