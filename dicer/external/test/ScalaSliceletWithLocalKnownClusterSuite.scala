package com.databricks.dicer.external

import com.databricks.dicer.external.SliceletSuite.ASSIGNER_CLUSTER_URI
import com.databricks.rpc.DatabricksObjectMapper

/**
 * Slicelet is configured to be in the same cluster as the Assigner(s), and the URI of the
 * cluster where the Slicelet is running is available.
 */
private trait SliceletSuiteWithLocalKnownCluster extends SliceletSuiteBase {
  override protected val watchFromDataPlane: Boolean = false

  override protected val expectedWhereAmIClusterUri: String = ASSIGNER_CLUSTER_URI.toASCIIString

  /** Set the location environment variable to the Assigner's cluster location. */
  override protected def locationEnvVarJson: String = DatabricksObjectMapper.toJson(
    Map(
      "cloud_provider" -> "AWS",
      "cloud_provider_region" -> "AWS_US_WEST_2",
      "environment" -> "DEV",
      "kubernetes_cluster_type" -> "GENERAL",
      "kubernetes_cluster_uri" -> s"${ASSIGNER_CLUSTER_URI.toASCIIString}",
      "region_uri" -> "region:dev/cloud1/public/region1",
      "regulatory_domain" -> "PUBLIC"
    )
  )

  /** Local Slicelets are not expected to have cluster URI (even if the environment has it). */
  override protected def expectedSliceletTargetIdentifier: Target = targetFactory(None, getSafeName)

  override protected def expectedAssignerCanonicalizedTargetIdentifier: Target =
    targetFactory(None, getSafeName)
}

private class ScalaSliceletWithLocalKnownClusterSuite
    extends ScalaSliceletSuite
    with SliceletSuiteWithLocalKnownCluster
    with SliceletSuiteWithKubernetesTargetFactory {}

private class ScalaSliceletWithLocalKnownClusterAndAppTargetFactorySuite
    extends ScalaSliceletSuite
    with SliceletSuiteWithLocalKnownCluster
    with SliceletSuiteWithAppTargetFactory {}
