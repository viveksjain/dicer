package com.databricks.dicer.external

import com.databricks.rpc.DatabricksObjectMapper

/**
 * Slicelet is configured to be in the same cluster as the Assigner(s), but the URI of the
 * cluster where the Slicelet is running is not available.
 */
private trait SliceletSuiteWithLocalUnknownCluster extends SliceletSuiteBase {
  override protected val watchFromDataPlane: Boolean = false

  override protected val expectedWhereAmIClusterUri: String = ""

  override protected def locationEnvVarJson: String = DatabricksObjectMapper.toJson(Map.empty)

  /** Local Slicelets are not expected to have cluster URI. */
  override protected def expectedSliceletTargetIdentifier: Target = targetFactory(None, getSafeName)

  override protected def expectedAssignerCanonicalizedTargetIdentifier: Target =
    targetFactory(None, getSafeName)
}

private class ScalaSliceletWithLocalUnknownClusterSuite
    extends ScalaSliceletSuite
    with SliceletSuiteWithLocalUnknownCluster
    with SliceletSuiteWithKubernetesTargetFactory {}

private class ScalaSliceletWithLocalUnknownClusterAndAppTargetFactorySuite
    extends ScalaSliceletSuite
    with SliceletSuiteWithLocalUnknownCluster
    with SliceletSuiteWithAppTargetFactory {}
