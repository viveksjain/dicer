package com.databricks.infra.lib

import com.databricks.api.proto.infra.infra.KubernetesCluster

/** Trait for accessing infrastructure definitions. */
trait InfraDataModel {
  def getInfraDef: ComputeInfraDefinition
}

/**
 * Provides minimal infrastructure metadata for Dicer, specifically Kubernetes cluster URIs
 * used by Targets.
 */
object InfraDataModel {

  /** Returns some example Kubernetes clusters for testing. */
  lazy val fromEmbedded: InfraDataModel = new InfraDataModel {
    override def getInfraDef: ComputeInfraDefinition = {
      val testClusters = Map(
        "kubernetes-cluster:test-env1/cloud-provider1/domain1/region1/cluster-type1/01" ->
        new KubernetesCluster(
          "kubernetes-cluster:test-env1/cloud-provider1/domain1/region1/cluster-type1/01"
        ),
        "kubernetes-cluster:test-env2/cloud-provider2/domain2/region2/cluster-type2/02" ->
        new KubernetesCluster(
          "kubernetes-cluster:test-env2/cloud-provider2/domain2/region2/cluster-type2/02"
        ),
        "kubernetes-cluster:test-env3/cloud-provider3/domain3/region3/cluster-type3/03" ->
        new KubernetesCluster(
          "kubernetes-cluster:test-env3/cloud-provider3/domain3/region3/cluster-type3/03"
        )
      )
      new ComputeInfraDefinition(testClusters)
    }
  }
}

/**
 * Container for infrastructure definitions, specifically Kubernetes clusters.
 *
 * @param kubernetesClusters Map of cluster identifiers to cluster metadata.
 */
class ComputeInfraDefinition(val kubernetesClusters: Map[String, KubernetesCluster])
