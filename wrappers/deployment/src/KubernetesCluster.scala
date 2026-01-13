package com.databricks.api.proto.infra.infra

/** Specification for a Kubernetes cluster. */
class KubernetesCluster(uri: String) {

  /** Returns the cluster's identifier URI. */
  def getUri: String = uri
}
