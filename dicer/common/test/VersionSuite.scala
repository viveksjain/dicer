package com.databricks.dicer.common

import com.databricks.caching.util.TestUtils.TestName
import com.databricks.testing.DatabricksTest
import com.databricks.dicer.external.Target
import io.prometheus.client.CollectorRegistry

import com.databricks.caching.util.MetricUtils
import com.databricks.dicer.common.TargetHelper.TargetOps

class VersionSuite extends DatabricksTest with TestName {

  /** The metric collector registry, used to verify metric values. */
  private val registry = CollectorRegistry.defaultRegistry

  gridTest("recordClientVersion populates metrics correctly")(
    Seq(
      Target.apply(_: String),
      (name: String) => Target.createAppTarget(transformToSafeAppTargetName(name), "instance-id")
    )
  ) { (targetFactory: String => Target) =>
    // Test plan: Record a client version via valid and invalid branches and validate that the
    // metrics are populated correctly.
    val target: Target = targetFactory(getSafeName)

    // Verify: recording a valid branch populates metrics with the expected commit timestamp string
    // and epoch millis.
    Version.recordClientVersion(
      target,
      AssignmentMetricsSource.Clerk,
      "test_client_version_customer_2024-09-13_17.05.12Z_test-branch-name_ffff9654_1957847387"
    )
    assert(
      MetricUtils.getMetricValue(
        registry,
        "dicer_client_build_info",
        Map(
          "targetCluster" -> target.getTargetClusterLabel,
          "targetName" -> target.getTargetNameLabel,
          "targetInstanceId" -> target.getTargetInstanceIdLabel,
          "source" -> AssignmentMetricsSource.Clerk.toString,
          "commitTimestamp" -> "2024-09-13_17.05.12Z",
          "language" -> "Scala"
        )
      ) == 1
    )
    assert(
      MetricUtils.getMetricValue(
        registry,
        "dicer_client_commit_timestamp",
        Map(
          "targetCluster" -> target.getTargetClusterLabel,
          "targetName" -> target.getTargetNameLabel,
          "targetInstanceId" -> target.getTargetInstanceIdLabel,
          "source" -> AssignmentMetricsSource.Clerk.toString
        )
      ) == 1726247112000L
    )

    // Verify: recording an invalid branch populates metrics with "unknown" commit timestamp string
    // and default epoch millis.
    Version.recordClientVersion(
      target,
      AssignmentMetricsSource.Clerk,
      "test_client_version_customer_17.05.12Z_91280$$$$$_1957847387"
    )

    assert(
      MetricUtils.getMetricValue(
        registry,
        "dicer_client_build_info",
        Map(
          "targetCluster" -> target.getTargetClusterLabel,
          "targetName" -> target.getTargetNameLabel,
          "targetInstanceId" -> target.getTargetInstanceIdLabel,
          "source" -> AssignmentMetricsSource.Clerk.toString,
          "commitTimestamp" -> "unknown",
          "language" -> "Scala"
        )
      ) == 1
    )
    assert(
      MetricUtils.getMetricValue(
        registry,
        "dicer_client_commit_timestamp",
        Map(
          "targetCluster" -> target.getTargetClusterLabel,
          "targetName" -> target.getTargetNameLabel,
          "targetInstanceId" -> target.getTargetInstanceIdLabel,
          "source" -> AssignmentMetricsSource.Clerk.toString
        )
      ) == 0L
    )
  }

}
