package com.databricks.dicer.assigner.algorithm

import com.databricks.dicer.assigner.algorithm.Algorithm.Config

/**
 * Clamps replica counts to configured resource-adjusted min and max bounds. Performed before
 * splitting and merging so that split and merge decisions are made with the correct per-replica
 * load values.
 */
private[assigner] object ConstraintPhase {
  def clampReplicas(config: Config, assignment: MutableAssignment): Unit = {
    val resourceAdjustedMinReplicas: Int = config.resourceAdjustedKeyReplicationConfig.minReplicas
    val resourceAdjustedMaxReplicas: Int = config.resourceAdjustedKeyReplicationConfig.maxReplicas
    for (entry <- assignment.sliceAssignmentsIterator) {
      // (Inlining this with its type annotation in the for loop above exceeds the column limit).
      val sliceAssignment: assignment.MutableSliceAssignment = entry
      val currentNumReplicas: Int = sliceAssignment.currentNumReplicas
      val clampedNumReplicas: Int = currentNumReplicas
        .max(resourceAdjustedMinReplicas)
        .min(resourceAdjustedMaxReplicas)
      sliceAssignment.adjustReplicas(clampedNumReplicas)
    }
  }
}
