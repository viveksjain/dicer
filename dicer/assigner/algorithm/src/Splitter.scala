package com.databricks.dicer.assigner.algorithm

import scala.collection.mutable

import com.databricks.dicer.assigner.algorithm.Algorithm.{Config, MIN_AVG_SLICE_REPLICAS}

/** Implements the split phases of the algorithm. */
private[assigner] object Splitter {

  /**
   * Splits hot slices to reduce per-replica load below the split threshold. This phase repeatedly
   * splits the hottest slice until all per-replica loads are below the threshold or no more splits
   * are possible. All else being equal, splitting is preferred over replication to maintain
   * affinity, so this phase runs before replication.
   */
  def splitHotSlices(config: Config, assignment: MutableAssignment): Unit = {
    splitInternal(
      assignment,
      minSliceReplicas = Int.MaxValue,
      splitThreshold = config.desiredLoadRange.splitThreshold
    )
  }

  /**
   * Performs zero-churn splits to ensure total replica count is at least MIN_AVG_SLICE_REPLICAS per
   * resource. This phase runs after de-replication and merging because those steps may have reduced
   * the count below the minimum.
   *
   * Zero-churn means splits don't change resource assignments - both child slices inherit the
   * parent's resources. Despite this, we still prioritize splitting hot slices to benefit the
   * migration phase.
   */
  def ensureMinTotalSliceReplicas(resources: Resources, assignment: MutableAssignment): Unit = {
    splitInternal(
      assignment,
      MIN_AVG_SLICE_REPLICAS * resources.availableResources.size,
      splitThreshold = -1.0 // Negative threshold means split regardless of load.
    )
  }

  /**
   * Continue splitting the hottest Slice until one of the following:
   *
   *  - There are no more Slices to split
   *  - There are at least `minSliceReplicas` Slice replicas in the assignment
   *  - The hottest splittable Slice is at or below `splitThreshold`
   */
  private def splitInternal(
      assignment: MutableAssignment,
      minSliceReplicas: Int,
      splitThreshold: Double): Unit = {
    if (minSliceReplicas <= assignment.currentNumTotalSliceReplicas) {
      // Short-circuit before creating a PQ of all Slices if we're already at or above
      // `minSliceReplicas`.
      return
    }

    // Maintain all (remaining) slice assignments in a priority queue so that we can easily find the
    // hottest Slice (by its per-replica load) at each step (ties are broken by slice order).
    // Note that the Slices in this queue is not necessarily splittable (e.g. when they are already
    // single-keyed in original assignment).
    val remainingCandidateSlices =
      mutable.PriorityQueue
        .empty[assignment.MutableSliceAssignment](assignment.sliceAsnOrderingByRawLoadPerReplica)
    remainingCandidateSlices ++= assignment.sliceAssignmentsIterator

    while (remainingCandidateSlices.nonEmpty &&
      assignment.currentNumTotalSliceReplicas < minSliceReplicas) {
      val sliceAssignment: assignment.MutableSliceAssignment = remainingCandidateSlices.dequeue()
      if (sliceAssignment.rawLoadPerReplica <= splitThreshold) {
        // All remaining slices are at or below the split threshold, so stop splitting.
        return
      }
      sliceAssignment.split() match {
        case Some((left, right)) =>
          remainingCandidateSlices.enqueue(left)
          remainingCandidateSlices.enqueue(right)
        case None =>
        // Unsplittable Slice; skip it!
      }
    }
  }
}
