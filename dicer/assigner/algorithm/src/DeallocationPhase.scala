package com.databricks.dicer.assigner.algorithm

/**
 * Implements the deallocation phase of the algorithm, which deallocates any Slice replicas that are
 * assigned to unhealthy resources.
 */
private[assigner] object DeallocationPhase {

  /** Runs the deallocation phase. */
  def deallocateUnhealthyResources(assignment: MutableAssignment): Unit = {
    // Deallocate any Slice replicas assigned to unhealthy resources.
    for (unhealthyResource: assignment.ResourceState <- assignment.getUnhealthyResourceStates) {
      // Note that we must avoid using an iterator here since the unhealthy resource's set of
      // assigned slices is modified by `deallocateResource` in the loop below, so we capture the
      // elements in a Vector.
      val assignedSlices: Vector[assignment.MutableSliceAssignment] =
        unhealthyResource.getAssignedSlices.toVector
      for (sliceAssignment: assignment.MutableSliceAssignment <- assignedSlices) {
        sliceAssignment.deallocateResource(unhealthyResource)
      }
    }
  }
}
