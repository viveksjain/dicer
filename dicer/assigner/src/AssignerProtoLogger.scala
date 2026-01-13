package com.databricks.dicer.assigner

import scala.concurrent.ExecutionContext

import com.databricks.dicer.common.Assignment
import com.databricks.dicer.external.Target

/**
 * The Assigner's structured logging utility. No-op logger implementation.
 */
private[assigner] trait AssignerProtoLogger {

  /**
   * Convenience method to log assignment update events.
   *
   * @param target the target for which the assignment was updated.
   * @param assignment the assignment containing generation and resource information.
   * @param contextOpt optional assignment generation context.
   */
  def logAssignmentUpdate(
      target: Target,
      assignment: Assignment,
      contextOpt: Option[AssignmentGenerator.AssignmentGenerationContext]): Unit

  /**
   * Convenience method to log preferred assigner change events.
   *
   * @param preferredAssignerValue the preferred assigner value containing role and assigner info
   *      as PreferredAssignerValue.
   */
  def logPreferredAssignerChange(preferredAssignerValue: PreferredAssignerValue): Unit
}

/** No-op implementation of [[AssignerProtoLogger]] that does not perform any logging. */
private object NoopAssignerProtoLogger extends AssignerProtoLogger {

  override def logAssignmentUpdate(
      target: Target,
      assignment: Assignment,
      contextOpt: Option[AssignmentGenerator.AssignmentGenerationContext]): Unit = {
    // No-op
    ()
  }

  override def logPreferredAssignerChange(preferredAssignerValue: PreferredAssignerValue): Unit = {
    // No-op
    ()
  }
}

private[assigner] object AssignerProtoLogger {

  /**
   * Creates a new [[AssignerProtoLogger]] instance. Always returns [[NoopAssignerProtoLogger]].
   *
   * @param assignerInfo the AssignerInfo.
   * @param generationSampleFraction the fraction of generations to sample.
   * @param executor the execution context to use for async logging operations.
   */
  def create(
      assignerInfo: AssignerInfo,
      generationSampleFraction: Double,
      executor: ExecutionContext): AssignerProtoLogger = {
    NoopAssignerProtoLogger
  }

  /**
   * A convenience method to create an [[AssignerProtoLogger]] that does not log any events.
   *
   * @param executor the execution context to use for async logging operations (unused).
   */
  def createNoop(executor: ExecutionContext): AssignerProtoLogger = {
    NoopAssignerProtoLogger
  }
}
