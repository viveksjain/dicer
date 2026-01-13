package com.databricks.dicer.assigner.config
import com.databricks.dicer.external.Target

/**
 * REQUIRES: `name` is a valid target name, conformant with RFC 1123. See [[Target.name]].
 *
 * A strongly-typed, validated wrapper around a target name. Used as a key in config maps, since
 * configurations in Dicer are scoped to target names. Similar to [[Target]], but the name is never
 * scoped to a cluster.
 */
case class TargetName(value: String) {
  Target.validateName(value)

  /** Returns whether the given target has this name. */
  def matches(target: Target): Boolean = value == target.name

  override def toString: String = value
}
object TargetName {

  /** Creates the target name for the given target. */
  def forTarget(target: Target): TargetName = TargetName(target.name)
}
