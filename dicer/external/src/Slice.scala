package com.databricks.dicer.external

import java.util.Objects

/**
 * REQUIRES: `lowInclusive` is less than `highExclusive`
 *
 * A Slice represents a range of [[SliceKey]]s of the form [lowInclusive .. highExclusive).
 * For example, the Slice ["A" .. "B") contains all keys that are greater than or equal to "A" and
 * strictly less than "B".
 *
 * The high key value may be the [[InfinitySliceKey]] sentinel, which sorts after any other
 * key. For example, the Slice ["M" .. ∞) contains all keys that are greater than or equal to "M".
 *
 * There is no representation of an empty Slice, as per the requirement of the type, `lowInclusive`
 * must be less than `highExclusive`.
 *
 * @param lowInclusive the inclusive lower bound for the Slice
 * @param highExclusive the exclusive upper bound for the Slice
 */
final class Slice(val lowInclusive: SliceKey, val highExclusive: HighSliceKey)
    extends Ordered[Slice] {
  require(
    lowInclusive < highExclusive,
    s"Low key must be less than high key: " +
    s"${lowInclusive.toDetailedDebugString} >= ${highExclusive.toDetailedDebugString}"
  )

  /** Returns whether `key` is contained in this Slice. */
  def contains(key: SliceKey): Boolean = lowInclusive <= key && key < highExclusive

  /** Returns whether `Slice` is contained in this Slice. */
  def contains(slice: Slice): Boolean = {
    lowInclusive <= slice.lowInclusive && highExclusive >= slice.highExclusive
  }

  /** Compares this to other by [[lowInclusive]] then by [[highExclusive]]. */
  override def compare(other: Slice): Int = {
    val lowCmp: Int = lowInclusive.compare(other.lowInclusive)
    if (lowCmp != 0) {
      lowCmp
    } else {
      highExclusive.compare(other.highExclusive)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: Slice =>
      this.lowInclusive == that.lowInclusive && this.highExclusive == that.highExclusive
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(lowInclusive, highExclusive)

  override def toString: String = s"[$lowInclusive .. $highExclusive)"
}

object Slice {

  /** A Slice containing all keys. */
  val FULL: Slice = Slice(SliceKey.MIN, InfinitySliceKey)

  /**
   * REQUIRES: lowInclusive < highExclusive
   *
   * Creates a closed-open Slice containing all keys `key` such that
   * `lowInclusive <= key < highExclusive`.
   */
  def apply(lowInclusive: SliceKey, highExclusive: HighSliceKey): Slice = {
    new Slice(lowInclusive, highExclusive)
  }

  /** Creates an open-ended Slice containing all keys `key` such that `lowInclusive <= key`. */
  def atLeast(lowInclusive: SliceKey): Slice = Slice(lowInclusive, InfinitySliceKey)
}
