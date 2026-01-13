package com.databricks.dicer.common

import com.databricks.dicer.common.SliceSetImpl.checkSlices
import com.databricks.dicer.external.{Slice, SliceKey}
import com.databricks.dicer.friend.SliceMap
import com.databricks.dicer.friend.SliceMap.GapEntry

/**
 * REQUIRES: `slices` are ordered and disconnected (there are gaps between successive Slices).
 *
 * A [[SliceSetImpl]] is a set of disconnected Slices supporting efficient Slice key containment
 * checks. Its internal representation is canonical: any two sets containing the same Slice keys are
 * represented as the same sequence of ordered, disconnected Slices.
 *
 * Immutable and thread-safe, though see disclaimers in [[SliceMap]] re. [[Vector]].
 *
 * @param slices An ordered and disconnected sequence of Slices.
 */
final class SliceSetImpl(val slices: Vector[Slice]) {
  checkSlices(slices)

  /** Is this set empty? */
  def isEmpty: Boolean = slices.isEmpty

  /** Is this set non-empty? */
  def nonEmpty: Boolean = slices.nonEmpty

  /** Returns whether this set contains the given key. */
  def contains(key: SliceKey): Boolean = {
    // Reuse SliceMap's binary search helper function to find the Slice containing `key`. If no such
    // Slice exists, the function returns -1. Since this set's Slices are ordered and disconnected,
    // they (more than) satisfy the requirements of the helper function.
    SliceMap.findIndexInOrderedDisjointEntries(slices, key, SliceMap.SLICE_ACCESSOR) >= 0
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: SliceSetImpl => this.slices == that.slices
    case _ => false
  }

  override def hashCode(): Int = slices.hashCode()

  override def toString: String = slices.mkString("{", ",", "}")
}
object SliceSetImpl {
  private val EMPTY = new SliceSetImpl(Vector.empty)

  /** Returns an empty set. */
  def empty: SliceSetImpl = EMPTY

  /** Creates a set containing the union of the given Slices. */
  def apply(slices: Slice*): SliceSetImpl = {
    apply(slices: TraversableOnce[Slice])
  }

  /** Creates a set containing the union of the given Slices. */
  def apply(slices: TraversableOnce[Slice]): SliceSetImpl = {
    val builder = newBuilder

    // Sort the Slices, since this is required by the builder.
    for (slice: Slice <- slices.toSeq.sorted) {
      builder.add(slice)
    }
    builder.build()
  }

  /** Returns a new builder. */
  def newBuilder: SliceSetImplBuilder = new SliceSetImplBuilder

  /**
   * Computes the bilateral difference between `left` and `right`. Returns a tuple of Slice sets,
   * where the Slices in the first set contain keys that are only in `left` and the Slices in the
   * second set contain keys that are only in `right`.
   *
   * We illustrate sample `left` and `right` inputs, and the expected `leftOnly` and `rightOnly`
   * results below (not to scale):
   *
   * <pre>
   * left:     [ 0, 10        )          [ 20,   50                      )
   * right:    [ 0, 5 )         [ 12, 30          )          [ 40, 50    )
   * leftOnly:        [ 5, 10 )                   [ 30, 40   )
   * rightOnly:                 [ 12, 20 )
   * </pre>
   */
  def diff(left: SliceSetImpl, right: SliceSetImpl): (SliceSetImpl, SliceSetImpl) = {
    // The `left` and `right` inputs are converted into maps with gaps, then intersected using the
    // existing `SliceMap.intersectSlices` method. The implementation performs some extra copies,
    // but is simpler since it relies on shared logic in SliceMap.
    val leftMap: SliceMap[GapEntry[Slice]] =
      SliceMap.createFromOrderedDisjointEntries(left.slices, SliceMap.SLICE_ACCESSOR)
    val rightMap: SliceMap[GapEntry[Slice]] =
      SliceMap.createFromOrderedDisjointEntries(right.slices, SliceMap.SLICE_ACCESSOR)
    type IntersectionEntry = SliceMap.IntersectionEntry[GapEntry[Slice], GapEntry[Slice]]
    val intersectionMap: SliceMap[IntersectionEntry] = SliceMap.intersectSlices(leftMap, rightMap)

    // Walk through the intersected Slices to find sub-Slice that are just on the left-hand side and
    // just on the right-hand side.
    val leftOnly = Vector.newBuilder[Slice]
    val rightOnly = Vector.newBuilder[Slice]
    for (intersection: IntersectionEntry <- intersectionMap.entries) {
      (intersection.leftEntry, intersection.rightEntry) match {
        case (GapEntry.Some(_), GapEntry.Some(_)) =>
        // Slice is in both left and right, no diff!
        case (GapEntry.Some(_), GapEntry.Gap(_)) =>
          // Slice is in left but not right, so it is a removed Slice.
          leftOnly += intersection.slice
        case (GapEntry.Gap(_), GapEntry.Some(_)) =>
          // Slice is in right but not left, so it is an added Slice.
          rightOnly += intersection.slice
        case (GapEntry.Gap(_), GapEntry.Gap(_)) =>
        // Slice is in neither left nor right, no diff!
      }
    }
    (new SliceSetImpl(leftOnly.result()), new SliceSetImpl(rightOnly.result()))
  }

  /** Checks the requirements for the internal representation used by [[SliceSetImpl]]. */
  @throws[IllegalArgumentException]("if `slices` are not ordered or connected")
  private def checkSlices(slices: Iterable[Slice]): Unit = {
    val it = slices.iterator
    if (!it.hasNext) {
      // Conditions are trivially satisfied.
      return
    }
    var prevSlice: Slice = it.next()
    while (it.hasNext) {
      val slice: Slice = it.next()
      require(
        slice.lowInclusive > prevSlice.highExclusive,
        s"$slice is not strictly after $prevSlice"
      )
      prevSlice = slice
    }
  }
}

/**
 * Builder accumulating ordered (by `lowInlusive` then `maxExclusive`) Slices to create a
 * [[SliceSetImpl]].
 */
final class SliceSetImplBuilder {

  // Implementation note: because Slices are added in order, the builder is able to "coalesce"
  // successive Slices in `coalescedSlice`, and emits the coalesced Slice to the `builder` only when
  // an added Slice is strictly after that coalescing Slice. The following trace of the
  // algorithm shows the internal state of the builder after various example inputs:
  //
  // Input       | coalescedSlice | builder           | Remarks
  // ------------|----------------|-------------------|--------------------------------------------
  // init        | None           | {}                |
  // add([1, 3)) | [1, 3)         | {}                |
  // add([2, 4)) | [1, 4)         | {}                | successive Slices overlap
  // add([4, 5)) | [1, 5)         | {}                | successive Slices are connected
  // add([6, 7)) | [6, 7)         | {[1, 5)}          | gap, emit first coalesced Slice
  // result()    |                | {[1, 5), [6, 7)}  | when done, the coalesced Slice is flushed

  /**
   * Slice that is currently being expanded to include overlapping or connected Slices provided to
   * the Builder.
   */
  private var coalescedSlice: Option[Slice] = None

  /** Vector to which coalesced Slices are emitted. */
  private val builder = Vector.newBuilder[Slice]

  /**
   * REQUIRES: `slice` does not sort before any Slice already added to the builder.
   *
   * Adds all keys in the given `slice` to the builder.
   */
  def add(slice: Slice): this.type = {
    coalescedSlice match {
      case Some(coalescedSlice: Slice) =>
        if (slice.lowInclusive > coalescedSlice.highExclusive) {
          // Flush the coalesced Slice, which is disconnected from the current Slice.
          builder += coalescedSlice
          this.coalescedSlice = Some(slice)
        } else {
          require(
            coalescedSlice <= slice,
            s"Slices must be added to the builder in order: $slice > $coalescedSlice"
          )
          if (coalescedSlice.highExclusive < slice.highExclusive) {
            // Expand the coalescing Slice to include the keys in the current Slice.
            this.coalescedSlice = Some(Slice(coalescedSlice.lowInclusive, slice.highExclusive))
          }
        }
      case None =>
        // First slice added to the builder; stage it.
        coalescedSlice = Some(slice)
    }
    this
  }

  /**
   * Returns a set containing the union of all Slices added to this builder. Defensively clears the
   * builder rather than throwing (which is more complicated than clearing) or returning bogus
   * results if the builder is subsequently used (which would result in subtle bugs in the --
   * admittedly unlikely -- event someone attempted to reuse the builder).
   */
  def build(): SliceSetImpl = {
    coalescedSlice match {
      case Some(coalescedSlice: Slice) =>
        // Flush the last coalescing slice.
        this.builder += coalescedSlice
        val slices: Vector[Slice] = this.builder.result()

        // Clear builder state before returning so that the builder is not left in an invalid state
        // after flushing the last coalesced slice.
        this.coalescedSlice = None
        this.builder.clear()
        new SliceSetImpl(slices)
      case None =>
        // No Slices were added to the builder.
        SliceSetImpl.empty
    }
  }
}
