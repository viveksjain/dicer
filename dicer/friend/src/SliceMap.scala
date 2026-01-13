package com.databricks.dicer.friend

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.VectorBuilder

import com.databricks.caching.util.AssertMacros.iassert
import com.databricks.dicer.common.SliceHelper.RichSlice
import com.databricks.dicer.external.{HighSliceKey, InfinitySliceKey, Slice, SliceKey}

/**
 * REQUIRES: entries are ordered by `slice.lowInclusive`, disjoint, and cover the full SliceKey
 * space from "" to ∞.
 *
 * A data structure supporting efficient lookup of the entry that contains a Slice key.
 *
 * Immutable and thread-safe. (Note that `immutable.Vector` is only thread-safe after construction,
 * and it is technically possible for callers of the `SliceMap` constructor to pass an unsafe
 * reference to a vector. To be pedantic, `SliceMap` is thread-safe if the code constructing it is
 * also thread-safe, but accesses do not need to be synchronized. It's a shame we need to think
 * about such things in Scala, but writing this comment was still more pleasant than requiring a
 * fully thread-safe collection type like Guava's ImmutableList, which is not ergonomic in Scala
 * code. See https://github.com/scala/bug/issues/7838 for details.)
 *
 * @param entries ordered, disjoint entries covering the full key space.
 * @param getSlice function applied to entries to get the corresponding Slices.
 */
class SliceMap[T](val entries: immutable.Vector[T], private[dicer] val getSlice: T => Slice) {
  SliceMap.validateCompleteSlices(entries, getSlice)

  /** Returns a new [[SliceMap]], applying the transformation `f` to each of the entries. */
  def map[U](getSlice: U => Slice)(f: T => U): SliceMap[U] = {
    new SliceMap[U](entries.map(f), getSlice)
  }

  /**
   * Looks up the entry in this map containing `key`. Guaranteed to succeed because of the class
   * invariants.
   */
  def lookUp(key: SliceKey): T =
    entries(SliceMap.findIndexInOrderedDisjointEntries(entries, key, getSlice))

  override def equals(obj: Any): Boolean = obj match {
    case that: SliceMap[T] => this.entries == that.entries
    case _ => false
  }

  override def hashCode(): Int = entries.hashCode()

  override def toString: String = entries.mkString("{", ", ", "}")
}

object SliceMap {

  /**
   * REQUIRES: See REQUIRES clause on the [[SliceMap]] constructor.
   *
   * Creates a [[SliceMap]] with [[Slice]] entries.
   */
  def ofSlices(entries: immutable.Vector[Slice]): SliceMap[Slice] = {
    new SliceMap(entries, SLICE_ACCESSOR)
  }

  val SLICE_ACCESSOR: Slice => Slice = { slice: Slice =>
    slice
  }

  /**
   * REQUIRES: See REQUIRES clause on the [[SliceMap]] constructor.
   *
   * Creates a [[SliceMap]] with [[GapEntry]] entries.
   */
  def ofGapEntries[T](
      entries: immutable.Vector[GapEntry[T]],
      getSlice: T => Slice): SliceMap[GapEntry[T]] = {
    new SliceMap(entries, getSlice = {
      case GapEntry.Gap(slice: Slice) => slice
      case GapEntry.Some(entry: T) => getSlice(entry)
    })
  }

  /**
   * REQUIRES: See REQUIRES clause on the [[SliceMap]] constructor.
   *
   * Creates a [[SliceMap]] with [[IntersectionEntry]] entries.
   */
  def ofIntersectionEntries[T, U](
      entries: immutable.Vector[IntersectionEntry[T, U]]): SliceMap[IntersectionEntry[T, U]] = {
    new SliceMap(entries, INTERSECTION_ENTRY_ACCESSOR)
  }

  val INTERSECTION_ENTRY_ACCESSOR: IntersectionEntry[_, _] => Slice = {
    entry: IntersectionEntry[_, _] =>
      entry.slice
  }

  /**
   * An entry in a Slice map created using [[SliceMap.intersectSlices()]].
   *
   * @param slice the intersection of `leftEntry.getSlice` and `rightEntry.getSlice`
   * @param leftEntry zipped entry from the left-hand Slice map.
   * @param rightEntry zipped entry from the right-hand Slice map.
   */
  case class IntersectionEntry[T, U](slice: Slice, leftEntry: T, rightEntry: U)

  /**
   * Returns intersections between entries in `left` and `right`. We illustrate sample `left` and
   * `right` inputs and the expected `result` below:
   *
   * <pre>
   * left:     | A            | B       | C       | D                    |
   * right:    | 1    | 2                               | 3    | 4       |
   * result:   | A,1  | A,2   | B,2     | C,2     | D,2 | D,3  | D,4     |
   * </pre>
   *
   * As you can see, the entry `A` on the left intersects entries `1` and `2` on the right, so the
   * result includes intersection entries `A,1` and `A,2`.
   */
  def intersectSlices[T, U](
      left: SliceMap[T],
      right: SliceMap[U]): SliceMap[SliceMap.IntersectionEntry[T, U]] = {
    val builder = immutable.Vector.newBuilder[SliceMap.IntersectionEntry[T, U]]

    // Use indices into left and right as iterators, since Java iterators are hard to code against.
    // Recall that both of the maps are ordered, complete, and disjoint (ordered partition of the
    // key space). We exploit this feature of the maps, advancing through the key space from "" to
    // ∞, and preserving the invariant that the Slices on the left and the right are always
    // intersecting.
    var leftIt = 0
    var rightIt = 0
    while (leftIt < left.entries.size) {
      val leftEntry: T = left.entries(leftIt)
      val leftSlice: Slice = left.getSlice(leftEntry)
      iassert(
        rightIt < right.entries.size,
        s"Left and right must advance past their last ∞ Slices during the same iteration: " +
        s"leftEntry=$leftEntry, right=$right"
      )
      val rightEntry: U = right.entries(rightIt)
      val rightSlice: Slice = right.getSlice(rightEntry)

      // Find the intersection of the left- and right- hand entries, which must be defined because
      // of the invariant described above.
      val intersectionOpt: Option[Slice] = leftSlice.intersection(rightSlice)
      iassert(
        intersectionOpt.isDefined,
        s"Slices must cover the full key space and iterators must advance in a coordinated way:" +
        s"leftEntry=$leftEntry, rightEntry=$rightEntry"
      )
      val intersection: Slice = intersectionOpt.get
      builder += IntersectionEntry(intersection, leftEntry, rightEntry)

      // Advance left and right iterators if the current entries have been consumed so that we
      // preserve the invariant that the Slices at `leftIt` and `rightIt` are intersecting and so
      // that we make progress.
      if (leftSlice.highExclusive == intersection.highExclusive) {
        leftIt += 1
      }
      if (rightSlice.highExclusive == intersection.highExclusive) {
        rightIt += 1
      }
    }
    iassert(
      leftIt == left.entries.size && rightIt == right.entries.size,
      s"Inputs not fully consumed: left=$left, right=$right, leftIt=$leftIt, rightIt=$rightIt"
    )
    SliceMap.ofIntersectionEntries(builder.result())
  }

  /**
   * An entry in a Slice map that is either a [[GapEntry.Gap]] or [[GapEntry.Some]] instance of
   * [[T]].
   */
  sealed trait GapEntry[+T] {

    /**
     * Returns whether this entry has [[GapEntry.Some]] value (true) or represents a
     * [[GapEntry.Gap]] (false).
     */
    def isDefined: Boolean

    /**
     * Returns the value for this entry when [[isDefined]] is true. Otherwise, throws
     * [[NoSuchElementException]].
     */
    def get: T
  }

  object GapEntry {

    /** Represents a Slice in the map for which a `T` instance is defined. */
    case class Some[+T](value: T) extends GapEntry[T] {
      override def isDefined: Boolean = true
      override def get: T = value
    }

    /** Represents a Slice in the map for which there is no `T` instance. */
    case class Gap[+T](slice: Slice) extends GapEntry[T] {
      override def isDefined: Boolean = false
      override def get: T = throw new NoSuchElementException("GapEntry.Gap.get")
    }
  }

  /**
   * REQUIRES: `entries` are disjoint and ordered (no pair of entries has overlapping slices).
   *
   * Creates a Slice map given entries that may not cover the entire key space. The provided entries
   * are wrapped in [[GapEntry.Some]] entries. Gaps in the key space are indicated by
   * [[GapEntry.Gap]] entries.
   */
  def createFromOrderedDisjointEntries[T](
      entries: Seq[T],
      getSlice: T => Slice): SliceMap[GapEntry[T]] = {
    val builder = immutable.Vector.newBuilder[GapEntry[T]]

    // Cursor tracking our position in the key space, which we're filling from MIN to Infinity.
    var cursor: HighSliceKey = SliceKey.MIN
    for (entry: T <- entries) {
      // Fill gap between this entry and the previous entry if necessary.
      if (cursor.isFinite) {
        val cursorKey: SliceKey = cursor.asFinite
        val lowKey: SliceKey = getSlice(entry).lowInclusive
        if (lowKey > cursorKey) {
          // Fill in a gap in the key space.
          builder += GapEntry.Gap(Slice(cursorKey, lowKey))
        }
      }
      // Append the current entry and advance the cursor.
      builder += GapEntry.Some(entry)
      cursor = getSlice(entry).highExclusive
    }
    // If the cursor is not at the end of the key space, fill in the gap at the end.
    if (cursor.isFinite) {
      val highExclusive: SliceKey = cursor.asFinite
      builder += GapEntry.Gap(Slice.atLeast(highExclusive))
    }
    // Note that the SliceMap constructor will check for overlaps in Slices (which is why we don't
    // bother checking for that requirement in the current function).
    SliceMap.ofGapEntries(builder.result(), getSlice)
  }

  /**
   * REQUIRES: entries are ordered by `slice.lowInclusive` and are disjoint (non-overlapping).
   *
   * Returns the index of the entry in `entries` containing `key`, or -1 if none contains the given
   * key.
   */
  def findIndexInOrderedDisjointEntries[T](
      entries: Seq[T],
      key: SliceKey,
      getSlice: T => Slice): Int = {

    /**
     * Recursive binary search implementation returning the index of the entry containing `key` from
     * indices in `[begin, end)`.
     */
    @tailrec def search(begin: Int, end: Int): Int = {
      if (begin >= end) {
        return -1
      }
      // Get the midpoint index, which is floor((begin + end) / 2), or in overflow-free integer math
      // (for the admittedly implausible case `entries` contains more than 1 << 30 entries):
      val mid: Int = (begin + end) >>> 1

      // Determine if `key` is before, after, or contained in the Slice at the midpoint of our
      // search.
      val midSlice: Slice = getSlice(entries(mid))
      val (newBegin, newEnd): (Int, Int) = {
        if (key < midSlice.lowInclusive) {
          // The key must be before the slice at mid, so its index must be in [begin, mid).
          (begin, mid)
        } else
          midSlice.highExclusive match {
            case highExclusive: SliceKey if key >= highExclusive =>
              // The key must be after the slice at mid, so its index must be in [mid+1, end).
              (mid + 1, end)
            case _ =>
              // In this `case` block, we know `key >= slice.lowInclusive` (negation of the `if`
              // condition above) and that `key < slice.highExclusive` (negation of first `case`).
              // Therefore, `key` is contained in `slice`, and we can return  the index of the
              // corresponding entry. Because `entries` are disjoint, we know that no other entry
              // may also contain `key`.
              return mid
          }
      }
      search(newBegin, newEnd)
    }
    search(begin = 0, end = entries.size)
  }

  /**
   * REQUIRES: entry slices are ordered by `slice.lowInclusive`, disjoint, and cover the full
   * SliceKey space from "" to ∞.
   */
  def validateCompleteSlices[T](entries: Iterable[T], getSlice: T => Slice): Unit = {
    require(entries.nonEmpty, "Must not be empty")
    var prevSlice: Slice = getSlice(entries.head)
    require(
      prevSlice.lowInclusive == SliceKey.MIN,
      s"""First entry must start at "": actual=$prevSlice"""
    )
    for (entry <- entries.tail) {
      val slice: Slice = getSlice(entry)
      require(prevSlice.highExclusive.isFinite, "Only last entry may have Infinity upper bound")
      val prevHighExclusive: SliceKey = prevSlice.highExclusive.asFinite

      // The Slice must start where the previous Slice ended.
      val boundaryCmp: Int = prevHighExclusive.compare(slice.lowInclusive)
      require(boundaryCmp >= 0, s"Gap between successive entries: $prevSlice and $slice")
      require(boundaryCmp <= 0, s"Overlap between successive entries: $prevSlice and $slice")
      prevSlice = slice
    }
    require(
      prevSlice.highExclusive == InfinitySliceKey,
      s"Last entry must have Infinity upper bound: actual=$prevSlice"
    )
  }

  /**
   * Given a SliceMap, returns a new SliceMap with all adjacent entries that have equal values
   * coalesced. For example,
   *
   * <pre>
   * sliceMap:     | A            | A       | C       | D       | D             |
   * result:       | A                      | C       | D                       |
   * </pre>
   *
   * @param sliceMap The original SliceMap to coalesce slices for.
   * @param setSlice The function to update a SliceMap entry to contain a different Slice. It will
   *                 also be used for equality comparison between SliceMap entries: Two SliceMap
   *                 entries `left` and `right` are considered equal if
   *                 `setSlice(left, Slice.FULL) == setSlice(right, Slice.FULL)`.
   *
   */
  def coalesceSlices[T](sliceMap: SliceMap[T], setSlice: (T, Slice) => T): SliceMap[T] = {
    val oldEntries: Vector[T] = sliceMap.entries
    val newEntries = new VectorBuilder[T]

    val getSlice: T => Slice = sliceMap.getSlice

    def equalInValue(left: T, right: T): Boolean = {
      setSlice(left, Slice.FULL) == setSlice(right, Slice.FULL)
    }

    // The variable used for combining the adjacent entries that have the same value while
    // traversing the SliceMap entries.
    var currentEntry: T = oldEntries.head

    for (nextEntry: T <- oldEntries.tail) {
      if (equalInValue(currentEntry, nextEntry)) {
        // If the next entry has the same value with the current one, combine it with the current
        // one.
        val newSlice = Slice(getSlice(currentEntry).lowInclusive, getSlice(nextEntry).highExclusive)
        currentEntry = setSlice(currentEntry, newSlice)
      } else {
        newEntries += currentEntry
        currentEntry = nextEntry
      }
    }

    newEntries += currentEntry

    new SliceMap[T](newEntries.result(), getSlice)
  }

  object forTest {
    def validateCompleteSlices(slices: Seq[Slice]): Unit = {
      SliceMap.validateCompleteSlices(slices, getSlice = { slice: Slice =>
        slice
      })
    }
  }
}
