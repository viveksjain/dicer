package com.databricks.dicer.friend

import java.util
import java.util.function.BiFunction

import com.google.common.collect.{BoundType, Range, TreeRangeMap}

import com.databricks.dicer.external.{InfinitySliceKey, Slice, SliceKey}
import com.databricks.dicer.friend.MutableSliceMap.{MapIterator, rangeFromSlice, sliceFromRange}

/**
 * A data structure representing a mapping from disjoint [[Slice]]s to non-null values, and lookups
 * of [[SliceKey]]s. The Slices contained in the data structure do not have to cover the full range.
 * It is similar to (and implemented using) Guava's `RangeMap`, but specialized for Slices and
 * with automatic coalescing for [[put]]/[[merge]].
 */
class MutableSliceMap[V] extends Iterable[(Slice, V)] {

  /**
   * The underlying RangeMap storing the mapping. It maintains the following invariants:
   *  - Ranges follow Slice bounds (i.e. they are closed-open or at-least). This property is
   *    maintained because the slices passed to [[put]]/[[merge]] follow it, and if they intersect
   *    with any existing slices in the map, the resulting splits that are created also follow the
   *    same slice bounds.
   *  - Contiguous entries have different values (otherwise they are coalesced during
   *    [[put]]/[[merge]]).
   */
  private val entries = TreeRangeMap.create[SliceKey, V]()

  /** Type alias representing an entry in [[entries]]. */
  type Entry = util.Map.Entry[Range[SliceKey], V]

  /** Return an ordered iterator over all entries in this map. */
  override def iterator: Iterator[(Slice, V)] = {
    val entrySet: util.Set[Entry] = entries.asMapOfRanges().entrySet()
    new MapIterator[V](entrySet.iterator())
  }

  /**
   * Get the number of unique entries in this map. Note that this is after splitting any overlapping
   * entries, and coalescing any adjacent common values.
   */
  override def size: Int = entries.asMapOfRanges().size()

  /** Looks up the entry in this map containing `key`. */
  def lookUp(key: SliceKey): Option[(Slice, V)] = {
    val entryOpt: Option[Entry] = Option(entries.getEntry(key))
    entryOpt.map(entry => (sliceFromRange(entry.getKey), entry.getValue))
  }

  /**
   * Map the given `slice` to `value`. If it overlaps with existing slices in this map, `combiner`
   * will be called with parameters (oldValue, newValue). If `combiner` returns null then it will
   * clear that part of the map. Note that it may be called multiple times if there are multiple
   * overlapping slices. Any contiguous slices that end up mapping to equivalent values will be
   * coalesced.
   *
   * Examples:
   * {{{
   * Map:     |------|     |---|  |--------|
   * Slice:        |---------|
   * Overlap:      |-|     |-|
   * Result:  |----|-|-----|-|-|  |--------|
   * }}}
   * `combiner` will be called twice, for the 2 overlapping slices shown. The result will be as
   * shown, but any contiguous slices with the same value will be coalesced.
   *
   * {{{
   * Map:     |------|      |----|
   * Slice:             |---|
   * Result:  |------|  |---|----|
   * }}}
   * As the new slice does not overlap with anything, `combiner` will not be called. The result will
   * be as shown, but any contiguous slices with the same value will be coalesced.
   */
  def merge(slice: Slice, value: V, combiner: (V, V) => V): Unit = {
    // Note on `combiner` and null: per Guava docs, `value` should not be null, but the combiner can
    // return null. The idiomatic way in Scala would be to make `combiner` return `Option[V]`, but
    // to then convert it back to null requires `V <: Nullable` which doesn't work when V is
    // `AnyVal`, e.g. `Int`. So we don't disallow null but don't particularly encourage it either.
    val rangeMapCombiner = new BiFunction[V, V, V] {
      override def apply(prevValue: V, newValue: V): V = combiner(prevValue, newValue)
    }
    entries.merge(rangeFromSlice(slice), value, rangeMapCombiner)

    coalesce(slice)
  }

  /**
   * Map the given `slice` to `value`. If it overlaps with existing slices in this map, the
   * overlapping portions will be replaced with `value`. Any contiguous slices that end up mapping
   * to the same value will be coalesced.
   */
  def put(slice: Slice, value: V): Unit = {
    entries.putCoalescing(rangeFromSlice(slice), value)
  }

  /** Clears all the slices and their values in the map. */
  def clear(): Unit = {
    entries.clear()
  }

  /**
   * Coalesce adjacent slice's values that are equal, in the range of all slices touching `slice`
   * (i.e. from the slice with `highExclusive == slice.lowInclusive` up to the slice with
   * `lowInclusive == slice.highExclusive`, if they exist). This method is used to restore the
   * [[entries]] invariant that contiguous entries must have different values.
   */
  private def coalesce(slice: Slice): Unit = {
    // Simply iterate through all entries covered by `slice`, and use `TreeRangeMap.putCoalescing`
    // to re-put all entries and coalesce any contiguous ranges with the same value.
    val subEntriesSet: util.Set[Entry] =
      entries.subRangeMap(rangeFromSlice(slice)).asMapOfRanges().entrySet()
    val subEntries = subEntriesSet.toArray(Array[Entry]())
    for (entry <- subEntries) {
      entries.putCoalescing(entry.getKey, entry.getValue)
    }
  }
}

object MutableSliceMap {

  /** Small wrapper to convert iterator over `Entry(Range[SliceKey], V)` into `(Slice, V)`. */
  private class MapIterator[V](iterator: util.Iterator[util.Map.Entry[Range[SliceKey], V]])
      extends Iterator[(Slice, V)] {
    override def hasNext: Boolean = iterator.hasNext
    override def next(): (Slice, V) = {
      val entry = iterator.next()
      val range: Range[SliceKey] = entry.getKey
      (sliceFromRange(range), entry.getValue)
    }
  }

  /**
   * REQUIRES: `range` bounds follow the semantics of [[Slice]], i.e. it has a closed lower bound,
   * and if it has an upper bound then it is open. This is satisfied by the invariants on `entries`
   * when used in this class.
   *
   * Converts a [[Range]] to an equivalent [[Slice]], or throws [[IllegalArgumentException]] if
   * there is no equivalent representation.
   */
  private def sliceFromRange(range: Range[SliceKey]): Slice = {
    require(range.hasLowerBound)
    require(range.lowerBoundType() == BoundType.CLOSED)
    if (range.hasUpperBound) {
      require(range.upperBoundType() == BoundType.OPEN)
      Slice(range.lowerEndpoint(), range.upperEndpoint())
    } else {
      Slice.atLeast(range.lowerEndpoint())
    }
  }

  /** Converts a [[Slice]] to an equivalent [[Range]]. */
  private def rangeFromSlice(slice: Slice): Range[SliceKey] = slice.highExclusive match {
    case highExclusive: SliceKey => Range.closedOpen(slice.lowInclusive, highExclusive)
    case InfinitySliceKey => Range.atLeast(slice.lowInclusive)
  }
}
