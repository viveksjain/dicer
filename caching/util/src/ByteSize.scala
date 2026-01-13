package com.databricks.caching.util

import java.lang.Long.numberOfTrailingZeros
import java.math.RoundingMode
import java.text.DecimalFormat

import scala.util.matching.Regex

import com.google.common.math.LongMath

import com.databricks.caching.util.ByteSize.{ByteSizeUnit, CANONICAL_UNITS, NON_CANONICAL_UNITS}

/**
 * Represents a measurement in bytes. This class is a wrapper around a `Long` value that supports
 * human-readable and approximate string representations of the size using standard storage units
 * (e.g., KiB and MiB). The recommended way to create a [[ByteSize]] literal is to use the factory
 * method with a unit, e.g.
 * {{{
 * ByteSize(42, ByteSize.KIBIBYTE)
 * }}}
 *
 * @param byteCount number of bytes
 */
case class ByteSize(byteCount: Long) extends AnyVal with Ordered[ByteSize] {

  /**
   * Returns a human-readable string representation of [[byteCount]].
   *
   * The size is rounded down to the nearest unit, where units includes B, KiB, MiB, GiB, TiB, PiB,
   * and EiB. For example:
   *
   * {{{
   * ByteSize(2048).toReadableString == "2 KiB"  // the amount and unit are separated by a space
   * ByteSize((1.5 * 1024).toLong).toReadableString == "1.5 KiB"  // rounded down to nearest unit
   * ByteSize((18.532 * Ki).toLong) == "18.53 KiB"  // rounded to two decimal places
   * }}}
   *
   * @note if the input size is negative, returns a negative sign plus the readable string for the
   * size. (i.e. -1024 is converted to "-1.0 KiB".).
   */
  def toReadableString: String = {
    if (byteCount == 0) {
      // Zero cannot be the parameter of `LongMath.log2`, so make this a special case.
      "0 B"
    } else if (byteCount == Long.MinValue) {
      // Long cannot represent the absolute value of Long.MinValue, return a pre-computed result.
      "-8 EiB"
    } else if (byteCount < 0) {
      // Add a "-" prefix and compute the readable string for the absolute value of the size.
      s"-${ByteSize(-byteCount).toReadableString}"
    } else {
      // Compute the nearest unit boundary, round value down to the boundary and format the result.
      val unit: ByteSizeUnit = CANONICAL_UNITS(LongMath.log2(byteCount, RoundingMode.DOWN) / 10)
      val amount: Double = byteCount.toDouble / unit.byteCount

      // Formats for ByteSize decimal representation.
      // 1. The number should have commas for thousands separators. (i.e. 1234 is formatted to
      //    1,234)
      // 2. The number should have up to two decimal places. (i.e. 3.1415 is formatted to 3.14)
      // 3. The leading zero should not be omitted if the integer part is 0. (i.e. 0.17 is 0.17)
      // Note: Java DecimalFormat is not thread-safe, so we construct a new one for each call.
      val amountStr: String = new DecimalFormat("#,##0.##").format(amount)
      s"$amountStr ${unit.name}"
    }
  }

  /**
   * Converts [[byteCount]] into a string consisting of an integer followed by a storage unit (B,
   * KB, KiB, MB, MiB, etc.).
   * In contrast with [[toReadableString()]], these strings do NOT discard any bits and there's
   * no rounding performed.
   * Both sets of canonical (KiB, MiB, etc.) and non-canonical (KB, MB) storage units are used and
   * the specific unit returned is based on whichever one yields the smallest numerical value.
   * Examples:
   *
   * {{{
   * ByteSize(1).toString() == "1 B"
   * ByteSize(2048).toString() == "2 KiB"
   * ByteSize(2000).toString() == "2 KB"
   * ByteSize(-3 * 1024 * 1024).toString() == "-3 MiB"
   * ByteSize(-3 * 1000 * 1000).toString() == "-3 MB"
   * ByteSize(1025).toString() == "1025 B"  // no rounding, so B used instead of KIB or KB
   * ByteSize((1.5 * 1024 * 1024).toLong) == "1536 KiB"
   * ByteSize(5 * 5 * 5 * 1024) == "125 KiB" // Prefer "125 KiB" to "128 KB" since 125 < 128
   * }}}
   */
  override def toString: String = {
    if (byteCount == 0) {
      // Zero has no obvious unit, so arbitrarily use B.
      return "0 B"
    }
    // Find the largest possible canonical unit that does not discard any bits.
    var unitIndex: Int = numberOfTrailingZeros(byteCount) / 10
    var unit: ByteSizeUnit = CANONICAL_UNITS(unitIndex)

    // See if we can do any better with non-canonical units. Note: `unitIndex` is the index of the
    // smallest non-canonical unit that has greater magnitude than the chosen canonical unit. For
    // `unitIndex = 0`, KB > B. For `unitIndex = 1`, MB > KiB. And so forth.
    while (unitIndex < NON_CANONICAL_UNITS.size
      && byteCount % NON_CANONICAL_UNITS(unitIndex).byteCount == 0) {
      unit = NON_CANONICAL_UNITS(unitIndex)
      unitIndex += 1
    }
    s"${byteCount / unit.byteCount} ${unit.name}"
  }

  /**
   * Returns the sum of the current ByteSize and another ByteSize `that`.
   *
   * @throws ArithmeticException If the result in bytes flows over the range of Long.
   */
  def +(that: ByteSize): ByteSize = {
    ByteSize(LongMath.checkedAdd(this.byteCount, that.byteCount))
  }

  /**
   * Returns the result of the current ByteSize subtracted by another ByteSize `that`.
   *
   * @throws ArithmeticException If the result in bytes flows over the range of Long.
   */
  def -(that: ByteSize): ByteSize = {
    ByteSize(LongMath.checkedSubtract(this.byteCount, that.byteCount))
  }

  /** Returns true iff [[byteCount]] is positive. */
  def isPositive: Boolean = {
    byteCount > 0
  }

  /** Returns true iff [[byteCount]] is non-negative, i.e. `>= 0`. */
  def isNonNegative: Boolean = {
    byteCount >= 0
  }

  override def compare(that: ByteSize): Int = {
    this.byteCount.compare(that.byteCount)
  }
}

object ByteSize {

  /** Creates a [[ByteSize]] with the given units, e.g. `ByteSize(5, ByteSize.KIBIBYTE)`. */
  def apply(count: Long, unit: ByteSizeUnit): ByteSize = ByteSize(count * unit.byteCount)

  /**
   * Constants for canonical units that can be used to initialize [[ByteSize]], and also used inside
   * [[fromString]]. See [[apply(count: Long, unit: ByteSizeUnit)*]].
   *
   * Note that we intentionally don't provide constants for non-canonical units to avoid confusion,
   * as readers might think e.g. `KILOBYTE` is 1024 bytes when in fact it is 1000.
   */
  val BYTE = new ByteSizeUnit("B", 1)
  val KIBIBYTE = new ByteSizeUnit("KiB", 1 << 10)
  val MEBIBYTE = new ByteSizeUnit("MiB", 1 << 20)
  val GIBIBYTE = new ByteSizeUnit("GiB", 1 << 30)
  val TEBIBYTE = new ByteSizeUnit("TiB", 1L << 40)
  val PEBIBYTE = new ByteSizeUnit("PiB", 1L << 50)
  val EXBIBYTE = new ByteSizeUnit("EiB", 1L << 60)

  /** Describes a byte size unit, e.g., `Unit("KiB", 1024)` for kibibytes. */
  class ByteSizeUnit private[ByteSize] (
      private[ByteSize] val name: String,
      private[ByteSize] val byteCount: Long)

  /** Canonical units. The magnitude of the unit at index `i` is `1024^i`. */
  private val CANONICAL_UNITS: Vector[ByteSizeUnit] =
    Vector(BYTE, KIBIBYTE, MEBIBYTE, GIBIBYTE, TEBIBYTE, PEBIBYTE, EXBIBYTE)

  /** Non-canonical units. The magnitude of the unit at index `i` is `1000^{i+1}`. */
  private val NON_CANONICAL_UNITS: Vector[ByteSizeUnit] =
    Vector("MB", "GB", "TB", "PB", "EB").scanLeft(new ByteSizeUnit("KB", 1000)) {
      (previous: ByteSizeUnit, name: String) =>
        new ByteSizeUnit(name, previous.byteCount * 1000)
    }

  /** Map from unit names ([[ByteSizeUnit.name]]) to units. */
  private val UNITS_BY_NAME: Map[String, ByteSizeUnit] =
    (CANONICAL_UNITS ++ NON_CANONICAL_UNITS).map { unit: ByteSizeUnit =>
      unit.name -> unit
    }.toMap

  /** Regex pattern for bytes strings (see [[fromString()]]). */
  private val REGEX: Regex = s"^(0|-?[1-9][0-9]*)\\s?(${UNITS_BY_NAME.keys.mkString("|")})$$".r

  /**
   * Given a string representation of the size (e.g. "1KB"), returns the number of bytes.
   *
   * The readable string is in the below format:
   * 1. Starting with an integer consists of one or more digits, with no leading zeroes.
   * 2. Followed by zero or one empty space.
   * 3. memory unit in upper case or in IEEE 1541-2002 format. (e.g. "GB" or "GiB") For example:
   * "300MB", "300 MB", and "300 MiB" are in correct formats.
   * "0", "0KB", "02KB", "20 mb", "20gb", and "1.5MB" are in wrong formats.
   * See [[REGEX]] for the regex pattern.
   *
   * @throws IllegalArgumentException if the memory specifier is in an invalid format or too large.
   */
  def fromString(sizeStr: String): ByteSize = sizeStr match {
    case REGEX(amountStr: String, unitName: String) =>
      val amount: Long = amountStr.toLong

      // `REGEX` ensures that `unitName` is in `UNITS_BY_NAME`, which is why the following lookup is
      // safe.
      val unit: ByteSizeUnit = UNITS_BY_NAME(unitName)
      val bytes = try {
        LongMath.checkedMultiply(amount, unit.byteCount)
      } catch {
        case exception: ArithmeticException =>
          val message: String = if (amount > 0) {
            s"The input memory size must be less than 8 EiB. Input size: $sizeStr."
          } else {
            s"The input memory size must be greater than or equal to -8 EiB. Input size: $sizeStr."
          }
          throw new IllegalArgumentException(message, exception)
      }
      ByteSize(bytes)
    case _ =>
      throw new IllegalArgumentException(
        s"The input memory size is not in the correct format. Input size: $sizeStr. " +
        "The correct format is of the form \"300MB\" or \"300MiB\"."
      )
  }
}
