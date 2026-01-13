package com.databricks.caching.util

import java.time.Instant

/**
 * A version number that may be interpreted as the number of milliseconds since the Unix epoch.
 * @param value The underlying value of the version number.
 */
class UnixTimeVersion private (val value: Long) extends AnyVal with Ordered[UnixTimeVersion] {

  override def toString: String = s"$value ($toTime)"

  /**
   * Returns the value of the version number as an [[Instant]], assuming that the number tracks the
   * number of millis since the Unix epoch.
   */
  def toTime: Instant = Instant.ofEpochMilli(value)

  override def compare(that: UnixTimeVersion): Int = {
    this.value.compare(that.value)
  }

}

object UnixTimeVersion {

  /** The minimum, legal [[UnixTimeVersion]]. */
  val MIN: UnixTimeVersion = UnixTimeVersion(0)

  implicit def apply(value: Long): UnixTimeVersion = {
    new UnixTimeVersion(value)
  }

  implicit def apply(value: Int): UnixTimeVersion = {
    UnixTimeVersion(value.toLong)
  }

}
