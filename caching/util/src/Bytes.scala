package com.databricks.caching.util

import com.google.protobuf.ByteString

/**
 * Utilities for interacting with byte arrays as lexicographically comparable strings, where bytes
 * are assumed to be unsigned.
 */
object Bytes {

  /** Returns whether the given byte is considered a printable ASCII character. */
  def isPrintable(b: Int): Boolean = {
    b >= 32 && b <= 126
  }

  /**
   * Provides a human-readable representation of the given ByteString.
   *
   * @param byteString The ByteString to convert to a String.
   * @param truncateLen If non-negative, the number of bytes to include from the ByteString in the
   *     result. If `byteString` is truncated, the result will end with "...".
   * @param isPrintable A function that decides if a particular byte in the string is printable or
   *     should have its hex code written.
   */
  def toString(byteString: ByteString, truncateLen: Int, isPrintable: Int => Boolean): String = {
    if (byteString.isEmpty || truncateLen == 0) {
      return "\"\"" // Return empty string representation.
    }
    val sb = new StringBuilder()
    // Implementation note: we use the efficient ByteIterator interface provided by ByteString
    // to avoid boxing and allocation.
    val iterator: ByteString.ByteIterator = byteString.iterator()
    var i: Int = 0
    while (iterator.hasNext && (truncateLen < 0 || i < truncateLen)) {
      val b: Int = iterator.nextByte() & 0xFF; // Ensure we treat the byte as unsigned.
      if (isPrintable(b)) {
        sb.append(b.toChar)
      } else {
        sb.append(f"\\x$b%02X")
      }
      i += 1
    }
    if (truncateLen >= 0 && i < byteString.size()) {
      sb.append("...")
    }
    sb.toString()
  }

  /**
   * Provides a human-readable representation of the given ByteString. Printable characters are
   * emitted as is, all others are hex-encoded.
   *
   * @param byteString The ByteString to convert to a String.
   */
  def toString(byteString: ByteString): String = {
    toString(byteString, -1, isPrintable)
  }

  /**
   * Provides a human-readable representation of the given ByteString. Printable characters are
   * emitted as is, all others are hex-encoded.
   *
   * @param byteString The ByteString to convert to a String.
   * @param truncateLen If non-negative, the number of bytes to include from the ByteString in the
   *     result. If `byteString` is truncated, the result will end with "...".
   */
  def toString(byteString: ByteString, truncateLen: Int): String = {
    toString(byteString, truncateLen, isPrintable)
  }

  /**
   * Provides a human-readable representation of the given ByteString.
   *
   * @param byteString The ByteString to convert to a String.
   * @param isPrintable A function that decides if a particular byte in the string is printable or
   *     should have its hex code written.
   */
  def toString(byteString: ByteString, isPrintable: Int => Boolean): String = {
    toString(byteString, -1, isPrintable)
  }
}
