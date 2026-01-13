package com.databricks.dicer.demo.common

import com.google.common.hash.Hashing
import com.google.common.primitives.Ints
import com.databricks.dicer.external.{SliceKey, SliceKeyFunction}

/** Common utilities across the demo client and server. */
object DemoCommon {

  /** The target name used by the demo service. */
  val TARGET_NAME: String = "demo-cache-app"

  /** Hash function for creating SliceKeys from integer keys, using FarmHash fingerprint64. */
  object IntKeyHashFunction extends SliceKeyFunction {
    override def apply(applicationKey: Array[Byte]): Array[Byte] = {
      Hashing.farmHashFingerprint64().hashBytes(applicationKey).asBytes
    }
  }

  /**
   * Converts an integer key to a SliceKey. It is critical that the server and client use the same
   * function to compute the slice key.
   */
  def toSliceKey(key: Int): SliceKey = {
    val keyBytes: Array[Byte] = Ints.toByteArray(key)
    SliceKey(keyBytes, IntKeyHashFunction)
  }
}
