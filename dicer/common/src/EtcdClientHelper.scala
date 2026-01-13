package com.databricks.dicer.common

import com.databricks.caching.util.EtcdClient.{Version => EtcdClientVersion}

/** Utility methods for converting between [[EtcdClientVersion]] and [[Generation]]. */
object EtcdClientHelper {

  /**
   * REQUIRES: `generation.incarnation` is non-loose, and has a store incarnation greater than zero.
   *
   * Returns the [[EtcdClientVersion]] used to represent `generation` in etcd.
   */
  def getVersionFromNonLooseGeneration(generation: Generation): EtcdClientVersion = {
    require(generation.incarnation.isNonLoose, "Incarnation must be non-loose")
    new EtcdClientVersion(generation.incarnation.value, generation.number.value)
  }

  /** Returns the [[Generation]] represented by `version`. */
  def createGenerationFromVersion(version: EtcdClientVersion): Generation = {
    // While we may be tempted to alert if this corresponds to a loose incarnation, the
    // `version` passed through here may represent the version high watermark, which may be
    // bootstrapped to a loose incarnation initially.
    Generation(Incarnation(version.highBits), version.lowBits)
  }
}
