package com.databricks.dicer.common

object LoadMeasurement {

  /**
   * REQUIRES: `load` is non-negative, finite number.
   *
   * Throws [[IllegalArgumentException]] if load value is invalid.
   */
  def requireValidLoadMeasurement(load: Double): Unit = {
    require(!load.isNaN, s"$load must be a number")
    require(load.signum >= 0, s"$load must be non-negative")
    require(!load.isPosInfinity, s"$load must be finite")
  }
}
