package com.databricks.caching.util

import com.databricks.conf.trusted.LocationConf

/**
 * Utility to help with testing WhereAmIHelper.
 */
object WhereAmITestUtils {

  /** Executes `thunk` with the given `locationConf` as the location configuration singleton. */
  @inline def withLocationConfSingleton[T](locationConf: LocationConf)(thunk: => T): T = {
    LocationConf.testSingleton(locationConf)
    try {
      thunk
    } finally LocationConf.restoreSingletonForTest()
  }

}
