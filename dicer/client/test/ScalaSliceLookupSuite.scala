package com.databricks.dicer.client

import scala.concurrent.Await
import scala.concurrent.duration._
import com.databricks.caching.util.LoggingStreamCallback
import com.databricks.dicer.common.{Assignment, Generation, ProposedSliceAssignment}
import com.databricks.dicer.common.TestSliceUtils._
import com.databricks.dicer.friend.SliceMap


/**
 * Tests for the Scala implementation of the slice lookup driver where the Slicelet is not
 * watching from the Data Plane.
 *
 * @note There is no concept of the 'Data Plane' in the open-source version of Dicer.
 */
class ScalaSliceLookupSuite extends ScalaSliceLookupSuiteBase(watchFromDataPlane = false) {

}
