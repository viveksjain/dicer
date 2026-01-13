package com.databricks.caching.util
import io.grpc.Status

import com.databricks.testing.DatabricksTest

class StatusOrSuite extends DatabricksTest {

  test("OK StatusOr") {
    // Test plan: Create a StatusOr from a value and verify that methods work as expected.
    val s = StatusOr.success(5)
    assert(s.isOk)
    assert(s.status == Status.OK)
    assert(s.get == 5)
    assert(s.getOrElse(10) == 5)
    assert(s.toString.contains("5"))
  }

  test("Error StatusOr") {
    // Test plan: Create a StatusOr from an error Status and verify that methods work as expected.
    val s = StatusOr.Failure[Int](Status.OUT_OF_RANGE)
    assert(!s.isOk)
    assert(s.status == Status.OUT_OF_RANGE)
    assertThrows[NoSuchElementException] {
      s.get
    }
    assert(s.getOrElse(10) == 10)
    assert(s.toString.contains("OUT_OF_RANGE"))
  }

  test("StatusOr.error throws with Status.OK") {
    // Test plan: When StatusOr.error is called with an OK status, it should throw an exception.
    assertThrows[IllegalArgumentException] {
      StatusOr.error(Status.OK)
    }
  }

  test("StatusOr equality") {
    // Test plan: Construct different instances of `StatusOr` with same and different elements
    // and verify that equals and hashCode work as expected.

    TestUtils.checkEquality(
      groups = Seq(
        Seq(StatusOr.success(1), StatusOr.success(1)),
        Seq(StatusOr.error(Status.ABORTED), StatusOr.error(Status.ABORTED)),
        // `Status` uses referential equality.
        Seq(StatusOr.error(Status.ABORTED.withDescription("test"))),
        Seq(StatusOr.error(Status.ABORTED.withDescription("test")))
      )
    )
  }
}
