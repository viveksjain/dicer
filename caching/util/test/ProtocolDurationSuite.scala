package com.databricks.caching.util
import com.databricks.testing.DatabricksTest
import com.databricks.caching.util.test.ProtocolDurationTestDataP
import com.databricks.caching.util.test.ProtocolDurationTestDataP.TestCaseP.Duration

import scala.util.control.NonFatal
import scala.concurrent.duration._

/** Tests that verify the protocol duration can be represented by Scala finite duration. */
class ProtocolDurationSuite extends DatabricksTest {

  private val TEST_DATA: ProtocolDurationTestDataP =
    TestUtils.loadTestData[ProtocolDurationTestDataP](
      "caching/util/test/data/protocol_duration_test_data.textproto"
    )

  test("Valid protocol durations") {
    // Test plan: Verify that all valid protocol durations are accepted.
    for (testCase <- TEST_DATA.validCases) {
      val testName: String = testCase.name.getOrElse(fail("expect a name for each case"))
      try {
        testCase.duration match {
          case Duration.Empty => fail(s"expect a non-empty duration for $testName")
          case Duration.Nanos(value) => value.nanos
          case Duration.Micros(value) => value.micros
          case Duration.Millis(value) => value.millis
          case Duration.Seconds(value) => value.seconds
        }
      } catch {
        case NonFatal(e) =>
          fail(s"expect $testName to succeed, but got exception: ${e.getMessage}", e)
      }
    }
  }

  test("Invalid protocol durations") {
    // Test plan: Verify that all invalid protocol durations are rejected with illegal argument
    // exceptions.
    for (testCase <- TEST_DATA.invalidCases) {
      val testName: String = testCase.name.getOrElse(fail("expect a name for each case"))
      try {
        testCase.duration match {
          case Duration.Empty => fail(s"expect a non-empty duration for $testName")
          case Duration.Nanos(value) => value.nanos
          case Duration.Micros(value) => value.micros
          case Duration.Millis(value) => value.millis
          case Duration.Seconds(value) => value.seconds
        }
        fail(s"expect $testName to fail, but it succeeded")
      } catch {
        case _: IllegalArgumentException => // Expected exception, do nothing.
        case NonFatal(e) =>
          fail(s"expect $testName to fail with IllegalArgumentException, but got: ${e.getMessage}")
      }
    }
  }
}
