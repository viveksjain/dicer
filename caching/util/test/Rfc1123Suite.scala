package com.databricks.caching.util

import com.databricks.caching.util.TestUtils.loadTestData
import com.databricks.caching.util.test.Rfc1123TestDataP
import com.databricks.testing.DatabricksTest

class Rfc1123Suite extends DatabricksTest {

  private val TEST_DATA: Rfc1123TestDataP =
    loadTestData[Rfc1123TestDataP]("caching/util/test/data/rfc1123_test_data.textproto")

  assert(TEST_DATA.getLongestValidString.length == 63)
  assert(TEST_DATA.getTooLongString.length == 64)

  gridTest("Valid strings")(TEST_DATA.validStrings) { s: String =>
    // Test plan: Verify that the expected strings are considered valid.
    assert(Rfc1123.isValid(s))
  }

  gridTest("Invalid strings")(TEST_DATA.invalidStrings) { s: String =>
    // Test plan: Verify that the expected strings are considered invalid.
    assert(!Rfc1123.isValid(s))
  }
}
