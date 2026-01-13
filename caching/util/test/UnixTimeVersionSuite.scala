package com.databricks.caching.util
import com.databricks.testing.DatabricksTest

class UnixTimeVersionSuite extends DatabricksTest {
  test("toString contains number and instant representation") {
    // Test plan: verify that UnixTimeVersion.toString contains both the underlying number and its
    // datetime representation when interpreted as milliseconds since the Unix epoch.
    val version = UnixTimeVersion(1716528871000L)

    // Note that we do not specify the exact format of the datetime representation because it's
    // unimportant; we only assert that Instant.toString's representation is included in the debug
    // string. If it were to change underneath one day, this test would be the same.
    assert(version.toString == s"1716528871000 (2024-05-24T05:34:31Z)")
  }
}
