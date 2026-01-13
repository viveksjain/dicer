package com.databricks.caching.util

import com.databricks.testing.DatabricksTest

class CancellableSuite extends DatabricksTest {
  test("NO_OP_CANCELLABLE") {
    // Test plan: Sanity check that cancelling a `Cancellable.NO_OP_CANCELLABLE` does not crash.
    Cancellable.NO_OP_CANCELLABLE.cancel()
  }
}
