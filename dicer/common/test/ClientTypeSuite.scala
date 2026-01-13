package com.databricks.dicer.common

import com.databricks.testing.DatabricksTest

class ClientTypeSuite extends DatabricksTest {

  test("toString") {
    // Test plan: Verify that the Clerk client type `toString` returns "clerk" and the Slicelet
    // client type `toString` returns "slicelet".

    assert(ClientType.Clerk.toString == "clerk")
    assert(ClientType.Slicelet.toString == "slicelet")
  }

  test("getMetricLabel") {
    // Test plan: Verify that getMetricLabel returns the correct metric label for both Clerk and
    // Slicelet client types, used for metrics.

    assert(ClientType.Clerk.getMetricLabel == "clerk")
    assert(ClientType.Slicelet.getMetricLabel == "slicelet")
  }

}
