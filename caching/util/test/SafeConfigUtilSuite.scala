package com.databricks.caching.util

import com.databricks.caching.util.TestUtils.assertThrow
import com.databricks.testing.DatabricksTest

class SafeConfigUtilSuite extends DatabricksTest {

  test("Test getNamespaceFromConfigFlagName") {
    // Test plan: run getNamespaceFromConfigFlagName with invalid flag names to verify the function
    // catches invalid flag names. Then run it with valid flag names and verify results are as
    // expected.
    assertThrow[IllegalArgumentException]("does not start with") {
      SafeConfigUtil.getNamespaceFromConfigFlagName("databricks.caching.namespaces.configs.foo")
    }

    assertThrow[IllegalArgumentException]("does not start with") {
      SafeConfigUtil.getNamespaceFromConfigFlagName(
        "databricks.caching.configs.namespaces.foo"
      )
    }

    assertThrow[IllegalArgumentException]("does not start with") {
      SafeConfigUtil.getNamespaceFromConfigFlagName(
        "foo.databricks.softstore.config.namespaces.foo"
      )
    }

    // The deprecated flag prefix doesn't get through the check.
    assertThrow[IllegalArgumentException]("does not start with") {
      SafeConfigUtil.getNamespaceFromConfigFlagName(
        "foo.databricks.softstore.namespaces.config.foo"
      )
    }

    val softclientFlagName = "databricks.softstore.config.softclient"
    val fooFlagName = "databricks.softstore.config.foo"
    SafeConfigUtil.getNamespaceFromConfigFlagName(softclientFlagName) == "softclient"
    SafeConfigUtil.getNamespaceFromConfigFlagName(fooFlagName) == "foo"

    // A namespace contains a "." in its name.
    val someFlagName = "databricks.softstore.config.some.flag"
    SafeConfigUtil.getNamespaceFromConfigFlagName(someFlagName) == "some.flag"
  }

  test("Test getTargetFromConfigFlagName") {
    // Test plan: run getTargetFromConfigFlagName with invalid flag names to verify the function
    // catches invalid flag names. Then run it with valid flag names and verify results are as
    // expected.
    assertThrow[IllegalArgumentException]("does not start with") {
      SafeConfigUtil.getTargetNameFromConfigFlagName("databricks.bad.assigner.targetConfig.foo")
    }

    val fooFlagName = "databricks.dicer.assigner.targetConfig.foo"
    SafeConfigUtil.getTargetNameFromConfigFlagName(fooFlagName) == "foo"

    // A target contains a "." in its name.
    val dotFlagName = "databricks.dicer.assigner.targetConfig.foo.flag"
    SafeConfigUtil.getTargetNameFromConfigFlagName(dotFlagName) == "foo.flag"
  }
}
