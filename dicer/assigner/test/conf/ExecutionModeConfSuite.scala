package com.databricks.dicer.assigner.conf

import com.databricks.caching.util.TestUtils.assertThrow
import com.databricks.conf.ConfigParseException
import com.databricks.conf.Configs
import com.databricks.testing.DatabricksTest
import com.databricks.dicer.assigner.conf.DicerAssignerConf.ExecutionMode

class ExecutionModeConfSuite extends DatabricksTest {
  test("ExecutionMode is instantiated correctly from config") {
    // Test plan: create `DicerAssignerConf` with different configured store parameters and verify
    // that [[DicerAssignerConf.executionMode]] returns correct [[ExecutionMode]] object.
    val defaultConfig = new DicerAssignerConf(Configs.empty)
    assert(defaultConfig.executionMode == ExecutionMode.ASSIGNER_SERVICE)

    val assignerServiceConfig = new DicerAssignerConf(
      Configs.parseMap(
        "databricks.dicer.assigner.executionMode" -> "assigner_service"
      )
    )
    assert(assignerServiceConfig.executionMode == ExecutionMode.ASSIGNER_SERVICE)

    val etcdBootstrapperConfig = new DicerAssignerConf(
      Configs.parseMap(
        "databricks.dicer.assigner.executionMode" -> "etcd_bootstrapper"
      )
    )
    assert(etcdBootstrapperConfig.executionMode == ExecutionMode.ETCD_BOOTSTRAPPER)

    assertThrow[ConfigParseException]("invalid_execution_mode") {
      new DicerAssignerConf(
        Configs.parseMap(
          "databricks.dicer.assigner.executionMode" -> "invalid_execution_mode"
        )
      )
    }
  }
}
