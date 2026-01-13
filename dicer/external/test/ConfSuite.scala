package com.databricks.dicer.external

import com.databricks.testing.DatabricksTest
import java.net.URI

import com.databricks.conf.Configs
import com.databricks.conf.trusted.ProjectConf
import com.databricks.conf.trusted.RPCPortConf
import com.databricks.backend.common.util.Project
import com.databricks.rpc.tls.TLSOptions

class ConfSuite extends DatabricksTest {
  private val FAKE_ASSIGNER_PORT = 80

  private val FAKE_SLICELET_PORT = 86

  /**
   * Creates a ClerkConf configuration where the slicelet watch RPC port is always set to the value
   * of FAKE_SLICELET_PORT.
   *
   * We create this config to test conf.getSliceletURI which only uses those config values, and so
   * we do not override any other config values when instantiating the conf.
   */
  private def createClerkConf: ClerkConf = {
    val clerkConfig = Configs.parseMap(
      "databricks.dicer.slicelet.rpc.port" -> FAKE_SLICELET_PORT,
      "databricks.dicer.assigner.rpc.port" -> FAKE_ASSIGNER_PORT
    )

    new ProjectConf(Project.TestProject, clerkConfig) with ClerkConf with RPCPortConf {
      override protected def dicerTlsOptions: Option[TLSOptions] = None
    }
  }

  test(
    "getSliceletURI returns a URI with both host and port defined when a DBNS host is not defined"
  ) {
    // Test plan: Verify that getSliceletURI returns a URI containing both a host and port when
    // the slicelet host is NOT a DBNS host.
    val sliceletHostName = "localhost"
    val conf = createClerkConf
    val expectedSliceletURI = new URI(s"$sliceletHostName:$FAKE_SLICELET_PORT")
    assertResult(expectedSliceletURI)(conf.getSliceletURI(sliceletHostName))
  }

}
