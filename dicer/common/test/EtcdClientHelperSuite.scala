package com.databricks.dicer.common

import com.databricks.caching.util.TestUtils.assertThrow
import com.databricks.testing.DatabricksTest
import com.databricks.caching.util.EtcdClient.{Version => EtcdClientVersion}

class EtcdClientHelperSuite extends DatabricksTest {

  test(
    "createGenerationFromVersion returns the expected values and roundtrips with " +
    "getVersionFromNonLooseGeneration"
  ) {
    // Test plan: Verify that createNonLooseGenerationFromVersion returns the expected Generation
    // for various versions and non-loose store incarnations, and roundtrips with
    // getVersionFromNonLooseGeneration.

    for (storeIncarnation <- Seq[Long](2, 42, 5L << 48 - 1)) {
      for (generationNumber <- Seq(0, 1, 42, Long.MaxValue)) {
        val version = EtcdClientVersion(
          storeIncarnation,
          generationNumber
        )
        val generation: Generation = EtcdClientHelper.createGenerationFromVersion(version)
        assert(generation.incarnation.isNonLoose)
        assert(generation.incarnation.value == version.highBits)
        assert(generation.number.value == version.lowBits.value)
        assert(EtcdClientHelper.getVersionFromNonLooseGeneration(generation) == version)
      }
    }
  }

  test(
    "getVersionFromNonLooseGeneration requires generation incarnation to be non-loose"
  ) {
    // Test plan: Verify that getVersionFromNonLooseGeneration requires generation incarnation to be
    // non-loose and have a non-zero store incarnation.

    // Generation incarnation has a > 0 store incarnation, but is loose.
    assertThrow[IllegalArgumentException]("Incarnation must be non-loose") {
      EtcdClientHelper.getVersionFromNonLooseGeneration(
        Generation(Incarnation(3), 42)
      )
    }
  }
}
