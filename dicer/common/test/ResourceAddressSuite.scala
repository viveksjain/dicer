package com.databricks.dicer.common

import com.databricks.dicer.external.ResourceAddress
import com.databricks.caching.util.TestUtils.{
  assertThrow,
  checkComparisons,
  checkEquality,
  loadTestData
}
import com.databricks.dicer.common.test.ResourceAddressTestDataP
import com.databricks.dicer.common.test.ResourceAddressTestDataP.{
  UnsupportedResourceAddressUriP,
  EqualityGroupP
}
import com.databricks.testing.DatabricksTest

import java.net.URI

class ResourceAddressSuite extends DatabricksTest {

  /** Test data loaded from textproto. */
  private val testData: ResourceAddressTestDataP =
    loadTestData[ResourceAddressTestDataP](
      "dicer/common/test/data/resource_address_test_data.textproto"
    )

  /** Helper creating a resource from the string representation of its URI. */
  private def resource(uri: String): ResourceAddress = ResourceAddress(new URI(uri))

  test("ResourceAddress compare") {
    // Test plan: verify that comparison operations work as expected for ResourceAddress instances.
    val orderedResources: IndexedSeq[ResourceAddress] =
      testData.orderedUris.map { uri: String =>
        resource(uri)
      }.toIndexedSeq
    checkComparisons(orderedResources)
  }

  test("ResourceAddress equality") {
    // Test plan: define groups of equivalent resources and verify that equals and hashCode work as
    // expected using test data.
    val equalityGroups: Seq[Seq[ResourceAddress]] =
      testData.equalityGroups.map((_: EqualityGroupP).uris.map { uri: String =>
        resource(uri)
      })
    checkEquality(equalityGroups)
  }

  test("ResourceAddress unsupported URI") {
    // Test plan: verify that an IllegalArgumentException is thrown when constructing a
    // ResourceAddress from unsupported URIs.
    for (unsupportedCase: UnsupportedResourceAddressUriP <- testData.unsupportedUriCases) {
      val uri = URI.create(unsupportedCase.getUri) // URI itself is valid, but is not supported.
      assertThrow[IllegalArgumentException](unsupportedCase.getExpectedErrorMessage) {
        ResourceAddress(uri)
      }
    }
  }
}
