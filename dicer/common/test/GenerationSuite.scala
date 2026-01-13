package com.databricks.dicer.common

import com.databricks.api.proto.dicer.common.GenerationP
import com.databricks.caching.util.TestUtils.{assertThrow, checkComparisons, loadTestData}
import com.databricks.dicer.common.test.GenerationTestDataP
import com.databricks.dicer.common.test.GenerationTestDataP.{
  CanonicalGenerationTestCaseP,
  InvalidGenerationTestCaseP,
  ToStringTestCaseP
}
import com.databricks.testing.DatabricksTest

import java.time.Instant

class GenerationSuite extends DatabricksTest {

  private val TEST_DATA: GenerationTestDataP =
    loadTestData[GenerationTestDataP]("dicer/common/test/data/generation_test_data.textproto")
  private val ORDERED_GENERATIONS: Vector[Generation] = TEST_DATA.orderedGenerations.map {
    proto: GenerationP =>
      Generation.fromProto(proto)
  }.toVector

  test("Generation compare") {
    // Test plan: Select a few generations: min and some other generations and perform comparison
    // operations on all of them w.r.t. each other including themselves.

    checkComparisons(ORDERED_GENERATIONS)
  }

  gridTest("Generation roundtripping")(ORDERED_GENERATIONS) { generation: Generation =>
    // Test plan: Create a generation and check that it roundtrips with the corresponding proto,
    // i.e., if we convert it to a proto, the generation is the same.

    val proto: GenerationP = generation.toProto
    val roundtrip: Generation = Generation.fromProto(proto)
    assert(generation == roundtrip)
  }

  gridTest("Generation canonicalization")(TEST_DATA.canonicalGenerationTestCases) {
    testCase: CanonicalGenerationTestCaseP =>
      // Test plan: Verify that parsing and reserializing a generation proto results in the expected
      // canonical representation.

      val generation = Generation.fromProto(testCase.getGeneration)
      assert(generation.toProto == testCase.getExpectedCanonicalGeneration)
  }

  gridTest("Generation proto validity")(TEST_DATA.invalidGenerationTestCases) {
    testCase: InvalidGenerationTestCaseP =>
      // Test plan: Generate some invalid protos and ensure that the validation fails with the right
      // exception and the relevant error string (or at least a snippet of it).

      assertThrow[IllegalArgumentException](testCase.getExpectedError) {
        Generation.fromProto(testCase.getGeneration)
      }
  }

  test("isLoose and isNonLoose incarnations") {
    // Test plan: Create incarnation with some non-negative odd and even numbers and validate only
    // positive even numbers are non-loose.

    for (strictIncarnationNumber: Long <- TEST_DATA.strictIncarnationNumbers) {
      val incarnation = Incarnation(strictIncarnationNumber)
      assert(incarnation.isNonLoose && !incarnation.isLoose, s"$incarnation is non-loose")
    }

    for (looseIncarnationNumber: Long <- TEST_DATA.looseIncarnationNumbers) {
      val incarnation = Incarnation(looseIncarnationNumber)
      assert(incarnation.isLoose && !incarnation.isNonLoose, s"$incarnation is loose")
    }
  }

  test("Invalid incarnations") {
    // Test plan: Construct invalid incarnations and expect it throw IllegalArgumentException.

    // Store incarnation must be non-negative.
    Incarnation(0)
    assertThrow[IllegalArgumentException]("Incarnation number must be non-negative") {
      Incarnation(-1)
    }
  }

  test("incarnation yields expected values") {
    // Test plan: Verify that Incarnation.value return the same value that was used to construct the
    // Incarnation.
    for (value <- Seq[Long](0, 1, 42, 5L << 48 - 1)) {
      assert(Incarnation(value).value == value)
    }
  }

  test("Large Incarnation throws error") {
    // Test plan: verify that attempting to create an Incarnation which is too large results in an
    // error.
    assertThrow[IllegalArgumentException]("intentional?") {
      Incarnation(5L << 48)
    }
  }

  test(
    f"""createForCurrentTime returns successor of lower bound when the current time is
       |in the past""".stripMargin
  ) {
    // Test plan: Verify that createForCurrentTime() returns the successor of the lower
    // bound when using the current time would return a past generation.
    val storeIncarnation = Incarnation(42)
    val now = Instant.ofEpochMilli(10)
    val lowerBoundExclusive = Generation(Incarnation(storeIncarnation.value), 11)
    assert(
      Generation.createForCurrentTime(
        storeIncarnation,
        now,
        lowerBoundExclusive
      ) == Generation(Incarnation(42), 12)
    )
  }

  test(
    "createForCurrentTime returns current time when above lower bound"
  ) {
    // Test plan: Verify that createForCurrentTime() returns a Generation using
    // the current time when the current time in milliseconds is ahead of the specified lower bound
    // value.
    val storeIncarnation = Incarnation(42)
    val now = Instant.ofEpochMilli(12)
    val lowerBoundExclusive = Generation(Incarnation(storeIncarnation.value), 10)
    assert(
      Generation.createForCurrentTime(
        storeIncarnation,
        now,
        lowerBoundExclusive
      ) == Generation(Incarnation(42), 12)
    )
  }

  test(
    "createForCurrentTime uses current time in incarnation bump"
  ) {
    // Test plan: Verify that createForCurrentTime() returns a Generation using the
    // current time when its incarnation is ahead of the lower bound, even if the lowest order 64
    // bits are below the lower 64 bits of the lower bound. This is really just a specific case of
    // the above test.
    val now = Instant.ofEpochMilli(10)
    val lowerBoundExclusive = Generation(Incarnation(43), 120491241421L)
    assert(
      Generation.createForCurrentTime(
        Incarnation(44),
        now,
        lowerBoundExclusive
      ) == Generation(Incarnation(44), 10)
    )
  }

  test("createForCurrentTime requires lower bound to be less than store incarnation") {
    // Test plan: verify that createForCurrentTime() throws if the lower bound has a
    // greater incarnation than the desired incarnation.
    assertThrow[IllegalArgumentException]("Impossible due to store incarnation: 42 > 41") {
      Generation.createForCurrentTime(
        Incarnation(41),
        Instant.ofEpochMilli(10),
        Generation(Incarnation(42), 10)
      )
    }
  }

  test("getNextLooseIncarnation and getNextNonLooseIncarnation are correct.") {
    // Test plan: Verify that getNextLooseIncarnation and getNextNonLooseIncarnation return the
    // expected value.
    // Verify this by calling it for 0, 1, 2 (requesting loose and non-loose) to cover all cases of
    // loose (special case 0), loose (non-special case 1), and non-loose. Also verify higher
    // non-special case numbers as a sanity check.
    for (entry <- TEST_DATA.expectedNextLooseIncarnations) {
      val incarnation = Incarnation(entry._1)
      val expectedNextLooseIncarnation = Incarnation(entry._2)
      val nextLooseIncarnation = incarnation.getNextLooseIncarnation
      assert(nextLooseIncarnation == expectedNextLooseIncarnation)
      assert(nextLooseIncarnation.isLoose)
    }
    for (entry <- TEST_DATA.expectedNextStrictIncarnations) {
      val incarnation = Incarnation(entry._1)
      val expectedNextNonLooseIncarnation = Incarnation(entry._2)
      val nextNonLooseIncarnation = incarnation.getNextNonLooseIncarnation
      assert(nextNonLooseIncarnation == expectedNextNonLooseIncarnation)
      assert(nextNonLooseIncarnation.isNonLoose)
    }
  }

  gridTest("toString includes incarnation and number")(TEST_DATA.toStringTestCases) {
    testCase: ToStringTestCaseP =>
      // Test plan: verify that toString() joins the string representations of Incarnation and
      // UnixTimeVersion with a '#'.
      val generation = Generation.fromProto(testCase.getGeneration)
      assert(generation.toString == testCase.getExpectedString)
  }
}
