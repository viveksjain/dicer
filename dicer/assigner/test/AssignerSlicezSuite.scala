package com.databricks.dicer.assigner

import java.util.UUID
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.util.Random
import org.apache.commons.lang3.StringUtils
import org.apache.commons.text.StringEscapeUtils
import com.databricks.dicer.assigner.AssignerTargetSlicezData.TargetConfigMethod
import com.databricks.dicer.assigner.AssignerTargetSlicezData.TargetConfigMethod.TargetConfigMethod
import com.databricks.dicer.assigner.AssignmentGenerator.GeneratorTargetSlicezData
import com.databricks.dicer.assigner.AssignmentStats.AssignmentChangeStats

import com.databricks.dicer.assigner.config.{ChurnConfig, InternalTargetConfig}
import com.databricks.dicer.assigner.config.InternalTargetConfig.{
  HealthWatcherTargetConfig,
  KeyReplicationConfig,
  LoadBalancingConfig,
  LoadBalancingMetricConfig,
  LoadWatcherTargetConfig
}
import com.databricks.dicer.assigner.algorithm.{LoadMap, Resources}
import com.databricks.dicer.common.TestSliceUtils._
import com.databricks.dicer.common.{
  Assignment,
  AssignmentConsistencyMode,
  ClerkSubscriberSlicezData,
  Generation,
  ProposedAssignment,
  SliceletSubscriberSlicezData,
  TestSliceUtils
}
import com.databricks.dicer.external.Target
import com.databricks.testing.DatabricksTest

class AssignerSlicezSuite extends DatabricksTest {

  /** Configuration used in `LoadBalancingMetricConfig`. */
  private val MAX_LOAD_HINT: Double = 1610.0

  /** The fake assigner information for the suite. */
  private val FAKE_ASSIGNER_INFO =
    AssignerInfo(UUID.randomUUID(), new java.net.URI("http://localhost:12345"))

  /** An arbitrary generation. */
  private val GENERATION: Generation = TestSliceUtils.createLooseGeneration(42)

  /** Create random assignments for tests. */
  private def createRandomAssignment: Assignment = {
    val resources: Resources = createResources("resource0", "resource1", "resource2")
    val proposedAsn1: ProposedAssignment = ProposedAssignment(
      predecessorOpt = None,
      createRandomProposal(
        numSlices = 10,
        resources = resources.availableResources.toIndexedSeq,
        numMaxReplicas = 3,
        rng = Random
      )
    )
    val generation: Generation = TestSliceUtils.createLooseGeneration(42)
    proposedAsn1.commit(
      isFrozen = false,
      AssignmentConsistencyMode.Affinity,
      generation
    )
  }

  test(
    "Check AssigerTargetSlicezData renders empty assignment and empty assignment change " +
    "stats data table HTML contents"
  ) {
    // Test plan: Create an AssignerTargetSlicezData with no assignment change stats and verify
    // that the HTML contents are as expected. We only verify assignment and assignment change-
    // related snippets, as load-related snippets are either checked by
    // [[AssignmentFormatterSuite]] or not rendered directly on the ZPage.
    val assignerTargetSlicezData = AssignerTargetSlicezData(
      target = Target("dummy-target"),
      sliceletsData = Seq(),
      clerksData = Seq(),
      assignmentOpt = None,
      generatorTargetSlicezData = GeneratorTargetSlicezData.EMPTY,
      targetConfig = AssignerTargetSlicezData.TargetConfigData(
        InternalTargetConfig.forTest.DEFAULT,
        AssignerTargetSlicezData.TargetConfigMethod.Dynamic
      )
    )

    // Verify assignment information.
    val assignmentDivRendered: String = assignerTargetSlicezData.createAssignmentDiv.render
    val expectedAssignmentFragment: String =
      "Target: <strong>dummy-target</strong></th></tr><div>" +
      "<tr style=\"background-color: PaleTurquoise;\"><th colspan=\"2\">Generation: None"
    assert(
      StringUtils.countMatches(assignmentDivRendered, expectedAssignmentFragment) == 1,
      s"Rendered:\n$assignmentDivRendered\n============\n" +
      s"Expected fragment:\n$expectedAssignmentFragment\n"
    )

    // Verify churn information.
    val churnDivRendered: String = assignerTargetSlicezData.createChurnDiv.render
    val expectedChurnFragment: String =
      "Target: <strong>dummy-target</strong></th></tr><div>" +
      "<tr style=\"background-color: PaleTurquoise;\"><th colspan=\"2\">Meaningful Assignment " +
      "Change: N/A</th></tr><tr style=\"background-color: PaleTurquoise;\">" +
      "<th colspan=\"2\">No Churn Ratio Information</th></tr></div>"
    assert(
      StringUtils.countMatches(churnDivRendered, expectedChurnFragment) == 1,
      s"Rendered:\n$churnDivRendered\n============\nExpected fragment:\n$expectedChurnFragment\n"
    )
  }

  test(
    "Check AssignerTargetSlicezData renders assignment change stats data in " +
    "GeneratorTargetSlicezData into table HTML contents"
  ) {
    // Test plan: Directly create AssignerTargetSlicezData using data from an assignment change and
    // verify golden snippets are rendered as expected. We only verify assignment change-related
    // snippets, as load-related snippets are either checked by [[AssignmentFormatterSuite]] or not
    // rendered directly on the ZPage.

    // Create random assignments and load map to calculate the assignment change stats from.
    val prevAsn: Assignment = createRandomAssignment
    val curAsn: Assignment = createRandomAssignment
    val loadMap: LoadMap = createRandomLoadMap(rng = Random, totalLoad = 1.0)

    // Calculate the assignment change stats from prevAsn to curAsn.
    val assignmentChangeStats: AssignmentChangeStats =
      AssignmentChangeStats.calculate(prevAsn, curAsn, loadMap)

    val assignerTargetSlicezData = AssignerTargetSlicezData(
      target = Target("dummy-target"),
      sliceletsData = Seq(),
      clerksData = Seq(),
      assignmentOpt = Some(curAsn),
      generatorTargetSlicezData = GeneratorTargetSlicezData(
        reportedLoadPerResourceOpt = None,
        reportedLoadPerSliceOpt = None,
        topKeysOpt = None,
        adjustedLoadPerResourceOpt = None,
        assignmentChangeStatsOpt = Some(assignmentChangeStats)
      ),
      targetConfig = AssignerTargetSlicezData.TargetConfigData(
        InternalTargetConfig.forTest.DEFAULT,
        AssignerTargetSlicezData.TargetConfigMethod.Dynamic
      )
    )

    // Verify assignment information. We only check the target name and generation, as assignment
    // load details are checked by [[AssignmentFormatterSuite]].
    val assignmentDivRendered: String = assignerTargetSlicezData.createAssignmentDiv.render
    val expectedAssignmentFragment: String =
      "Target: <strong>dummy-target</strong></th></tr><div>" +
      "<tr style=\"background-color: PaleTurquoise;\"><th colspan=\"2\">Generation: " +
      s"""${curAsn.generation}</th></tr>"""
    assert(
      StringUtils.countMatches(assignmentDivRendered, expectedAssignmentFragment) == 1,
      s"Rendered:\n$assignmentDivRendered\n============\n" +
      s"Expected fragment:\n$expectedAssignmentFragment\n"
    )

    // Verify churn information.
    val churnDivRendered: String = assignerTargetSlicezData.createChurnDiv.render
    val expectedChurnFragment: String =
      "Target: <strong>dummy-target</strong></th></tr><div><tr style=" +
      "\"background-color: PaleTurquoise;\"><th colspan=\"2\">Meaningful Assignment Change: " +
      s"""${assignmentChangeStats.isMeaningfulAssignmentChange.toString}</th></tr>""" +
      "<tr style=\"background-color: PaleTurquoise;\"><th>Churn Type</th><th>Churn Ratio</th>" +
      "</tr><tr style=\"background-color: PaleTurquoise;\"><td>Load Balancing Churn Ratio</td>" +
      s"""<td>${assignmentChangeStats.loadBalancingChurnRatio.toString}</td></tr>""" +
      "<tr style=\"background-color: PaleTurquoise;\"><td>Addition Churn Ratio</td>" +
      s"""<td>${assignmentChangeStats.additionChurnRatio.toString}</td></tr>""" +
      "<tr style=\"background-color: PaleTurquoise;\"><td>Removal Churn Ratio</td>" +
      s"""<td>${assignmentChangeStats.removalChurnRatio.toString}</td></tr>"""
    assert(
      StringUtils.countMatches(churnDivRendered, expectedChurnFragment) == 1,
      s"Rendered:\n$churnDivRendered\n============\nExpected fragment:\n$expectedChurnFragment\n"
    )
  }

  test("Check AssignerSlicezData data table HTML contents") {
    // Test plan: Directly create AssignerSlicezData with random data and verify snippets are
    // rendered as expected. We create targets across all combinations of having an assignment vs
    // not and having default vs static config.
    //
    // Note: This test only checks for the presence of all targets without doing detailed validation
    // for each rendered [[GeneratorTargetSlicezData]], which is checked either in the above
    // `AssignerTargetSlicezData` test or in [[AssignmentFormatterSuite]].

    val assignment: Assignment = createRandomAssignment

    var i = 0
    val allTargetsData = mutable.ArrayBuffer[AssignerTargetSlicezData]()
    // Track the expected HTML fragments in the zpage output.
    val expectedUniqueFragments = mutable.ArrayBuffer[String]()
    for (hasAssignment: Boolean <- Seq(true, false)) {
      for (targetConfigMethod: TargetConfigMethod <- Seq(
          TargetConfigMethod.Dynamic,
          TargetConfigMethod.Static
        )) {
        val target = Target(s"softstore-storelet-$i")
        val targetDescription: String = target.toParseableDescription
        val sliceletsData: immutable.Seq[SliceletSubscriberSlicezData] =
          (0 until Random.nextInt(3)).map { j =>
            SliceletSubscriberSlicezData(s"slicelet-$j", s"localhost:12345")
          }
        val clerksData: Seq[ClerkSubscriberSlicezData] = (0 until Random.nextInt(5)).map { j =>
          ClerkSubscriberSlicezData(s"clerk-$j")
        }
        // Set `minDuration` to `i + 1` seconds so that we can uniquely match against the detailed
        // configuration in `expectedUniqueFragments`.
        val internalTargetConfig = InternalTargetConfig.forTest.DEFAULT.copy(
          loadWatcherConfig = LoadWatcherTargetConfig.DEFAULT.copy(minDuration = (i + 1).seconds),
          loadBalancingConfig = LoadBalancingConfig(
            loadBalancingInterval = 1.minute,
            ChurnConfig.DEFAULT,
            LoadBalancingMetricConfig(MAX_LOAD_HINT)
          ),
          keyReplicationConfig = KeyReplicationConfig(minReplicas = 1, maxReplicas = 2),
          healthWatcherConfig = HealthWatcherTargetConfig(true)
        )
        val targetConfigData = AssignerTargetSlicezData.TargetConfigData(
          internalTargetConfig,
          targetConfigMethod
        )
        allTargetsData.append(
          AssignerTargetSlicezData(
            target,
            sliceletsData,
            clerksData,
            assignmentOpt = if (hasAssignment) Option(assignment) else None,
            generatorTargetSlicezData = GeneratorTargetSlicezData.EMPTY,
            targetConfigData
          )
        )

        // Verify: The assignment accessor on the TargetSlicezData returns the expected assignment.
        if (hasAssignment) {
          assert(allTargetsData.last.getAssignmentOpt == Some(assignment))
        } else {
          assert(allTargetsData.last.getAssignmentOpt == None)
        }

        // Overview information.
        expectedUniqueFragments.append(
          s"""<th colspan="2">Target: <strong>$targetDescription</strong> """ +
          s"Slicelets: ${sliceletsData.size} Clerks: ${clerksData.size}</th>"
        )

        // Configuration method and color.
        val configColor: String =
          AssignerTargetSlicezData.TargetConfigData.TARGET_CONFIG_BACKGROUND_COLOR
        val configMethodAndColorFragment =
          s"Target: <strong>$targetDescription</strong></th></tr><tr>" +
          s"""<td style="background-color: $configColor;">""" +
          s"""Configuration Method</td><td style="background-color: $configColor;">""" +
          s"""Configuration Detail</td></tr><tr><td style="background-color: $configColor;">""" +
          targetConfigMethod.toString
        expectedUniqueFragments.append(
          configMethodAndColorFragment
        )

        // Detailed configuration information.
        val primaryRateConfigDescription: String =
          s"""
           |  primary_rate_metric_config {
           |    max_load_hint: $MAX_LOAD_HINT
           |    imbalance_tolerance_hint: DEFAULT
           |    uniform_load_reservation_hint: NO_RESERVATION
           |  }""".stripMargin
        expectedUniqueFragments.append(
          s"""target_config {$primaryRateConfigDescription
          |  key_replication_config {
          |    min_replicas: 1
          |    max_replicas: 2
          |  }
          |}
          |advanced_config {
          |  load_watcher_config {
          |    min_duration_seconds: ${i + 1}
          |    max_age_seconds: 300
          |    use_top_keys: true
          |  }
          |  key_replication_config {
          |    min_replicas: 1
          |    max_replicas: 2
          |  }
          |  health_watcher_config {
          |    observe_slicelet_readiness: true
          |  }
          |}""".stripMargin
        )

        i += 1
      }
    }

    val assignerSlicezData =
      AssignerSlicezData(
        FAKE_ASSIGNER_INFO,
        PreferredAssignerValue.ModeDisabled(GENERATION),
        allTargetsData
      )
    // Get HTML rendered result in String.
    val renderedHtmlString = assignerSlicezData.getHtml.render

    for (fragment: String <- expectedUniqueFragments) {
      assert(
        StringUtils.countMatches(renderedHtmlString, fragment) == 1,
        s"Rendered:\n$renderedHtmlString\n============\nExpected fragment:\n$fragment\n"
      )
    }
  }

  test("Check PreferredAssignerSlicezData data table HTML contents for all roles") {
    val otherAssignerInfo: AssignerInfo =
      AssignerInfo(UUID.randomUUID(), new java.net.URI("http://localhost:12345"))

    for (mode <- Seq(
        PreferredAssignerValue.ModeDisabled(GENERATION),
        PreferredAssignerValue.SomeAssigner(FAKE_ASSIGNER_INFO, GENERATION),
        PreferredAssignerValue.SomeAssigner(otherAssignerInfo, GENERATION),
        PreferredAssignerValue.NoAssigner(GENERATION),
        PreferredAssignerValue.ModeDisabled(GENERATION)
      )) {

      val assignerSlicezData = AssignerSlicezData(FAKE_ASSIGNER_INFO, mode, Seq())
      val html = assignerSlicezData.getHtml.render

      def assertContains(html: String, fragment: String): Unit = {
        assert(
          html.contains(fragment),
          s"Rendered:\n$html\n============\nExpected fragment:\n$fragment\n"
        )
      }

      mode match {
        case PreferredAssignerValue.ModeDisabled(generation: Generation) =>
          assertContains(html, "<th>Action</th><td>Always generating assignments</td>")
          assertContains(html, s"<th>Generation</th><td>$generation</td>")

        case PreferredAssignerValue
              .SomeAssigner(preferredAssignerInfo: AssignerInfo, generation: Generation) =>
          if (preferredAssignerInfo == FAKE_ASSIGNER_INFO) {
            assertContains(html, "<th>Role</th><td>Preferred assigner</td>")
            assertContains(html, "<th>Action</th><td>Generating assignments while preferred</td>")
          } else {
            assertContains(html, "<th>Role</th><td>Standby</td>")
            assertContains(html, "<th>Action</th><td>Monitoring health of preferred assigner</td>")
            assertContains(html, s"<th>PA UUID</th><td>${otherAssignerInfo.uuid.toString}</td>")
          }
          assertContains(html, s"<th>Generation</th><td>$generation</td>")

        case PreferredAssignerValue.NoAssigner(generation: Generation) =>
          assertContains(html, "<th>Role</th><td>Standby without preferred</td>")
          assertContains(
            html,
            "<th>Action</th><td>Attempting to take over as preferred assigner</td>"
          )
          assertContains(html, "<th>PA UUID</th><td>None</td>")
          assertContains(html, s"<th>Generation</th><td>$generation</td>")
      }
    }
  }

  test("Check escaping contents for AssignerSlicez") {
    // Test plan: Create AssignerSlicezData with contents that needs escaping and test if they get
    // handled appropriately.

    val target = Target("softstore-storelet-4")
    val nonsenseName = "<button>&nonsense name</button>"
    val nonsenseAddress = "&& 1 === 1"

    // Calculate expected escaped contents using `StringEscapeUtils.escapeHtml4`.
    val targetDescriptionEscaped = StringEscapeUtils.escapeHtml4(target.toParseableDescription)
    val nonsenseNameEscaped = StringEscapeUtils.escapeHtml4(nonsenseName)
    val nonsenseAddressEscaped = StringEscapeUtils.escapeHtml4(nonsenseAddress)

    val targetSlicezData = AssignerTargetSlicezData(
      target = target,
      sliceletsData = Seq(SliceletSubscriberSlicezData(nonsenseName, nonsenseAddress)),
      clerksData = Seq(ClerkSubscriberSlicezData("clerk")),
      assignmentOpt = None,
      generatorTargetSlicezData = GeneratorTargetSlicezData.EMPTY,
      targetConfig = AssignerTargetSlicezData.TargetConfigData(
        InternalTargetConfig.forTest.DEFAULT,
        AssignerTargetSlicezData.TargetConfigMethod.Static
      )
    )

    val assignerSlicezData =
      AssignerSlicezData(
        FAKE_ASSIGNER_INFO,
        PreferredAssignerValue.ModeDisabled(GENERATION),
        Seq(targetSlicezData)
      )

    val renderedData = assignerSlicezData.getHtml.render

    // Verify the rendered HTML should contain snippets with escaped contents rather than raw ones.
    assert(renderedData.contains(FAKE_ASSIGNER_INFO.uri.toString))
    assert(renderedData.contains(FAKE_ASSIGNER_INFO.uuid.toString))
    assert(renderedData.contains(targetDescriptionEscaped))
    assert(!renderedData.contains(nonsenseName))
    assert(!renderedData.contains(nonsenseAddress))
    assert(renderedData.contains(nonsenseNameEscaped))
    assert(renderedData.contains(nonsenseAddressEscaped))
  }
}
