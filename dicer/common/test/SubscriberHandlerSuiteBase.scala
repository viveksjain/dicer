package com.databricks.dicer.common

import scala.concurrent.Future
import scala.concurrent.duration._
import com.databricks.api.proto.dicer.common.{ClientRequestP, ClientResponseP}
import com.databricks.caching.util.TestUtils
import com.databricks.caching.util.TestUtils.TestName
import com.databricks.dicer.common.SubscriberHandler.Location
import com.databricks.dicer.common.TestSliceUtils._
import com.databricks.dicer.external.Target
import com.databricks.testing.DatabricksTest
import java.net.URI

/**
 * Abstract base class for SubscriberHandler tests that contains test logic that should work
 * across implementations.
 *
 * Concrete implementations (ScalaSubscriberHandlerSuite, RustSubscriberHandlerSuite) must
 * implement the abstract methods to provide language-specific behavior.
 */
abstract class SubscriberHandlerSuiteBase extends DatabricksTest with TestName {

  /** Timeout to request for watch requests. */
  protected val TIMEOUT: FiniteDuration = 1.minute

  /** Target to use for the current test. */
  protected def target: Target =
    Target.createKubernetesTarget(
      new URI("kubernetes-cluster:test-env/cloud1/public/region1/clustertype3/01"),
      getSafeName
    )

  /**
   * Creates a SubscriberHandlerDriver for testing. The driver manages the handler lifecycle
   * and provides methods to interact with it.
   *
   * @param handlerLocation The location of the handler.
   * @param handlerTarget The target for which the hanlder is serving.
   * @return A driver that can be used to interact with the handler.
   */
  protected def createDriver(
      handlerLocation: Location,
      handlerTarget: Target): SubscriberHandlerDriver

  /** Creates [[SliceletData]] for a Slicelet with the given address. */
  protected final def createSliceletData(uri: String): SliceletData = {
    SliceletData(
      createTestSquid(uri),
      ClientRequestP.SliceletDataP.State.RUNNING,
      "localhostNamespace",
      attributedLoads = Vector.empty,
      unattributedLoadOpt = None
    )
  }

  /** Creates a simulated watch request from a subscriber with the given data. */
  protected final def createClientRequest(
      knownGeneration: Generation,
      data: SubscriberData,
      debugName: String,
      requestTarget: Target = target): ClientRequest = {
    ClientRequest(
      requestTarget,
      SyncAssignmentState.KnownGeneration(knownGeneration),
      debugName,
      TIMEOUT,
      data,
      supportsSerializedAssignment = false
    )
  }

  /**
   * Creates a random assignment to `uris` with `generation`. URIs map to Squids according to
   * [[createTestSquid()]].
   */
  protected final def createRandomAssignment(
      generation: Generation,
      uris: Seq[String]): Assignment = {
    ProposedAssignment(
      predecessorOpt = None,
      createRandomProposal(
        10,
        uris.map(uri => createTestSquid(uri)).toVector,
        numMaxReplicas = 1,
        new scala.util.Random
      )
    ).commit(
      isFrozen = false,
      AssignmentConsistencyMode.Affinity,
      generation
    )
  }

  test("Empty Redirect") {
    // Test plan: Verify that the server responds to watch requests with empty redirect if the
    // `handleWatch` is called with an empty Redirect, regardless of what request was sent.
    val driver = createDriver(Location.Assigner, target)

    // Set an assignment so there is something to watch.
    val assignment1: Assignment = createRandomAssignment(
      generation = Generation(Incarnation(1), 42),
      Vector("pod0", "pod1")
    )
    driver.setAssignment(assignment1)

    val request = createClientRequest(Generation.EMPTY, ClerkData, "subscriber1")
    val fut: Future[ClientResponseP] = driver.handleWatch(request, redirectOpt = None)

    val response = ClientResponse.fromProto(TestUtils.awaitResult(fut, Duration.Inf))
    assert(response.redirect.addressOpt.isEmpty)
  }
}
