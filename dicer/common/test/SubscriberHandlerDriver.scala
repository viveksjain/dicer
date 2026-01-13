package com.databricks.dicer.common

import scala.concurrent.Future
import com.databricks.api.proto.dicer.common.ClientResponseP

/**
 * A wrapper around a [[SubscriberHandler]] to provide a common interface to different
 * implementations (e.g., running in the main test process or in a subprocess).
 */
trait SubscriberHandlerDriver {

  /** Sets the assignment for the SubscriberHandler. */
  def setAssignment(assignment: Assignment): Unit

  /**
   * Handles a watch request.
   *
   * @param request The client request to handle.
   * @param redirectOpt Optional redirect to include in the response.
   */
  def handleWatch(request: ClientRequest, redirectOpt: Option[Redirect]): Future[ClientResponseP]
}
