package com.databricks.dicer.client

import java.net.URI

/**
 * Helper object for constructing watch addresses (URIs) for Dicer clients to connect to the
 * Assigner watch server.
 */
object WatchAddressHelper {

  /**
   * Returns the Assigner URI that a client needs to connect to by the given assigner hostname
   * and port.
   */
  def getAssignerURI(assignerHostName: String, assignerPort: Int): URI = {
    URI.create(s"https://$assignerHostName:$assignerPort")
  }
}
