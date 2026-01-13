package com.databricks.dicer.assigner

import java.net.{URI, URISyntaxException}
import java.util.UUID

import com.databricks.api.proto.dicer.assigner.AssignerInfoP

/**
 * Identifying information for an assigner instance.
 *
 * @param uuid A unique identifier for the instance.
 * @param uri The URI for issuing requests against the instance.
 */
case class AssignerInfo(
    uuid: UUID,
    uri: URI
) {
  def toProto: AssignerInfoP = {
    AssignerInfoP(
      Some(uuid.getMostSignificantBits),
      Some(uuid.getLeastSignificantBits),
      Some(uri.toString)
    )
  }

  override def toString: String = s"(uuid: $uuid, uri: $uri)"
}
object AssignerInfo {

  /**
   * REQUIRES: REQUIRES: proto must be a valid AssignerInfoP (see spec in proto file).
   *
   * Returns a new AssignerInfo instance from the given protocol buffer.
   */
  def fromProto(proto: AssignerInfoP): AssignerInfo = {
    // An all-zero UUID is invalid.
    require(proto.getUuidHigh != 0 || proto.getUuidLow != 0, "Invalid UUID")
    require(proto.getUri.nonEmpty, "URI must be non-empty")
    try {
      val uuid = new UUID(proto.getUuidHigh, proto.getUuidLow)
      val uri = new URI(proto.uri.get)
      AssignerInfo(uuid, uri)
    } catch {
      case _: URISyntaxException =>
        throw new IllegalArgumentException(s"Invalid URI: $proto")
    }
  }
}
