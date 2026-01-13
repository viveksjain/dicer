package com.databricks.caching.util

import io.grpc.Status

/* Contains methods related to transforming [[Status]]. */
object StatusUtils {

  /** Convert `ex` into an appropriate [[Status]]. */
  def convertExceptionToStatus(ex: Throwable): Status = ex match {
    case ex: io.grpc.StatusException =>
      ex.getStatus
    case ex: io.grpc.StatusRuntimeException =>
      ex.getStatus
    case ex: IllegalArgumentException =>
      var status = Status.INVALID_ARGUMENT
      if (ex.getMessage != null) {
        status = status.withDescription(ex.getMessage)
      }
      status.withCause(ex)
    case ex: Throwable =>
      // All other exceptions map to UNKNOWN.
      var status = Status.UNKNOWN
      if (ex.getMessage != null) {
        status = status.withDescription(ex.toString)
      }
      status.withCause(ex)
  }
}
