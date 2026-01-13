package com.databricks.api

import com.databricks.ErrorCode
import com.databricks.api.base.DatabricksServiceException

/** Provides utilities for extracting error codes from exceptions. */
object ErrorCodeUtils {

  /**
   * Gets the final error code from an exception, recursively traversing the exception chain
   * to find any nested DatabricksServiceException.
   */
  @scala.annotation.tailrec
  def getFinalErrorCode(ex: Throwable): ErrorCode = {
    ex match {
      case null => ErrorCode.UNKNOWN
      // If the exception is a DatabricksServiceException, return its error code immediately.
      case serviceEx: DatabricksServiceException => serviceEx.getErrorCode
      case _ => getFinalErrorCode(ex.getCause)
    }
  }
}
