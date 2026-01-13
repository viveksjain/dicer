package com.databricks

sealed trait ErrorCode {
  def name: String = toString
}

/** Provides a minimal set of error codes used in Dicer for the open source version. */
object ErrorCode {
  case object UNKNOWN extends ErrorCode
  case object FAILED_PRECONDITION extends ErrorCode
  case object NOT_FOUND extends ErrorCode
  case object INTERNAL_ERROR extends ErrorCode
  case object REQUEST_LIMIT_EXCEEDED extends ErrorCode
}
