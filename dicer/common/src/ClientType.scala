package com.databricks.dicer.common

/** An identifier for the type of client. */
private[dicer] sealed trait ClientType {

  /** Returns the label used for metrics. */
  def getMetricLabel: String
}

private[dicer] object ClientType {

  /** A type for Clerk clients. */
  case object Clerk extends ClientType {
    override def toString: String = "clerk"
    override def getMetricLabel: String = toString
  }

  /** A type for Slicelet clients. */
  case object Slicelet extends ClientType {
    override def toString: String = "slicelet"
    override def getMetricLabel: String = toString
  }
}
