package com.databricks.rpc

/**
 * Minimal representation of SSL/TLS arguments.
 *
 * This represents the common subset needed by tests: PEM paths for key/cert and trust.
 */
case class SslArguments(
    keyManagerCertChainPathOpt: Option[String] = None,
    keyManagerPrivateKeyPathOpt: Option[String] = None,
    trustManagerCertsPathOpt: Option[String] = None) {

  def isDisabled: Boolean = this == SslArguments.DISABLED_SSL_ARGUMENTS
}

object SslArguments {

  private val DISABLED_SSL_ARGUMENTS: SslArguments = SslArguments()

  /** An SslArguments that disables TLS. Can be used for either client or server side. */
  def disabled: SslArguments = DISABLED_SSL_ARGUMENTS
}
