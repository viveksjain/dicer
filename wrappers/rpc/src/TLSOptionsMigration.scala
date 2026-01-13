package com.databricks.rpc.tls

import com.databricks.rpc.SslArguments
import java.io.File

/**
 * Converts legacy `SslArguments` to `TLSOptions`.
 *
 * The inputs are paths to PEM files:
 * - key manager: certificate chain PEM + private key PEM (or a single combined PEM for both)
 * - trust manager: CA/leaf certificates PEM
 *
 * If only a trust manager is configured, the client verifies the server, but does not present a
 * certificate (no mTLS). If a trust manager is configured on the server, the server requires a
 * client certificate (mTLS).
 */
object TLSOptionsMigration {

  /**
   * Configures the key manager on the builder if (and only if) both key manager paths are present.
   *
   * We enforce "both-or-neither": it is invalid to provide only one of cert chain or private key.
   */
  private def addKeyManagerIfConfigured(
      builder: TLSOptions.Builder,
      sslArgs: SslArguments): TLSOptions.Builder = {
    (sslArgs.keyManagerCertChainPathOpt, sslArgs.keyManagerPrivateKeyPathOpt) match {
      case (Some(certChainPath), Some(privateKeyPath)) =>
        builder.addKeyManager(new File(certChainPath), new File(privateKeyPath))
      case (None, None) =>
        builder
      case _ =>
        throw new IllegalArgumentException(
          "SslArguments requires both or neither of cert chain and private key paths " +
          "for key manager config."
        )
    }
  }

  /**
   * Configures the trust manager on the builder if a trust bundle path is present.
   *
   * Note: whether this results in one-way TLS vs mTLS depends on where the resulting TLSOptions is
   * used (client vs server); see [[TLSOptions]] for details.
   */
  private def addTrustManagerIfConfigured(
      builder: TLSOptions.Builder,
      sslArgs: SslArguments): TLSOptions.Builder = {
    sslArgs.trustManagerCertsPathOpt match {
      case Some(trustPath) =>
        builder.addTrustManager(new File(trustPath))
      case None =>
        builder
    }
  }

  /**
   * Converts an `SslArguments` to an `Option[TLSOptions]`.
   *
   * Returns None when TLS is disabled, otherwise returns a TLSOptions configured from the provided
   * PEM file paths.
   */
  def convert(sslArgs: SslArguments): Option[TLSOptions] = {
    if (sslArgs.isDisabled) {
      None
    } else {
      val withKeyManager: TLSOptions.Builder =
        addKeyManagerIfConfigured(builder = TLSOptions.builder, sslArgs = sslArgs)
      val withTrustManager: TLSOptions.Builder =
        addTrustManagerIfConfigured(builder = withKeyManager, sslArgs = sslArgs)

      Some(withTrustManager.build())
    }
  }
}
