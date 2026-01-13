package com.databricks.rpc.testing

import com.databricks.rpc.SslArguments

/**
 * [[SslArguments]] for testing.
 *
 * Tests use self-signed certs generated at runtime by [[TestTLSOptions]].
 */
object TestSslArguments {

  /** Server-side [[SslArguments]]. */
  val serverSslArgs: SslArguments =
    SslArguments(
      keyManagerCertChainPathOpt = Some(TestTLSOptions.serverKeystorePath),
      // We use a combined PEM (cert chain + private key) for both paths.
      keyManagerPrivateKeyPathOpt = Some(TestTLSOptions.serverKeystorePath),
      trustManagerCertsPathOpt = Some(TestTLSOptions.serverTruststorePath)
    )

  /** Client-side [[SslArguments]]. */
  val clientSslArgs: SslArguments =
    SslArguments(
      keyManagerCertChainPathOpt = Some(TestTLSOptions.clientKeystorePath),
      // We use a combined PEM (cert chain + private key) for both paths.
      keyManagerPrivateKeyPathOpt = Some(TestTLSOptions.clientKeystorePath),
      trustManagerCertsPathOpt = Some(TestTLSOptions.clientTruststorePath)
    )
}
