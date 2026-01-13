package com.databricks.dicer.common

import com.databricks.conf.DbConf
import com.databricks.rpc.tls.TLSOptions
import java.io.File

/**
 * NOTE to customers: There is nothing in this file that should be relevant for customers. Even
 * though this class is inherited by DicerClientConf, please do not use any extra information from
 * this file other than what is specified in the external directory.
 *
 * A trait that encapsulates the SSL arguments needed by the Clerk, Slicelet, and Assigner, both in
 * production code and the test environment. From the perspective of the internal Dicer code, the
 * two interesting functions are `dicerClientSslArgs` and `dicerServerSslArgs`. The code needs
 * to use the former for client channels (e.g., when connecting to the Assigner)" and the latter is
 * used for starting a gRPC server by the Slicelet and Assigner. We need to break out the SSL
 * arguments in this manner for test only. In production, SSL arguments are the same. However, in
 * test code, `TestSslArguments` are separated out. Hence, by structuring the production code to
 * deal with client and server SSL arguments separately, we can write tests that override
 * `dicerClientSslArgs` and `dicerServerSslArgs` in the appropriate manner.
 *
 * NOTE to Dicer engineers: Please make sure that there is no public variable/method in this file.
 */
trait CommonSslConf extends DbConf {

  /**
   * TLSOptions that are expected to be set by the customer (as specified in DicerClientConf).
   * Marked as protected so that Dicer code can use it only in `DicerClientConf` and not other
   * parts of the Dicer code.
   */
  protected def dicerTlsOptions: Option[TLSOptions]

  /**
   * TLSOptions for connecting to the Assigner.
   * If not overridden, it defaults to [[dicerTlsOptions]].
   */
  protected def dicerClientTlsOptions: Option[TLSOptions] = dicerTlsOptions

  /**
   *  TLSOptions for allowing the Slicelet to start a gRPC server.
   *  If not overridden, it defaults to [[dicerTlsOptions]].
   */
  protected def dicerServerTlsOptions: Option[TLSOptions] = dicerTlsOptions

  /**
   * TLSOptions for connecting to the Assigner. In test, these need to be overridden with
   * `TestTLSOptions.clientTlsOptionsOpt`.
   */
  private[dicer] final def getDicerClientTlsOptions: Option[TLSOptions] = {
    (clientKeystoreOpt, clientTruststoreOpt) match {
      case (Some(keystorePath), Some(truststorePath)) =>
        Some(
          TLSOptions.builder
            .addKeyManager(new File(keystorePath), new File(keystorePath))
            .addTrustManager(new File(truststorePath))
            .build()
        )
      case _ => dicerClientTlsOptions
    }
  }

  /**
   * TLSOptions to allow the Slicelet to start a gRPC server. In test, these need to be
   * overridden with `TestTLSOptions.serverTlsOptionsOpt`.
   */
  private[dicer] final def getDicerServerTlsOptions: Option[TLSOptions] = {
    (serverKeystoreOpt, serverTruststoreOpt) match {
      case (Some(keystorePath), Some(truststorePath)) =>
        Some(
          TLSOptions.builder
            .addKeyManager(new File(keystorePath), new File(keystorePath))
            .addTrustManager(new File(truststorePath))
            .build()
        )
      case _ => dicerServerTlsOptions
    }
  }

  /**
   * The following configurations are used to build and override TLSOptions for the client and
   * server.
   */
  private val clientKeystoreOpt: Option[String] =
    configure[Option[String]]("databricks.dicer.library.client.keystore", None)
  private val clientTruststoreOpt: Option[String] =
    configure[Option[String]]("databricks.dicer.library.client.truststore", None)

  /** See comments on [[clientKeystoreOpt]]. This state is for the server TLSOptions. */
  private val serverKeystoreOpt: Option[String] =
    configure[Option[String]]("databricks.dicer.library.server.keystore", None)
  private val serverTruststoreOpt: Option[String] =
    configure[Option[String]]("databricks.dicer.library.server.truststore", None)
}
