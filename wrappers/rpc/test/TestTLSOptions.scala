package com.databricks.rpc.testing

import com.databricks.rpc.tls.TLSOptions
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

/**
 * Test utilities for TLS configuration.
 *
 * Provides file paths suitable for configs that expect key/cert and trust PEM paths.
 *
 * Note: We intentionally do not use Netty's SelfSignedCertificate here. It is not supported on
 * some JDKs, which would abort tests during static initialization.
 *
 * We also avoid embedding any private keys in source to satisfy secret scanning. Instead, we
 * generate ephemeral test certs at runtime using `openssl`.
 */
object TestTLSOptions {

  private case class GeneratedFiles(
      combinedPem: File,
      trustPem: File
  )

  /**
   * PRECONDITION: `args` is non-empty and `args.head` resolves to an executable on PATH.

   * Runs an `openssl` command and fails fast if it exits with a non-zero status code.
   *
   * @throws RuntimeException if the command exits non-zero.
   */
  private def runOpenssl(args: Seq[String]): Unit = {
    val process = new ProcessBuilder(args: _*).redirectErrorStream(true).start()
    val src = scala.io.Source.fromInputStream(process.getInputStream, UTF_8.name())
    val output: String =
      try src.mkString
      finally src.close()
    val exitCode: Int = process.waitFor()
    if (exitCode != 0) {
      throw new RuntimeException(s"openssl failed (exitCode=$exitCode): $output")
    }
  }

  /**
   * Generates a short-lived self-signed certificate and writes it to temp files.
   * We write to temp files to avoid embedding private keys in source.
   *
   * This is `lazy` so test suites that never exercise TLS do not pay the cost of running openssl
   * or creating temp files. It also ensures the temp files are generated at most once per JVM.
   *
   * Specs:
   * - Requires `openssl` to be available on PATH at runtime.
   * - Creates a single combined PEM file containing cert + private key.
   * - Creates a trust PEM containing the certificate.
   * - Uses CN=localhost for compatibility with local URI tests.
   */
  private lazy val generated: GeneratedFiles = {
    val dir = Files.createTempDirectory("dicer-tls-test").toFile
    dir.deleteOnExit()

    val combinedPem = new File(dir, "combined.pem")
    val trustPem = new File(dir, "trust.pem")

    val keyPem = new File(dir, "key.pem")
    val certPem = new File(dir, "cert.pem")

    runOpenssl(
      Seq(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        keyPem.getAbsolutePath,
        "-out",
        certPem.getAbsolutePath,
        "-days",
        "3650",
        "-nodes",
        "-subj",
        "/CN=localhost"
      )
    )

    val combined: String = Files.readString(certPem.toPath, UTF_8) + "\n" + Files.readString(
        keyPem.toPath,
        UTF_8
      )
    Files.writeString(combinedPem.toPath, combined, UTF_8)

    val trust: String = Files.readString(certPem.toPath, UTF_8)
    Files.writeString(trustPem.toPath, trust, UTF_8)

    GeneratedFiles(combinedPem = combinedPem, trustPem = trustPem)
  }

  /**
   * Client-side TLS options for testing.
   *
   * This is `lazy` to ensure certificate generation happens only when TLS is actually used, and
   * to share the generated temp files across tests within the same JVM.
   */
  lazy val clientTlsOptions: TLSOptions =
    TLSOptions.builder
    // We allow a combined PEM file (cert chain + private key) for both paths in tests.
      .addKeyManager(generated.combinedPem, generated.combinedPem)
      .addTrustManager(generated.trustPem)
      .build()

  /**
   * Server-side TLS options for testing.
   *
   * This is `lazy` for the same reason as [[clientTlsOptions]]: defer work until needed and share
   * the generated temp files across tests within the same JVM.
   */
  lazy val serverTlsOptions: TLSOptions =
    TLSOptions.builder
    // We allow a combined PEM file (cert chain + private key) for both paths in tests.
      .addKeyManager(generated.combinedPem, generated.combinedPem)
      .addTrustManager(generated.trustPem)
      .build()

  /** Optional client-side TLS options for testing. */
  val clientTlsOptionsOpt: Option[TLSOptions] = Some(clientTlsOptions)

  /** Optional server-side TLS options for testing. */
  val serverTlsOptionsOpt: Option[TLSOptions] = Some(serverTlsOptions)

  /** Client-side TLS options with service identity for testing. */
  val clientWithServiceIdentityTlsOptions: TLSOptions = clientTlsOptions

  /** Server-side TLS options with service identity for testing. */
  val serverWithServiceIdentityTlsOptions: TLSOptions = serverTlsOptions

  /** Client-side key manager path for tests (combined PEM). */
  def clientKeystorePath: String = generated.combinedPem.getAbsolutePath

  /** Client-side trust path for tests (PEM). */
  def clientTruststorePath: String = generated.trustPem.getAbsolutePath

  /** Server-side key manager path for tests (combined PEM). */
  def serverKeystorePath: String = generated.combinedPem.getAbsolutePath

  /** Server-side trust path for tests (PEM). */
  def serverTruststorePath: String = generated.trustPem.getAbsolutePath
}
