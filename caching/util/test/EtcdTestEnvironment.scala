package com.databricks.caching.util

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.Await
import scala.concurrent.duration._
import com.google.protobuf.ByteString
import io.etcd.jetcd
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.kv.GetResponse
import io.etcd.jetcd.options.{DeleteOption, GetOption, OptionsUtil}

import scala.collection.SortedMap

/**
 * A wrapper around an etcd instance running in Docker that can be used for testing. The etcd
 * instance is automatically started when this class is instantiated and automatically stopped when
 * it is closed.
 *
 * This class offers several blocking, [[String]]-based convenience methods for interacting with
 * this etcd instance. The input [[String]]s are encoded using UTF-8, and the read bytes are
 * decoded using UTF-8 as well, where non-UTF-8 bytes are replaced with a debug string. If the
 * UTF-8 assumption is too restrictive, callers should use a [[jetcd.Client]] instance directly.
 */
class EtcdTestEnvironment private (etcdWrapper: EtcdWrapper) extends AutoCloseable {

  /** Etcd endpoint for test environment. */
  def endpoint: String = etcdWrapper.endpoint

  /** A [[jetcd.Client]] pointed at this etcd instance. */
  val jetcdClient: jetcd.Client = EtcdClient.createJetcdClient(Seq(endpoint), None)

  /**
   * Deletes all data from within this etcd instance. Useful for resetting state between tests
   * without having to tear down the entire process, which is expensive.
   */
  def deleteAll(): Unit = {
    val allKeysBegin = ByteSequence.from("*", UTF_8)
    val allKeysEnd = OptionsUtil.prefixEndOf(ByteSequence.from(Array.empty[Byte]))
    jetcdClient.getKVClient
      .delete(
        allKeysBegin,
        DeleteOption.newBuilder().isPrefix(true).withRange(allKeysEnd).build()
      )
      .get()
  }

  /** Deletes all the keys starting with `prefix`. */
  def deleteAllWithPrefix(prefix: Array[Byte]): Unit = {
    jetcdClient.getKVClient
      .delete(
        ByteSequence.from(prefix),
        DeleteOption.newBuilder().isPrefix(true).build()
      )
      .get()
  }

  /**
   * Returns the value stored for the given `key` encoded as UTF-8, or [[None]] if no value exists.
   */
  def getKey(key: String): Option[String] = {
    get(key, GetOption.newBuilder().isPrefix(false).build()).get(key)
  }

  /** Reads all stored key-value pairs with the given key prefix encoded as UTF-8. */
  def getPrefix(prefix: String): SortedMap[String, String] = {
    get(prefix, GetOption.newBuilder().isPrefix(true).build())
  }

  /**
   * Reads all stored key-value pairs with keys in the range `[lowInclusive, highExclusive)`,
   * when each endpoint is encoded as UTF-8.
   */
  def getRange(lowInclusive: String, highExclusive: String): SortedMap[String, String] = {
    get(
      lowInclusive,
      GetOption.newBuilder().withRange(ByteSequence.from(highExclusive, UTF_8)).build()
    )
  }

  /** Reads all key-value pairs stored in this etcd instance. */
  def getAll(): SortedMap[String, String] = {
    // A zero byte start and end key tells etcd to read everything. The null terminator '\0' is
    // encoded as a zero byte in UTF-8.
    get(
      '\u0000'.toString,
      GetOption.newBuilder().withRange(ByteSequence.from('\u0000'.toString, UTF_8)).build()
    )
  }

  /** Puts `value` encoded as UTF-8 for `key` encoded as UTF-8. */
  def put(key: String, value: String): Unit = {
    jetcdClient.getKVClient
      .put(ByteSequence.from(key, UTF_8), ByteSequence.from(value, UTF_8))
      .get()
  }

  /**
   * Returns a client configured the same as in [[createEtcdClient]],
   * and additionally initializes the generation high watermark to the minimum version value.
   *
   * Initialization for any given [[EtcdClient.Config.keyNamespace]] should only be performed once;
   * if multiple clients are needed, callers should use [[createEtcdClient]] instead.
   */
  def createEtcdClientAndInitializeStore(config: EtcdClient.Config): EtcdClient = {
    val client: EtcdClient = createEtcdClient(config)
    Await.result(
      client.initializeVersionHighWatermarkUnsafe(EtcdClient.Version.MIN),
      Duration.Inf
    )
    client
  }

  /** Initializes the generation high watermark to the minimum version value in the etcd. */
  def initializeStore(namespace: EtcdClient.KeyNamespace): Unit = {
    createEtcdClientAndInitializeStore(EtcdClient.Config(namespace))
  }

  /** Creates an [[EtcdClient]] connecting to this instance. */
  def createEtcdClient(config: EtcdClient.Config): EtcdClient = {
    EtcdClient.create(
      Seq(endpoint),
      tlsOptionsOpt = None,
      config
    )
  }

  /**
   * Creates an [[EtcdClient]] connecting to this instance and an interposing jetcd wrapper for
   * injecting errors and spying on calls. When no store incarnation is given, chooses a unique
   * store incarnation for the client to avoid interference with other tests.
   */
  def createEtcdClientWithInterposingJetcdWrapper(
      clock: TypedClock,
      config: EtcdClient.Config
  ): (EtcdClient, InterposingJetcdWrapper) = {
    val jetcdClient = EtcdClient.createJetcdClient(Seq(endpoint), None)
    val interposingJetcdWrapper = InterposingJetcdWrapper.create(jetcdClient, clock)
    val client = EtcdClient.forTest(interposingJetcdWrapper, config)
    (client, interposingJetcdWrapper)
  }

  /** Teardown etcd. */
  override def close(): Unit = {
    etcdWrapper.close()
  }

  /**
   * Reads key-value pair starting from `lowInclusive` (limit is established by `option`) via
   * [[jetcdClient]].
   */
  private def get(lowInclusive: String, option: GetOption): SortedMap[String, String] = {
    val key: ByteSequence = ByteSequence.from(lowInclusive, UTF_8)
    val getResponse: GetResponse = jetcdClient.getKVClient
      .get(key, option)
      .get()
    val builder = SortedMap.newBuilder[String, String]
    getResponse.getKvs.forEach { kv =>
      {
        val key: ByteSequence = kv.getKey
        val value: ByteSequence = kv.getValue
        builder += (stringFromUtf8(key) -> stringFromUtf8(value))
      }
    }
    builder.result()
  }

  /**
   * Performs conversion from UTF-8 byte sequence to string. If the bytes are not valid UTF-8,
   * returns a debug string including formatted bytes.
   */
  private def stringFromUtf8(bytes: ByteSequence): String = {
    // Use ByteString to validate that the bytes are valid UTF-8.
    val byteString = ByteString.copyFrom(bytes.getBytes)
    if (byteString.isValidUtf8) {
      byteString.toStringUtf8
    } else {
      s"NON UTF-8 (consider using jetcd.Client instead) ****: ${Bytes.toString(byteString)}"
    }
  }
}
object EtcdTestEnvironment {

  /** Starts an etcd instance in Docker. */
  def create(): EtcdTestEnvironment = {
    val etcdWrapper = EtcdWrapper.create()
    new EtcdTestEnvironment(etcdWrapper)
  }
}
