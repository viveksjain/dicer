package com.databricks.caching.util

import scala.util.Try
import io.prometheus.client.CollectorRegistry
import com.databricks.caching.util.EtcdClient.KeyNamespace

/** The [[EtcdClient]] operations whose latencies are described in the histogram. */
object OperationType extends Enumeration {

  /** An operation that creates a new versioned key. */
  val CREATE: OperationType.Value = Value("create")

  /** An operation that updates an existing key. */
  val UPDATE: OperationType.Value = Value("update")
}

/** The possible results of an [[EtcdClient]] operation */
object OperationResult extends Enumeration {

  /** An operation completed successfully. */
  val SUCCESS: OperationResult.Value = Value("success")

  /**
   * A create or update operation completed successfully and failed to commit a new version because
   * a key was present with an unexpected version.
   */
  val WRITE_OCC_FAILURE_KEY_PRESENT: OperationResult.Value = Value("write_occ_failure_key_present")

  /**
   * A create or update operation completed successfully and failed to commit a new version because
   * an expected key was absent.
   */
  val WRITE_OCC_FAILURE_KEY_ABSENT: OperationResult.Value = Value("write_occ_failure_key_absent")

  /**
   * An operation did not complete successfully.
   */
  val FAILURE: OperationResult.Value = Value("failure")
}

object EtcdClientLatencyHistogram {

  private val METRIC_LABEL_NAMES = Vector("operationResult", "keyNamespace")

  /**
   * Factory method for creating a [[EtcdClientLatencyHistogram]].
   *
   * @param metric The name of the histogram metric, e.g. "dicer_etcd_client_op_latency".
   * */
  def apply(metric: String): EtcdClientLatencyHistogram = {
    val histogram: CachingLatencyHistogram = CachingLatencyHistogram(metric, METRIC_LABEL_NAMES)
    new EtcdClientLatencyHistogram(histogram)
  }

  object forTest {
    def apply(
        metric: String,
        clock: TypedClock,
        bucketsSecs: Vector[Double],
        registry: CollectorRegistry = CollectorRegistry.defaultRegistry)
        : EtcdClientLatencyHistogram = {
      val histogram: CachingLatencyHistogram = CachingLatencyHistogram.staticForTest.create(
        metric,
        clock,
        METRIC_LABEL_NAMES,
        bucketsSecs,
        registry
      )
      new EtcdClientLatencyHistogram(histogram)
    }
  }
}

/**
 * A thin wrapper around [[CachingLatencyHistogram]] for recording [[EtcdClient]] latency metrics.
 * The wrapper enforces a set of labels and buckets that are appropriate for [[EtcdClient]], and it
 * provides a convenience method for recording the latency of asynchronous thunks.
 *
 * The wrapper, like the underlying [[CachingLatencyHistogram]], is threadsafe.
 */
class EtcdClientLatencyHistogram private (histogram: CachingLatencyHistogram) {

  /**
   * Records a latency observation for an asynchronous thunk that performs the given `operation`.
   *
   * @param thunk Asynchronous thunk to instrument.
   */
  def recordLatencyAsync[T](
      operation: OperationType.Value,
      keyNamespace: KeyNamespace,
      computeOperationResult: Try[T] => OperationResult.Value)(
      thunk: => Pipeline[T]): Pipeline[T] = {
    def computeExtraLabels(triedResult: Try[T]): Seq[String] = {
      Seq(computeOperationResult(triedResult).toString, keyNamespace.value)
    }
    histogram.recordLatencyAsync(operation.toString, computeExtraLabels) { thunk }
  }
}
