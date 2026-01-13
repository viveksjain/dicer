package com.databricks.caching.util

import io.grpc.Status
import io.grpc.StatusException

/** Test-only helpers useful for etcd-related tests. */
object EtcdTestUtils {

  /**
   * A wrapper around `thunk` which will immediately retry *once* if it fails due to an internal
   * version high watermark error from EtcdClient.
   *
   * This is useful in tests because EtcdClient expects that the caller manually retries operations
   * which fail this way.
   */
  def retryOnceOnWatermarkError[T](thunk: => T): T = {
    try {
      thunk
    } catch {
      case e: StatusException
          if e.getStatus.getCode == Status.INTERNAL.getCode && e.getMessage.contains("watermark") =>
        // Allow a single retry on the internal watermark error. The watermark should have been
        // updated synchronously, and so a retry should succeed.
        thunk
    }
  }
}
