package com.databricks.common.util

import java.util.concurrent.locks.{Lock => JLock}

/**
 * Helper object for locking. Duplicated from the Caching util library. We don't use the caching
 * util library directly because we would like to keep the same dependency structure as internal
 * code, where wrappers don't depend on the caching folder.
 */
object Lock {

  /** Executes `thunk` while holding `lock`. */
  @inline def withLock[T](lock: JLock)(thunk: => T): T = {
    // Attempt to acquire the lock outside of the try block so that the lock is only released when
    // it is successfully acquired.
    lock.lock()
    try {
      thunk
    } finally lock.unlock()
  }
}
