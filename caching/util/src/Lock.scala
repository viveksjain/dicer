package com.databricks.caching.util

import java.util.concurrent.locks.{Lock => JLock}

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
