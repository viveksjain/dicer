package com.databricks.caching.util

import java.util.concurrent.locks.{Condition, Lock => JLock, ReentrantLock}
import java.util.concurrent.TimeUnit

import javax.annotation.concurrent.ThreadSafe

/**
 * A lock that cannot be acquired reentrantly. Behaves the same as [[ReentrantLock]], except that if
 * a thread attempts to acquire the lock when it already holds it, an
 * [[IllegalMonitorStateException]] is thrown. This is useful if we want to define clear ownership
 * of the lock acquisition, and calling [[unlock]] in the middle of a method (e.g. to make an
 * upcall) will guarantee to release the lock.
 */
@ThreadSafe
class NonReentrantLock(useFairUnderlyingLock: Boolean = false) extends JLock {

  /** Underlying lock. We just add checks at relevant places that it is not acquired reentrantly. */
  private val underlyingLock = new ReentrantLock(useFairUnderlyingLock)

  /** Acquires the lock, blocking if it is not available. */
  @throws[IllegalMonitorStateException]("if the current thread already holds this lock")
  override def lock(): Unit = {
    checkNotHeldByCurrentThread()
    underlyingLock.lock()
  }

  /** Releases the lock. */
  @throws[IllegalMonitorStateException]("if the current thread does not hold this lock")
  override def unlock(): Unit = {
    underlyingLock.unlock()
  }

  /**
   * Acquires the lock only if it is free at the time of invocation. Returns whether the lock was
   * acquired.
   */
  @throws[IllegalMonitorStateException]("if the current thread already holds this lock")
  override def tryLock(): Boolean = {
    checkNotHeldByCurrentThread()
    underlyingLock.tryLock()
  }

  /**
   * Acquires the lock if it is free within the given waiting time and the current thread has not
   * been interrupted. See [[ReentrantLock#tryLock(long, java.util.concurrent.TimeUnit)]] for more
   * details.
   */
  @throws[IllegalMonitorStateException]("if the current thread already holds this lock")
  @throws[InterruptedException]("if the current thread is interrupted")
  override def tryLock(timeout: Long, unit: TimeUnit): Boolean = {
    checkNotHeldByCurrentThread()
    underlyingLock.tryLock(timeout, unit)
  }

  /** Acquires the lock unless the current thread is interrupted. */
  @throws[IllegalMonitorStateException]("if the current thread already holds this lock")
  @throws[InterruptedException]("if the current thread is interrupted")
  override def lockInterruptibly(): Unit = {
    checkNotHeldByCurrentThread()
    underlyingLock.lockInterruptibly()
  }

  /**
   * Returns a new Condition instance that is bound to this Lock instance. See
   * [[ReentrantLock#newCondition]] for more details.
   */
  override def newCondition(): Condition = {
    underlyingLock.newCondition()
  }

  /** Returns whether this lock is held by the current thread. */
  def isHeldByCurrentThread: Boolean = {
    underlyingLock.isHeldByCurrentThread
  }

  /**
   * Checks whether the lock is already held by the current thread, in which case it throws
   * [[IllegalMonitorStateException]].
   */
  private def checkNotHeldByCurrentThread(): Unit = {
    if (isHeldByCurrentThread) {
      throw new IllegalMonitorStateException("Thread attempting to reacquire lock it already holds")
    }
  }
}
