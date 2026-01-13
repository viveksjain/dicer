package com.databricks.caching.util

import io.grpc.Status

/**
 * StatusOr represents a [[Status]] which either represents failure, or is OK and contains a
 * successfully computed value of type T.
 *
 * {{{
 * val userOr: StatusOr[User] = getUser()
 * if (!userOr.ok()) {
 *   logger.warning(s"Failed to get user: $userOr")
 * } else {
 *   val numFriends = userOr.get().getFriends().getSize()
 * }
 * }}}
 *
 * It is constructed using the factory [[StatusOr.success]] method for successful values or
 * [[StatusOr.error]].
 *
 * {{{
 * Try(StdIn.readLine("Enter an Int:\n").toInt) match {
 *   case Success(v) => StatusOr.success(v)
 *   case Failure(e) => StatusOr.error(Status.INVALID_ARGUMENT)
 * }
 * }}}
 */
sealed trait StatusOr[+T] {

  /** Returns true if the [[status]] is OK, false otherwise. */
  def isOk: Boolean

  /** Returns the status in this StatusOr. */
  def status: Status

  /**
   * REQUIRES: [[isOk]].
   *
   * Returns the value in this StatusOr.
   */
  def get: T

  /** Returns the value in this StatusOr, otherwise the result of evaluating `default`. */
  def getOrElse[U >: T](default: => U): U
}
object StatusOr {

  /** Construct a successful StatusOr with a value. */
  def success[T](value: T): StatusOr[T] = Success(value)

  /**
   * REQUIRES: `status` is not OK.
   *
   * Construct a failed StatusOr which does not hold a value.
   */
  def error[T](status: Status): StatusOr[T] = Failure(status)

  /** The success case. */
  case class Success[+T](value: T) extends StatusOr[T] {
    override def isOk: Boolean = true
    override def status: Status = Status.OK
    override def get: T = value
    override def getOrElse[U >: T](default: => U): U = value
  }

  /**
   * REQUIRES: `error` is not ok.
   *
   * The failure case.
   */
  case class Failure[+T](error: Status) extends StatusOr[T] {
    require(!error.isOk, "Cannot create a StatusOr.Failure with an OK Status")

    override def isOk: Boolean = false
    override def status: Status = error
    override def get: T = {
      throw new NoSuchElementException(s"get() called on non-OK StatusOr: $error")
    }
    override def getOrElse[U >: T](default: => U): U = default
  }
}
