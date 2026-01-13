package com.databricks.caching.util

import scala.concurrent.{Await, ExecutionContext, ExecutionException, Future}
import scala.concurrent.duration._

import org.scalactic.source.Position
import org.scalatest.Assertions
import org.scalatest.exceptions.TestFailedException

import com.databricks.macros.sourcecode.File
import com.databricks.macros.sourcecode.Line
import com.databricks.testing.AsyncTestHelpers

/**
 * An object that helps in waiting for assertions to be satisfied. Similar to
 * [[AsyncTestHelpers.eventually]], but also periodically logs assertion failures with the given
 * [[logPrefix]] tag to facilitate debugging when tests fail.
 *
 * Sample usage:
 *
 * {{{
 * AssertionWaiter("expected calls").await {
 *   assert(listener.calls == expectedCalls)
 * }
 * }}}
 *
 * @param logPrefix    prefix included when logging assertion errors observed before `timeout`.
 * @param timeout      after this timeout, any exception thrown by `fun` will propagate.
 * @param pollInterval interval at which `fun` is invoked.
 * @param logInterval  interval at which assertion errors are logged.
 * @param ecOpt        the execution context on which to run the function passed to [[await()]], or
 *                     None to indicate that the function should run on the calling thread.
 */
case class AssertionWaiter(
    logPrefix: String,
    // Change to 5.secs for quicker test failures when debugging
    timeout: FiniteDuration = 30.seconds,
    pollInterval: FiniteDuration = 5.milliseconds,
    logInterval: FiniteDuration = 3.seconds,
    ecOpt: Option[ExecutionContext] = None)
    extends AsyncTestHelpers {

  /**
   * Invokes the given function repeatedly until it succeeds or until the given timeout. Like
   * [[AsyncTestHelpers.eventually]], but this method also periodically logs assertion failures with
   * the given `logPrefix` tag to facilitate debugging when tests fail.
   *
   * @param fun the function to be invoked.
   * @param pos the position of the caller, used for `eventually` call (implicitly populated).
   * @param file the file of the caller, used for logging (implicitly populated).
   * @param line the line of the caller, used for logging (implicitly populated).
   */
  def await[T](fun: => T)(implicit pos: Position, file: File, line: Line): T = {
    // Use a dedicated logger to pretty-print "debugString" context in the logs and to get periodic
    // logging of assertion errors.
    val logger = PrefixLogger.create(getClass, logPrefix)
    logger.info("Waiting")(file = file, line = line)
    eventually(timeout = timeout, pollInterval = pollInterval) {
      try {
        ecOpt match {
          case Some(ec: ExecutionContext) =>
            // Run `fun` on `ec` and block until it completes.
            val fut: Future[T] = Future { fun }(ec)
            try {
              // We allow `fut` to take as much as `timeout` to complete, even when the remaining
              // timeout is shorter, since assertion errors are more actionable than timeouts, but
              // we don't want to wait forever on (say) a blocked executor.
              Await.result(fut, timeout)
            } catch {
              case e: ExecutionException if e.getCause != null =>
                // Unwrap exception causing the future to fail.
                throw e.getCause
            }
          case None =>
            // Run `fun` on the calling thread.
            fun
        }
      } catch {
        // Periodically log assertion errors so that we can see the progression in test logs.
        // Log both `AssertionError` (which is thrown by `Predef.assert`, implicitly imported in
        // every Scala source file), and `TestFailedException` (which is thrown by
        // `Assertions.assert` and is used by tests). We rely on `AsyncTestHelpers.eventually` to
        // surface the final error message in test results (so that there's no need to grep test
        // logs for the cause of the failure).
        case e @ (_: TestFailedException | _: AssertionError) =>
          // Periodically log so that we can see the progression of values in test logs.
          logger.info(s"Still waiting: ${e.getMessage}", every = logInterval)(
            file = file,
            line = line
          )
          throw e
      }
    }(pos = pos)
  }

  /**
   * Defensive overload intended to prevent callers from accidentally supplying a predicate to the
   * await method.
   */
  def await(predicate: => Boolean)(implicit pos: Position): Nothing = {
    Assertions.fail(
      "await { predicate } is not supported. Did you mean await { assert(predicate) }?"
    )
  }
}
