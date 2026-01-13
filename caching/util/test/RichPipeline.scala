package com.databricks.caching.util

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Implements extensions to [[Pipeline]] allowing outcomes to be concisely observed in tests.
 *
 * The implicit class itself is nested in this object so that callers must explicitly import it
 * into scope.
 */
object RichPipeline {

  /** Implicit class containing the extension methods. */
  implicit class Implicits[T](pipeline: Pipeline[T]) {

    /**
     * Triggers execution of the pipeline by converting it to a future, asserts that the future is
     * completed, and returns its outcome.
     */
    def getNonBlocking: Try[T] = {
      val future: Future[T] = pipeline.toFuture
      assert(future.isCompleted)
      future.value.get
    }

    /** Triggers execution of the pipeline by converting it to a future and awaits its outcome. */
    def await(): Try[T] = {
      val future: Future[T] = pipeline.toFuture
      Await.ready(future, Duration.Inf)
      future.value.get
    }
  }

}
