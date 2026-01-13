package com.databricks.caching.util

import com.databricks.caching.util.TestUtils.assertThrow
import com.databricks.caching.util.InstantiationTracker.{
  PerProcessSingleton,
  PerProcessSingletonType
}
import com.databricks.testing.DatabricksTest

import java.util.concurrent.{ExecutorService, ForkJoinPool, Future}

class InstantiationTrackerSuite extends DatabricksTest {

  test("Per-process singleton") {
    // Test plan: verify that multiple calls to `enforceAndRecord` with PerProcessSingleton throws
    // an exception containing the source code location of the first instantiation.
    val tracker = InstantiationTracker.create[PerProcessSingletonType]()
    tracker.enforceAndRecord(PerProcessSingleton)

    assertThrow[IllegalStateException]("InstantiationTrackerSuite.scala:18") {
      tracker.enforceAndRecord(PerProcessSingleton)
    }
  }

  test("Per-process, per-key singleton") {
    // Test plan: verify that multiple calls to `enforceAndRecord` with different keys does not
    // throw until an existing key is reused.
    case class UniquenessKey(value: String)
    val tracker = InstantiationTracker.create[UniquenessKey]()
    tracker.enforceAndRecord(UniquenessKey("key1"))
    tracker.enforceAndRecord(UniquenessKey("key2"))

    assertThrow[IllegalStateException]("InstantiationTrackerSuite.scala:31") {
      tracker.enforceAndRecord(UniquenessKey("key2"))
    }
  }

  test("Concurrent instantiations") {
    // Test plan: verify that concurrent calls to `enforceAndRecord` with the same key results in
    // exactly one call throwing. If the implementation has a concurrency bug (due to using a
    // ConcurrentHashMap, for example), we would expect this test to be flaky.

    val tracker = InstantiationTracker.create[PerProcessSingletonType]()

    val executor: ExecutorService = new ForkJoinPool()
    val exitStatusFutures: Seq[Future[Unit]] = for (_ <- 0 until 10) yield {
      executor.submit[Unit] { () =>
        tracker.enforceAndRecord(PerProcessSingleton)
      }
    }

    AssertionWaiter("wait for all tasks to return").await {
      assert(exitStatusFutures.forall { exitStatusFuture: Future[Unit] =>
        exitStatusFuture.isDone
      })
    }

    // Exactly one should have succeeded, and the other 9 should have thrown.
    assert(exitStatusFutures.count { exitStatusFuture: Future[Unit] =>
      try {
        exitStatusFuture.get()
        false
      } catch {
        case _: Throwable => true
      }
    } == 9)
  }
}
