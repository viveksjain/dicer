package com.databricks.dicer.assigner

import com.databricks.caching.util.SequentialExecutionContext
import com.databricks.dicer.common.Incarnation

class InMemoryStoreSuite extends CommonStoreSuiteBase {
  override def createStore(sec: SequentialExecutionContext): Store =
    InMemoryStore(sec, Incarnation.MIN)

  /** No cleanup of resources for InMemoryStore. */
  override def destroyStore(store: Store): Unit = {}
}
