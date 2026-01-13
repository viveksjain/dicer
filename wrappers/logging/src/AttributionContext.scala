package com.databricks.logging

/** Provides request metatdata across method calls. In OSS this is a no-op. */
final class AttributionContext private () {}

object AttributionContext {

  /** A singleton instance that represents the "current" attribution context. */
  private val defaultContext = new AttributionContext()

  /** Returns the current attribution context. */
  def current: AttributionContext = defaultContext

  /** A background attribution context. */
  def background: AttributionContext = defaultContext
}
