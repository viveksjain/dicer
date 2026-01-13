package com.databricks

/** Lightweight trait for components that can provide debug information. */
trait HasDebugString {

  /** Provides HTML-formatted debug output for web display. */
  def debugHtml: String
}
