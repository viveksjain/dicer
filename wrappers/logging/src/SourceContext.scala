package com.databricks.macros

import _root_.sourcecode.{File => ExtFile, Line => ExtLine, FullName => ExtFullName}

/**
 * Provides compile-time File, Line, and FullName information. This is a thin wrapper around
 * [[https://github.com/com-lihaoyi/sourcecode]] library. We have this wrapper so that we can
 * keep the same structure as the internal version.
 */
object sourcecode {

  /** Represents a source code file path, captured at compile time. */
  type File = ExtFile

  /** Companion object providing File macro functionality. */
  val File: ExtFile.type = ExtFile

  /** Represents a source code line number, captured at compile time. */
  type Line = ExtLine

  /** Companion object providing Line macro functionality. */
  val Line: ExtLine.type = ExtLine

  /** Represents the innermost enclosing lexical context, captured at compile time. */
  type FullName = ExtFullName

  /** Companion object providing FullName macro functionality. */
  val FullName: ExtFullName.type = ExtFullName
}
