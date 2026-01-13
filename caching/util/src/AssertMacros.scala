package com.databricks.caching.util

import scala.reflect.macros.blackbox
import scala.language.experimental.macros

object AssertMacros {

  /**
   * Asserts an internal invariant that is always expected to be true (e.g. a representation
   * invariant), and disables line coverage reporting for the message expression. It is not useful
   * to report coverage gaps on the message for such an internal invariant because by design there
   * is no sequence of operations a test could use to cause it to be violated.
   *
   * This macro is a shorthand for:
   *
   * {{{
   *   if (!<condition>) {
   *     // $COVERAGE-OFF$
   *     throw new java.lang.AssertionError("assertion failed: " + <message>)
   *     // $COVERAGE-ON$
   *   }
   * }}}
   *
   * Note the following differences with [[Predef.assert()]]:
   *
   *  - The message is not a thunk, but is nonetheless lazily evaluated on `!condition` because this
   *    is a macro.
   *  - The condition is always evaluated, even when "-Xelide-below MAXIMUM" is passed to scalac. In
   *    practice, this should make no difference for build configurations used at Databricks, but it
   *    is nonetheless worth noting.
   */
  def iassert(condition: Boolean, message: String): Unit = macro Impl.iassertWithMessage

  /**
   * A version of [[iassert()]] without a message. This macro is a shorthand for:
   *
   * {{{
   *   if (!condition) {
   *     // $COVERAGE-OFF$
   *     throw new java.lang.AssertionError("assertion failed")
   *     // $COVERAGE-ON$
   *   }
   * }}}
   */
  def iassert(condition: Boolean): Unit = macro Impl.iassertWithoutMessage

  /**
   * Asserts that the current code path is unreachable, and disables line coverage reporting for the
   * nominally unreachable line.
   *
   * This macro is shorthand for:
   *
   * {{{
   *   // $COVERAGE-OFF$
   *   throw new java.lang.AssertionError("reached unreachable code: " + <message>)
   *   // $COVERAGE-ON$
   * }}}
   * */
  def ifail(message: String): Nothing = macro Impl.ifail

  /** The implementation for the `iassert` macro. */
  // Implementation notes:
  // - Scala requires that we declare the implementation of a macro as public. We nest the
  //   implementation in a private object to avoid exposing it to users of the library.
  // - Due to Scala macro limitations, the implementation has to express the type of the thunk
  //   expression imprecisely as `c.Expr[Any]`. Note that the public interface declares the type
  //   precisely as `message: String` and that compiler will generate an error if the argument
  //   does not conform to that type.
  // - We have defined unit tests to be sure that the message expression is not evaluated when the
  //   condition is true. This laziness is important because some messages may be expensive to
  //   format.
  private object Impl {
    def iassertWithMessage(
        c: blackbox.Context)(condition: c.Expr[Boolean], message: c.Expr[Any]): c.Expr[Unit] = {
      import c.universe._
      reify {
        if (!condition.splice) {
          // $COVERAGE-OFF$: Disable line coverage reporting for the unreachable message.
          throw new java.lang.AssertionError("assertion failed: " + message.splice)
          // $COVERAGE-ON$
        }
      }
    }

    def iassertWithoutMessage(c: blackbox.Context)(condition: c.Expr[Boolean]): c.Expr[Unit] = {
      import c.universe._
      reify {
        if (!condition.splice) {
          // $COVERAGE-OFF$: Disable line coverage reporting for the unreachable case.
          throw new java.lang.AssertionError("assertion failed")
          // $COVERAGE-ON$
        }
      }
    }

    def ifail(c: blackbox.Context)(message: c.Expr[Any]): c.Expr[Nothing] = {
      import c.universe._
      reify {
        // $COVERAGE-OFF$: Disable line coverage reporting for unreachable code.
        throw new java.lang.AssertionError("reached unreachable code: " + message.splice)
        // $COVERAGE-ON$
      }
    }
  }
}
