package com.databricks.caching.util

import com.databricks.caching.util.TestUtils.assertThrow
import com.databricks.testing.DatabricksTest

class AssertMacrosSuite extends DatabricksTest {

  test("iassert should not evaluate message thunk if condition is true") {
    // Test plan: Verify that iassert does not throw an exception and does not evaluate the
    // message thunk if the condition is true.

    // Define a function that lets us track whether the message thunk was evaluated.
    var thunkEvaluated = false
    def onThunkEvaluated: Boolean = {
      thunkEvaluated = true
      thunkEvaluated
    }

    AssertMacros.iassert(true, s"Message thunk that should NOT be evaluated ${onThunkEvaluated}")
    assert(thunkEvaluated == false)
  }

  test("iassert should throw AssertionError and evaluate message if condition is false") {
    // Test plan: Verify that iassert throw an exception and evaluates the message thunk if
    // the condition is false.

    // Define a function that lets us track whether the message thunk was evaluated.
    var thunkEvaluated = false
    def onThunkEvaluated: Boolean = {
      thunkEvaluated = true
      thunkEvaluated
    }

    assertThrow[AssertionError]("Message thunk that should be evaluated true") {
      AssertMacros.iassert(false, s"Message thunk that should be evaluated ${onThunkEvaluated}")
    }
    assert(thunkEvaluated == true)
  }

  test("iassert without message") {
    // Test plan: Verify that iassert does not throw an exception if the condition is true, and does
    // otherwise.
    AssertMacros.iassert(true)
    assertThrow[AssertionError]("assertion failed") {
      AssertMacros.iassert(false)
    }
  }

  test("ifail") {
    // Test plan: verify that ifail works as expected with unreachable code.
    def classify(i: Int): String = {
      i % 2 match {
        case 0 => "even"
        case 1 => "odd"
        case -1 => "odd"
        // This compiles because ifail is declared to return Nothing.
        case _ => AssertMacros.ifail(s"i is neither even nor odd: $i")
      }
    }
    assert(classify(2) == "even")
    assert(classify(-1) == "odd")
    assert(classify(7) == "odd")
  }

  test("ifail (failure)") {
    // Test plan: Verify that ifail throws an exception if "unreachable" code is reached.
    assertThrow[AssertionError]("reached unreachable code: foo") {
      AssertMacros.ifail("foo")
    }
  }
}
