package com.databricks.caching.util

import com.databricks.testing.DatabricksTest

class AsciiTableSuite extends DatabricksTest {

  test("AsciiTable simple") {
    // Test plan: create a table with two columns, add three rows, and verify the output.
    val table = new AsciiTable(AsciiTable.Header("Name", 10), AsciiTable.Header("Age", 3))
      .appendRow("Alice", "30")
      .appendRow("Bob", "20")
      .appendRow("Charlie", "40")
    assert(
      table.toString() ==
      """┌─────────┬─────┐
        |│ Name    │ Age │
        |├─────────┼─────┤
        |│ Alice   │ 30  │
        |│ Bob     │ 20  │
        |│ Charlie │ 40  │
        |└─────────┴─────┘
        |""".stripMargin
    )
  }

  test("AsciiTable truncation") {
    // Test plan: create a table with three columns each with a different max width. Add three rows,
    // each containing values overflowing one of the columns, and verify the output renders the
    // truncated values correctly.
    val table = new AsciiTable(
      AsciiTable.Header("Foo", 5),
      AsciiTable.Header("Bar", 3),
      AsciiTable.Header("Foobar", 8)
    ).appendRow("123456", "123", "12345678")
      .appendRow("12345", "1234", "12345678")
      .appendRow("12345", "123", "123456789")
    assert(
      table.toString() ==
      """┌───────┬─────┬──────────┐
        |│ Foo   │ Bar │ Foobar   │
        |├───────┼─────┼──────────┤
        |│ 12... │ 123 │ 12345678 │
        |│ 12345 │ ... │ 12345678 │
        |│ 12345 │ 123 │ 12345... │
        |└───────┴─────┴──────────┘
        |""".stripMargin
    )
  }
}
