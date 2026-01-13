package com.databricks.caching.util

import com.google.protobuf.ByteString

import com.databricks.testing.DatabricksTest

class BytesSuite extends DatabricksTest {
  test("Bytes toString") {
    // Test plan: run toString on the end keys, some printable and some non-printable keys. Ensure
    // that the human-readable strings are as expected for both byte arrays and the corresponding
    // ByteStrings.

    val min = ByteString.EMPTY
    val printable = ByteString.copyFromUtf8("Hello")
    val unprintable = ByteString.copyFrom(Array[Byte](0, 20, 31, 127, -35, -1, -128))
    val partial = ByteString.copyFrom(Array[Byte](0, 20, 65, 31, 127, 97, -35, -1, 99, -128))
    val eightBytePrintable = ByteString.copyFromUtf8("longword")
    logger.info(s"Keys: $min $printable $unprintable $partial $eightBytePrintable")
    assert(Bytes.toString(min) == "\"\"")

    assert(Bytes.toString(printable) == "Hello")

    assert(Bytes.toString(unprintable) == "\\x00\\x14\\x1F\\x7F\\xDD\\xFF\\x80")

    assert(Bytes.toString(partial) == "\\x00\\x14A\\x1F\\x7Fa\\xDD\\xFFc\\x80")

    assert(Bytes.toString(eightBytePrintable) == "longword")
  }

  test("Bytes toString truncated") {
    // Test plan: check that toString truncates long strings correctly.
    val bytes = ByteString.copyFromUtf8("The quick brown fox jumps over the lazy dog")
    assert(Bytes.toString(bytes, 16) == "The quick brown ...")

    // Check boundary cases.
    assert(
      Bytes.toString(bytes, 0) == "\"\""
    )
    assert(
      Bytes.toString(bytes, bytes.size() - 1) == "The quick brown fox jumps over the lazy do..."
    )
    assert(
      Bytes.toString(bytes, bytes.size()) == "The quick brown fox jumps over the lazy dog"
    )
    assert(
      Bytes.toString(bytes, bytes.size() + 1) == "The quick brown fox jumps over the lazy dog"
    )
  }

  test("Bytes toString not truncated") {
    val byteString1 = ByteString.copyFromUtf8("The quick brown fox jumps over the lazy dog")
    assert(
      Bytes.toString(byteString1, byteString1.size) ==
      "The quick brown fox jumps over the lazy dog"
    )
    assert(
      Bytes.toString(byteString1, byteString1.size + 10) ==
      "The quick brown fox jumps over the lazy dog"
    )
  }

  test("override isPrintable") {
    // test plan: Verify that Bytes.toString respects custom isPrintable predicates when converting
    // ByteStrings to human-readable format. The default isPrintable considers ASCII chars 32-126
    // as printable, but this test overrides it to only consider letters as printable. Digits "123"
    // should be hex-escaped as \x31\x32\x33 while letters "ABC" remain as-is.
    val byteString = ByteString.copyFromUtf8("ABC123")
    assert(
      """ABC\x31\x32\x33""" == Bytes.toString(byteString, (b: Int) => Character.isLetter(b.toChar))
    )
  }
}
