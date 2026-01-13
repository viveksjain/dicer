package com.databricks.common.alias

import com.google.protobuf.CodedInputStream
import _root_.scalapb.{GeneratedMessageCompanion, GeneratedMessage, Message}

/*
 * In open source environment, we depend on ScalaPB 1.0. However, internally we use ScalaPB 0.9.1.
 * This file provides compatibility shims so that we can use the same codebase in both environments.
 */
object RichScalaPB {

  implicit class RichMessage[T <: Message[T] with GeneratedMessage](message: T) {
    def mergeFrom(input: CodedInputStream): T =
      message.companion.asInstanceOf[GeneratedMessageCompanion[T]].merge(message, input)
  }
}
