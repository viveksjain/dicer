package com.databricks.conf

import com.databricks.rpc.DatabricksObjectMapper

/**
 * Takes the raw JSON configuration input, and parses it into the given type T.
 * Note that even simple strings will be quoted (as required for valid JSON).
 * @tparam T the output type that the parser will produce.
 */
trait ConfigParser[T] {

  /**
   * Parses the JSON string into type T.
   * @throws ConfigParseException if parsing fails.
   */
  def parse(mapper: DatabricksObjectMapper, json: String): T
}

/**
 * Default configuration parser that resolves a quoted string representation of a list to a value of
 * list. For example, given json
 *     "\"[\"a\", \"b\", \"c\"]\""
 * and `T: Seq`, the parser will parse it into a list and return
 *    Seq("a", "b", "c")
 * If `json` is not a quoted string list, or if `T` is not a list type, we pass-through the input to
 * [[DatabricksObjectMapper]].
 */
class DefaultParser[T: Manifest] extends ConfigParser[T] {
  override def parse(mapper: DatabricksObjectMapper, json: String): T = {
    val isSeqType: Boolean = (manifest[T].runtimeClass.isAssignableFrom(classOf[Seq[AnyRef]])
      || manifest[T].runtimeClass.isAssignableFrom(classOf[List[AnyRef]])
      || manifest[T].runtimeClass.isArray)
    val isJsonStr = json.startsWith("\"[") && json.endsWith("]\"")
    if (isSeqType && isJsonStr) {
      val strippedJson = DatabricksObjectMapper.fromJson[String](json)
      mapper.readValue[T](strippedJson)
    } else {
      // Pass through the Jackson-deserialized form.
      mapper.readValue[T](json)
    }
  }
}

/** Exception thrown when configuration parsing fails. */
class ConfigParseException(msg: String, cause: Throwable = null)
    extends RuntimeException(msg, cause)
