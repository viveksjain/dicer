package com.databricks.rpc

/** Dummy implementation for test compatibility with the internal version. */
sealed abstract class HttpMethod(val name: String)

object HttpMethod {
  case object GET extends HttpMethod("GET")
  case object POST extends HttpMethod("POST")
  case object PUT extends HttpMethod("PUT")
  case object DELETE extends HttpMethod("DELETE")
  case object HEAD extends HttpMethod("HEAD")
  case object OPTIONS extends HttpMethod("OPTIONS")
  case object PATCH extends HttpMethod("PATCH")

  /** Returns the constant object of this type with the specified name. */
  def valueOf(method: String): HttpMethod = method match {
    case "GET" => GET
    case "POST" => POST
    case "PUT" => PUT
    case "DELETE" => DELETE
    case "HEAD" => HEAD
    case "OPTIONS" => OPTIONS
    case "PATCH" => PATCH
  }
}
