package com.databricks.rpc

/** Dummy implementation for test compatibility with the internal version. */
class HttpRequest {
  def headers(): RequestHeaders = new RequestHeaders()
}

object HttpRequest {
  def of(method: HttpMethod, path: String): HttpRequest = new HttpRequest()
}
