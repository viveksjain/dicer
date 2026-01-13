package com.databricks.rpc

/** Dummy implementation of request headers in OSS. */
class RequestHeaders {
  def contains(header: String): Boolean = false
}

object RequestHeaders {
  def builder(): RequestHeadersBuilder = new RequestHeadersBuilder()
}

/** Dummy implementation of request headers builder in OSS that is a no-op. */
class RequestHeadersBuilder {
  def method(method: HttpMethod): RequestHeadersBuilder = this
  def path(path: String): RequestHeadersBuilder = this
  def add(headerName: String, headerValue: String): RequestHeadersBuilder = this
  def build(): RequestHeaders = new RequestHeaders()
}
