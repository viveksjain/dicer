package com.databricks.rpc

import com.databricks.common.http.HttpRequestInfo
import com.databricks.common.serviceidentity.ServiceIdentity

/** Context of the RPC call, used for identifying the caller of the RPC call. */
case class RPCContext() {

  /** Identity of the caller service. Always returns None in the open source version. */
  def getCallerIdentity: Option[ServiceIdentity] = None

  val httpRequest: HttpRequestInfo = new HttpRequestInfo()
}
