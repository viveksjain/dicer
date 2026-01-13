package com.databricks.rpc.tls

import io.etcd.jetcd

/** Dummy implementation in open-source for JetcdTLS. Currently SSL is not supported. */
object JetcdTLS {
  def configureClient(opts: TLSOptions, builder: jetcd.ClientBuilder): Unit = {}
}
