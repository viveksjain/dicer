package com.databricks.common.serviceidentity

/** Identity of the caller service. Exists for internal code compatibility. */
case class ServiceIdentity(serviceName: String, shardName: String)
