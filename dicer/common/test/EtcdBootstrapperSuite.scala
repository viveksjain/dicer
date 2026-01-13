package com.databricks.dicer.common

import com.databricks.caching.util.ServerTestUtils
import com.databricks.dicer.common.EtcdBootstrapper.{BootstrapRequest, ExitCode}
import com.databricks.caching.util.{EtcdTestEnvironment, EtcdClient, EtcdKeyValueMapper}
import com.databricks.caching.util.UnixTimeVersion
import com.databricks.rpc.DatabricksServerWrapper
import com.databricks.testing.DatabricksTest

class EtcdBootstrapperSuite extends DatabricksTest {
  private[this] val dockerizedEtcd = EtcdTestEnvironment.create()

  private[this] val NAMESPACE = EtcdClient.KeyNamespace("test-namespace")
  private[this] val NON_LOOSE_INCARNATION: Incarnation = Incarnation(2)
  private[this] val LOOSE_INCARNATION: Incarnation = Incarnation.MIN

  override def beforeEach(): Unit = {
    dockerizedEtcd.deleteAll()
  }

  override def afterAll(): Unit = {
    dockerizedEtcd.close()
  }

  namedGridTest("success written when no high watermark already exists")(
    Map("non-loose incarnation" -> NON_LOOSE_INCARNATION, "loose incarnation" -> LOOSE_INCARNATION)
  ) { incarnation: Incarnation =>
    // Test plan: Verify `bootstrapEtcd` for returns success exit code when no high watermark exists
    // in the etcd cluster. Verify this by calling `bootstrapEtcd` with a against an empty etcd
    // cluster, then verifying the returned exit code is `SUCCESS` and the high watermark is written
    // to the etcd cluster.
    assert(
      dockerizedEtcd
        .getKey(EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE))
        .isEmpty
    )

    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(
            dockerizedEtcd.createEtcdClient(
              EtcdClient.Config(NAMESPACE)
            ),
            incarnation
          )
        )
      )
      == ExitCode.SUCCESS
    )

    assert(
      dockerizedEtcd
        .getKey(EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE))
        .contains(
          EtcdKeyValueMapper.ForTest
            .toVersionValueString(EtcdClient.Version(incarnation.value, UnixTimeVersion.MIN))
        )
    )
  }

  test("when there is high watermark already exist, bootstrap returns with success") {
    // Test plan: Verify when there is high watermark existing in the etcd cluster, `bootstrapEtcd`
    // still returns `SUCCESS`, and the existing high watermark remains untouched. Verify this by
    // writing some high watermarks to the etcd cluster (with same and different store incarnation
    // with the previous one), then calling `bootstrapEtcd` against the etcd cluster. Verify the
    // returned exit code is `SUCCESS`, and the high watermark in the etcd cluster remains
    // untouched.
    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(
            dockerizedEtcd.createEtcdClient(
              EtcdClient.Config(NAMESPACE)
            ),
            NON_LOOSE_INCARNATION
          )
        )
      )
      == ExitCode.SUCCESS
    )
    assert(
      dockerizedEtcd
        .getKey(EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE))
        .contains(
          EtcdKeyValueMapper.ForTest.toVersionValueString(
            EtcdClient.Version(NON_LOOSE_INCARNATION.value, UnixTimeVersion.MIN)
          )
        )
    )

    // Try to re-bootstrap etcd with same store incarnation. Should report success and leave the
    // existing value untouched.
    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(
            dockerizedEtcd.createEtcdClient(
              EtcdClient.Config(NAMESPACE)
            ),
            NON_LOOSE_INCARNATION
          )
        )
      )
      == ExitCode.SUCCESS
    )
    assert(
      dockerizedEtcd
        .getKey(EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE))
        .contains(
          EtcdKeyValueMapper.ForTest.toVersionValueString(
            EtcdClient.Version(NON_LOOSE_INCARNATION.value, UnixTimeVersion.MIN)
          )
        )
    )

    // Try to re-bootstrap etcd with a different store incarnation. Like above, it should report
    // success and leave the existing value untouched.
    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(
            dockerizedEtcd.createEtcdClient(
              EtcdClient.Config(NAMESPACE)
            ),
            Incarnation(NON_LOOSE_INCARNATION.value + 2)
          )
        )
      )
      == ExitCode.SUCCESS
    )
    assert(
      dockerizedEtcd
        .getKey(EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE))
        .contains(
          EtcdKeyValueMapper.ForTest.toVersionValueString(
            EtcdClient.Version(NON_LOOSE_INCARNATION.value, UnixTimeVersion.MIN)
          )
        )
    )
  }

  test("when etcd data is corrupted, bootstrap returns failure exit code") {
    // Test plan: Verify when there is corrupted high watermark in the etcd cluster, `bootstrapEtcd`
    // returns `FAILURE`. Verify this by writing some corrupted high watermark to the etcd cluster,
    // then calling 'bootstrapEtcd` against it, verifying the returned exit code is `FAILURE`.
    dockerizedEtcd.put(
      EtcdKeyValueMapper.ForTest.getVersionHighWatermarkKeyString(NAMESPACE),
      "not-a-version"
    )
    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(
            dockerizedEtcd.createEtcdClient(
              EtcdClient.Config(NAMESPACE)
            ),
            NON_LOOSE_INCARNATION
          )
        )
      )
      == ExitCode.RETRYABLE_FAILURE
    )
  }

  test("Single failing request fails batch") {
    // Test plan: Verify that bootstrapEtcdBlocking() reports a failure when a single request in a
    // batch fails, even if the other requests succeed.
    val unresponsiveServer: DatabricksServerWrapper =
      ServerTestUtils.createUnresponsiveServer(port = 0)
    val failingClient = EtcdClient.create(
      etcdEndpoints = Seq(s"http://localhost:${unresponsiveServer.activePort()}"),
      tlsOptionsOpt = None,
      EtcdClient.Config(EtcdClient.KeyNamespace("failing-namespace"))
    )
    val workingClient = dockerizedEtcd.createEtcdClient(
      EtcdClient.Config(EtcdClient.KeyNamespace("successful-namespace"))
    )

    assert(
      EtcdBootstrapper.bootstrapEtcdBlocking(
        Seq(
          BootstrapRequest(failingClient, Incarnation(42)),
          BootstrapRequest(workingClient, Incarnation(43))
        )
      )
      == ExitCode.RETRYABLE_FAILURE
    )

    // Confirm that `workingClient` succeeded by reading etcd.
    assert(
      dockerizedEtcd
        .getKey(
          EtcdKeyValueMapper.ForTest
            .getVersionHighWatermarkKeyString(workingClient.config.keyNamespace)
        )
        .contains(
          EtcdKeyValueMapper.ForTest.toVersionValueString(
            EtcdClient.Version(43, UnixTimeVersion.MIN)
          )
        )
    )
  }
}
