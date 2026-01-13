package com.databricks.dicer.external

import java.util.concurrent.locks.ReentrantLock

import com.google.common.collect.{ImmutableRangeSet, Range}

import scala.collection.JavaConverters.asJavaIterable

import com.databricks.backend.common.util.Project
import com.databricks.conf.Config
import com.databricks.conf.trusted.ProjectConf
import com.databricks.conf.trusted.RPCPortConf
import com.databricks.logging.ConsoleLogging
import com.databricks.rpc.SslArguments
import com.databricks.rpc.tls.{TLSOptions, TLSOptionsMigration}

/**
 * This file contains abstractions that represent sample configurations and services that customers
 * of Dicer are expected to have. They show patterns on how one can use Dicer in test. The code
 * pattern may not correspond completely to a customer's code - please use judgement what makes
 * sense and/or reach out to Caching team.
 */
object Samples {

  /**
   * [[SampleClientConf]] represents a Conf that is expected to present in the production code for a
   * service using Dicer. It should extend [[ClerkConf]] and override dicerSslArgs to provide the
   * SSL arguments that it uses for its own services.
   */
  class SampleClientConf(project: Project.Project, rawConfig: Config)
      extends ProjectConf(project, rawConfig)
      with ClerkConf
      with RPCPortConf {
    // Configure sslArgs based on Truststore and Keystore for your service. For test environment, we
    // need to override this because of the difference in TLS/SSL setup, so we leave this
    // disabled here. PLEASE DON'T DO THIS IN PRODUCTION.
    val sslArgs: SslArguments = SslArguments.disabled

    // Pass the configured TLS/SSL arguments to be used for connection to Dicer.
    // In test code, these will be configured by the DicerTestEnvironment.
    override def dicerTlsOptions: Option[TLSOptions] = TLSOptionsMigration.convert(sslArgs)
  }

  /**
   * [[SampleServerConf]] represents a Conf that would be used by service being sharded. It should
   * extend [[SliceletConf]] and override dicerSslArgs.
   */
  class SampleServerConf(project: Project.Project, rawConfig: Config)
      extends ProjectConf(project, rawConfig)
      with SliceletConf
      with RPCPortConf {
    // Read the comment in [[SampleClientConf]].
    val sslArgs: SslArguments = SslArguments.disabled

    // Pass the configured TLS/SSL arguments to be used for connection to Dicer.
    // In test code, these will be configured by the DicerTestEnvironment.
    override def dicerTlsOptions: Option[TLSOptions] = TLSOptionsMigration.convert(sslArgs)
  }

  /** Converts the given `slice` to a Guava `Range`. */
  def sliceToRange(slice: Slice): Range[SliceKey] = slice.highExclusive match {
    case highExclusive: SliceKey => Range.closedOpen(slice.lowInclusive, highExclusive)
    case InfinitySliceKey => Range.atLeast(slice.lowInclusive)
  }

  /** A sample server for the given `target` that tracks the listener and Slicelet. */
  class SampleServer(conf: SampleServerConf, target: Target) extends ConsoleLogging {
    private val slicelet = Slicelet(conf, target)

    override def loggerName: String = getClass.getName

    /**
     * A SliceletListener that keeps track of ranges assigned to the Slicelet that owns this
     * listener.
     */
    object Listener extends SliceletListener {

      /** The lock that guards all the state in the listener. */
      private val lock = new ReentrantLock()

      /** The Slices currently assigned to this Slicelet, if any. */
      private var assignedRanges: ImmutableRangeSet[SliceKey] = ImmutableRangeSet.of[SliceKey]()

      override def onAssignmentUpdated(): Unit = {
        // Convert assigned Slices to an ImmutableRangeSet supporting diffing.
        val assignedSlices: Seq[Slice] = slicelet.assignedSlices
        val currentAssignedRanges =
          ImmutableRangeSet.copyOf(asJavaIterable(assignedSlices.map(sliceToRange)))

        lock.lock()
        try {
          // Swap current assigned ranges.
          val previousAssignedRanges = this.assignedRanges
          this.assignedRanges = currentAssignedRanges

          // Remove ranges that are in the previous but not the current assignment.
          val removed: ImmutableRangeSet[SliceKey] =
            previousAssignedRanges.difference(currentAssignedRanges)

          // Add ranges that are in the current but not the previous assignment.
          val added: ImmutableRangeSet[SliceKey] =
            currentAssignedRanges.difference(previousAssignedRanges)

          logger.info(log"${conf.dicerSliceletRpcPort}, Added: $added, Removed: $removed")
        } finally lock.unlock()
      }

      /** Returns the ranges assigned to this Slicelet sorted by the low end point. */
      def getAssignedRanges: ImmutableRangeSet[SliceKey] = {
        try {
          lock.lock()
          this.assignedRanges
        } finally lock.unlock()
      }
    }

    /** Starts the server, i.e., create the Slicelet and a server. */
    def start(selfPort: Int): SampleServer = {
      logger.info(log"Started sample server ($target)")
      slicelet.start(selfPort, Some(Listener))
      this
    }

    /** Returns the Slicelet managed by this server. */
    def getSlicelet: Slicelet = slicelet
  }
}
