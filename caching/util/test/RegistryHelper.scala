package com.databricks.caching.util

import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.CollectorRegistry
import scala.collection.JavaConverters._

object RegistryHelper {

  /**
   * Helper for the Prometheus metric registry. We need to have such a helper since the OSS
   * Prometheus Java client (0.10.0+) automatically appends "_total" suffix to counter metric names
   * as per the OpenMetrics standard, while the internal version does not. So we automatically try
   * both the original name and the name with "_total" suffix in case `metric` refers to a counter
   * metric.
   */
  def getMetricName(registry: CollectorRegistry, metric: String): String = {
    // In the implementation, we simply fold the cases of _counter and something that is not
    // found to be the same and add "_total".
    val metricSamplesIter: Iterator[MetricFamilySamples] =
      registry.filteredMetricFamilySamples(Set(metric).asJava).asScala
    if (metricSamplesIter.hasNext) {
      metric
    } else {
      metric + "_total"
    }
  }
}
