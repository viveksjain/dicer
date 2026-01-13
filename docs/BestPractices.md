## Best practices

1. Some amount of misdirected requests (requests where the server believes it does not own the key) are inevitable since the assignment is distributed asynchronously, and requests can be delayed in the network. However, a high percentage of misdirected requests can indicate a serious misconfiguration issue. We recommend recording misdirected requests in a server-side metric, setting up alerts that monitor the overall percentage of misdirected requests, and making sure those alerts are part of your release qualification process.

1. Generally speaking, services should not reject unaffinitized requests, but rather prefer to serve them even if that means degraded performance (e.g. due to cache misses). This optimizes for availability and follows the “RPCs must flow” principle.

1. Give thought to how load is measured and reported to Dicer. Simple services can often get away with reporting a load of 1000 for each request, but if your system has highly skewed requests costs (e.g. a write costs much more than a read), consider measuring the relative costs of request types and reporting the appropriate amount of load for each type.

1. In overload scenarios, report some load even for the requests which were throttled/rejected or otherwise load shed. This helps Dicer to understand the full picture of the offered load on the service and react more quickly and robustly to these situations.

1.  Do not create a Clerk per request. Clerks are thread-safe, and should be shared, as each Clerk creates a gRPC channel and consumes long-lived resources.

1. Clerks need to get an initial assignment after they are constructed before they can route requests to servers. If the downstream Dicer sharded service is a “hard” dependency of a client, consider waiting a bounded time for the Clerk to become ready before declaring readiness (i.e. via readiness probes) in order to avoid initial periods during startup where requests are failed due to the Clerk not having an initial assignment.

1. Clients and servers must generate the same SliceKey for the same application key. Consider factoring out common utilities to share between client and server to ensure that they are in sync.


## Considerations for load reporting and load balancing configuration

High-level advice on load and Dicer:

  * Keep it simple: it can be tricky to choose the load metric reported to Dicer. CPU? Hard to correctly attribute to requests/keys and noisy. Requests? Easy to attribute, probably a better choice\! In general, consider what the resource bottleneck is in your system, and report that load (or some relatively stable proxy for that load, e.g., request for CPU). In some applications, the bottleneck could even be memory or even the number of threads.

  * Keep it stable: radically changing the way load is reported to Dicer makes deployment risky, as Dicer will assume that the load reported by all Slicelets is comparable, even when only half of the Slicelets are using the new reporting scheme. Prefer small tweaks, and try to maintain the rough scale of the load reported. Otherwise Dicer will be comparing apples and oranges when making load balancing decisions\!

  * Dicer’s load metric is a “rate” based load metric. You may prefer to balance load based on a metric that is more “gauge”-like, such as memory or number of threads. In such cases, you will need to adapt your instantaneous measurements to approximate rates. For example, if you’re interested in load balancing memory, you can report the approximate “integral” of your memory gauge function to Dicer by sampling it. Pseudo-code:

```java
def reportLoad(): Unit = {
  val now = clock.now()
  val timeSinceLastReport = now - this.lastReportTime 
  for (workUnit <- activeWorkUnits) {
    Using.resource(slicelet.createHandle(workUnit.key)) {
      handle: SliceKeyHandle =>
        handle.incrementLoadBy(timeSinceLastReport * workUnit.measureMemory())
    }
  }
  this.lastReportTime = now
}

ec.periodicallyRun(() => reportLoad(), 1.second)
```

  * Adaptability: applications should apply some “scaling factor” to the load reported to Slicelets. For example, if you are primarily load balancing on requests, consider assigning an arbitrary cost of 1000 units per request. This gives some flexibility should you later decide to incorporate an additional load metric, or variable weights.
   
    NOTE: load is reported to the Slicelet as an Int rather than a Double, so it is not possible to provide alternate weights where 1.0 is your baseline. Dicer restricts the load reporting type so that it can internally make assumptions about the range of reported values.
