# FAQ
1. *How many keys (or SliceKeys) can Dicer support? Can I have billions or trillions of keys?*
    
    The Dicer service deals in slices, and not individual keys, allowing an application’s key set to scale without impact on the Dicer service.

1. *How much QPS can Dicer handle?*
    
    Dicer's service costs are proportional to the number of pods in your service, and QPS imposes no additional cost (QPS is reflected in load reports to Dicer, but those reports are received at a steady rate that is independent of the observed load). The Slicelet and Clerk libraries do very little work in the request hot path (lookup in local data structure basically).

1. *Do I need to report the load to Dicer?*
    
    Yes, this is required, and all load must be reported, including any load that was shed or throttled. Dicer cannot load balance what it does not know about.

1. *Can I tune Dicer to never reassign a key, or at least do so very infrequently?*
    
    While Dicer provides an [imbalance\_tolerance\_hint](dicer/external/proto/target.proto) parameter that allows applications to trade higher load imbalance for reduced keyspace churn under skewed workloads, assignment changes remain inevitable when pod health changes or during rolling restarts.

1. *What happens if Dicer goes down?*
    
    RPCs will continue to flow, as the Dicer assignment is cached locally on Clerks and Slicelets, and will remain in effect even if the Clerks and Slicelets are not able to watch for the current assignment. Also, Slicelets will continue to distribute the (cached) assignment to new clients (Clerks), allowing them to also send requests according to the assignment. However, the assignment will not be updated in response to health or load changes, or rolling restarts where pod addresses change.

1. *How long does it take Dicer to reassign keys during a pod restart?*
    
    Dicer supports zero-down-time for planned restarts (e.g. during a binary update). Dicer gets notified before the Pod restarts (thanks to Kubernetes grace periods, defaulted to 30s) and moves slices away in advance of the restart. If the pod crashes, on the other hand, Dicer will detect the crash in roughly 30s to move the slices away. Some notes:
    1. Applications should not change terminationGracePeriod to \< 30 seconds or greater than 5 minutes.
    1. Pods with multiple containers with Slicelets are not supported and may not have zero downtime rolling updates.

1. *What kinds of hash functions are suitable for generating SliceKeys?*
    
    FarmHash is a good one, but whatever you choose, it’s important that it is stable (i.e. will produce the same output given the same input now and for all eternity across all processes/runtimes/languages etc) and is high-quality (i.e. the output has high entropy).
    

1. *Is it possible to change my choice of key to use with Dicer in the future? E.g. start with userID as a key, but later change to using userID:appID?*

    Changing keys will mean there is some period (i.e. during rollout) where the same data in your application is referenced by two different SliceKeys, and will therefore be assigned to twice as many resources. Typically, key changes involve draining traffic, making the key change, and undraining, or serving the same data in multiple places for each of the two key types during the rollout. If you need help working out the best strategy for your application, please reach out.
