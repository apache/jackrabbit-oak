<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->
# Metrics

The `DocumentNodeStore` and the underlying `DocumentStore` expose a number of
metrics that can be used to monitor a running system.

Depending on the `StatisticsProvider` in use by Oak, these metrics can then be
accessed via JMX or bridged to [Apache Sling Metrics](https://sling.apache.org/documentation/bundles/metrics.html).

There are different types of metrics used by the `DocumentNodeStore` and the
`DocumentStore`:

- Counter: a monotonically increasing number. Though, the value will be reset
  to zero when Oak is restarted.
- Meter: measures the rate of an event or action per second.
- Timer: measures the rate of an event or action per second and provides
  information on the distribution of the duration (percentiles).

## DocumentNodeStore

The following `DocumentNodeStore` Counter metrics are available, independent of
the underlying `DocumentStore` in use (MongoDB, RDB, etc.):

Name | Description | Unit
-----|-------------|-----
CacheStats.Document-Documents.element | The number of elements (documents) in the Documents cache. | |
CacheStats.Document-Documents.eviction | The number of evictions from the Documents cache. | |
CacheStats.Document-Documents.hit | The number of cache hits for the Documents cache. | |
CacheStats.Document-Documents.loadTime | The accumulated load time on a Documents cache miss. | nanoseconds |
CacheStats.Document-Documents.miss | The number of cache misses for the Documents cache. | |
CacheStats.Document-Documents.request | The number of lookup requests made on the Documents cache. This is the sum of cache hits and misses. | |
CacheStats.Document-LocalDiff.element | The number of elements in the LocalDiff cache. | |
CacheStats.Document-LocalDiff.eviction | The number of evictions from the LocalDiff cache. | |
CacheStats.Document-LocalDiff.hit | The number of cache hits for the LocalDiff cache. | |
CacheStats.Document-LocalDiff.loadTime | The accumulated load time on the LocalDiff cache. Please note, the LocalDiff cache is pro-actively populated and never loads on a cache miss. This value will always be zero. | nanoseconds |
CacheStats.Document-LocalDiff.miss | The number of cache misses for the LocalDiff cache. | |
CacheStats.Document-LocalDiff.request | The number of lookup requests made on the LocalDiff cache. This is the sum of cache hits and misses. | |
CacheStats.Document-MemoryDiff.element | The number of elements in the MemoryDiff cache. | |
CacheStats.Document-MemoryDiff.eviction | The number of evictions in the MemoryDiff cache. | |
CacheStats.Document-MemoryDiff.hit | The number of hits for the MemoryDiff cache. | |
CacheStats.Document-MemoryDiff.loadTime | The accumulated load time on the MemoryDiff cache. | nanoseconds |
CacheStats.Document-MemoryDiff.miss | The number of cache misses for the MemoryDiff cache. | |
CacheStats.Document-MemoryDiff.request | The number of lookup requests made on the MemoryDiff cache. This is the sum of cache hits and misses. | |
CacheStats.Document-NodeChildren.element | The number of elements in the NodeChildren cache. | |
CacheStats.Document-NodeChildren.eviction | The number of evictions in the NodeChildren cache. | |
CacheStats.Document-NodeChildren.hit | The number of hits for the NodeChildren cache. | |
CacheStats.Document-NodeChildren.loadTime | The accumulated load time on the NodeChildren cache. | nanoseconds |
CacheStats.Document-NodeChildren.miss | The number of cache misses for the NodeChildren cache. | |
CacheStats.Document-NodeChildren.request | The number of lookup requests made on the NodeChildren cache. This is the sum of cache hits and misses. | |
CacheStats.Document-NodeState.element | The number of elements in the NodeState cache. | |
CacheStats.Document-NodeState.eviction | The number of evictions in the NodeState cache. | |
CacheStats.Document-NodeState.hit | The number of hits for the NodeState cache. | |
CacheStats.Document-NodeState.loadTime | The accumulated load time on the NodeState cache. | nanoseconds |
CacheStats.Document-NodeState.miss | The number of cache misses for the NodeState cache. | |
CacheStats.Document-NodeState.request | The number of lookup requests made on the NodeState cache. This is the sum of cache hits and misses. | |
CacheStats.Document-PrevDocuments.element | The number of elements (documents) in the PrevDocuments cache. | |
CacheStats.Document-PrevDocuments.eviction | The number of evictions in the PrevDocuments cache. | |
CacheStats.Document-PrevDocuments.hit | The number of hits for the PrevDocuments cache. | |
CacheStats.Document-PrevDocuments.loadTime | The accumulated load time on the PrevDocuments cache. | nanoseconds |
CacheStats.Document-PrevDocuments.miss | The number of cache misses for the PrevDocuments cache. | |
CacheStats.Document-PrevDocuments.request | The number of lookup requests made on the PrevDocuments cache. This is the sum of cache hits and misses. | |

More information on CacheStats is also available in the [CacheStatsMBean](https://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/api/jmx/CacheStatsMBean.html)
of the Oak API.

The following Meters are available, each exposing a cumulative count and various
rates for the event (mean, one minute, five minute, fifteen minute):

Name | Description | Rate unit
-----|-------------|----------
DOCUMENT_NS_BGR_LAG | The approximate lag in milliseconds of external changes the async background reader pulls in from other cluster nodes. | average lag in milliseconds
DOCUMENT_NS_BGR_NUM_CHANGES_RATE | The number of changes the async background reader pulls in from other cluster nodes. This does not necessarily mean a cluster node reads the documents for all those changes, but it must at least invalidate the affected documents in the cache.| changes per second
DOCUMENT_NS_BGW_NUM_WRITE_RATE | The number of documents the async background writer updates and pushes to the DocumentStore. | updates per second
DOCUMENT_NS_BRANCH_COMMIT_COUNT | The number of branch commits performed by this DocumentNodeStore. | branch commits per second
DOCUMENT_NS_MERGE_BRANCH_COMMIT_COUNT | The number of branch commits merged. This count can be lower than the branch commit count because some branch commits have not yet been merged or never will be merged. | branch commits merged per second
DOCUMENT_NS_MERGE_CHANGES_RATE | The number of changes merged. A change is defined by an update or insert of a document. | changes per second
DOCUMENT_NS_MERGE_FAILED_EXCLUSIVE | The number of failed merges while holding the merge lock exclusively. | failed merges per second
DOCUMENT_NS_MERGE_SUCCESS_COUNT | The number of successful merges | merges per second
DOCUMENT_NS_MERGE_SUCCESS_EXCLUSIVE | The number of successful merges while holding the merge lock exclusively. | merges per second
DOCUMENT_NS_MERGE_SUCCESS_SUSPENDED | The number of successful merges that previously had to be suspended because of a not yet visible conflict from another cluster node. | merges per second

> Note: a merge operation on the DocumentNodeStore level is roughly equivalent
to a JCR Session save() operation.

A `DocumentNodeStore` also exposes the following Timers with different
percentiles for the duration of an operation. The timings include the latency
between Oak and a backend store like MongoDB.

Name | Description
-----|------------
DOCUMENT_NS_BGR_CACHE_INVALIDATE | The time it takes to invalidate affected cache entries on a background read operation.
DOCUMENT_NS_BGR_DIFF_CACHE | The time it takes to populate the diff cache for external changes on a background read operation.
DOCUMENT_NS_BGR_DISPATCH | The time it takes the `DocumentNodeStore` to dispatch external changes on a background read operation.
DOCUMENT_NS_BGR_LOCK | The time it takes to acquire locks on a background read operation.
DOCUMENT_NS_BGR_READ_HEAD | The time it takes to read the root document on a background read operation.
DOCUMENT_NS_BGR_TOTAL_TIME | The total time of a background read operation. 
DOCUMENT_NS_BGW_CLEAN | The time it takes to clean up orphaned branches and collisions on a background write operation.
DOCUMENT_NS_BGW_LOCK | The time it takes to acquire locks on a background write operation.
DOCUMENT_NS_BGW_SPLIT | The time it takes to split documents in a background write operation.
DOCUMENT_NS_BGW_SWEEP | The time it takes to sweep documents in a background write operation.
DOCUMENT_NS_BGW_TOTAL_TIME | The total time of a background write operation.
DOCUMENT_NS_LEASE_UPDATE | The time it takes to update the lease.
DOCUMENT_NS_MERGE_SUCCESS_TIME | The time it takes a successful merge to complete. This does not include merge attempts that failed.
DOCUMENT_NS_MERGE_SUCCESS_NORMALIZED_TIME | The normalized time it takes a successful merge to complete. Normalization is done by dividing the merge time by the number of merged changes.
DOCUMENT_NS_HEAD_OF_QUEUE_WAIT_TIME | The time a merge had to wait in the commit queue until it was the head of the queue and could proceed.
DOCUMENT_NS_MERGE_SUSPEND_TIME | The time a merge was suspended due to a conflicting write on another cluster node.
DOCUMENT_NS_MERGE_LOCK_TIME | The time it took to acquire the merge lock.
DOCUMENT_NS_MERGE_COMMIT_HOOK_TIME | The time it took to run the commit hook on the changes to merge.
DOCUMENT_NS_MERGE_CHANGES_APPLIED_TIME | The time it took to apply the changes to the DocumentStore.

> Note: Oak tracks the duration for above timers at millisecond or microsecond
accuracy, but tools may translate the duration percentiles to different units.

If a `DocumentNodeStore` is configured with persistent caches, then the
following Meters are available:

Name | Description | Rate unit
-----|-------------|----------
PersistentCache.NodeCache.children.BROADCAST_RECV | The number of broadcast messages received by the `children` persistent cache. | messages per second 
PersistentCache.NodeCache.children.CACHE_PUT | The number of entries put into the `children` persistent cache. | entries per second
PersistentCache.NodeCache.children.HITS | The number of hits for the `children` persistent cache. | hits per second
PersistentCache.NodeCache.children.INVALIDATE_ALL | The number of invalidate all operations on the `children` persistent cache. | events per second
PersistentCache.NodeCache.children.INVALIDATE_ONE | The number of times an entry from the `children` persistent cache was explicitly invalidated. | entries per second
PersistentCache.NodeCache.children.LOAD_EXCEPTIONS | The number of times a loader threw an exception on a cache miss for the `children` persistent cache. | exceptions per second
PersistentCache.NodeCache.children.PUT_REJECTED_SECONDARY_CACHE | The number of rejected put operations on the `children` persistent cache because the configuration excludes the key. | rejects per second
PersistentCache.NodeCache.children.REQUESTS | The number of lookup requests made on the `children` persistent cache. This does not include lookup operations for keys that have been excluded by configuration. | requests per second
PersistentCache.NodeCache.diff.BROADCAST_RECV | The number of broadcast messages received by the `diff` persistent cache. | messages per second 
PersistentCache.NodeCache.diff.CACHE_PUT | The number of entries put into the `diff` persistent cache. | entries per second
PersistentCache.NodeCache.diff.HITS | The number of hits for the `diff` persistent cache. | hits per second
PersistentCache.NodeCache.diff.INVALIDATE_ALL | The number of invalidate all operations on the `diff` persistent cache. | events per second
PersistentCache.NodeCache.diff.INVALIDATE_ONE | The number of times an entry from the `diff` persistent cache was explicitly invalidated. | entries per second
PersistentCache.NodeCache.diff.LOAD_EXCEPTIONS | The number of times a loader threw an exception on a cache miss for the `diff` persistent cache. | exceptions per second
PersistentCache.NodeCache.diff.PUT_REJECTED_SECONDARY_CACHE | The number of rejected put operations on the `diff` persistent cache because the configuration excludes the key. | rejects per second
PersistentCache.NodeCache.diff.REQUESTS | The number of lookup requests made on the `diff` persistent cache. This does not include lookup operations for keys that have been excluded by configuration. | requests per second
PersistentCache.NodeCache.local_diff.BROADCAST_RECV | The number of broadcast messages received by the `local_diff` persistent cache. | messages per second 
PersistentCache.NodeCache.local_diff.CACHE_PUT | The number of entries put into the `local_diff` persistent cache. | entries per second
PersistentCache.NodeCache.local_diff.HITS | The number of hits for the `local_diff` persistent cache. | hits per second
PersistentCache.NodeCache.local_diff.INVALIDATE_ALL | The number of invalidate all operations on the `local_diff` persistent cache. | events per second
PersistentCache.NodeCache.local_diff.INVALIDATE_ONE | The number of times an entry from the `local_diff` persistent cache was explicitly invalidated. | entries per second
PersistentCache.NodeCache.local_diff.LOAD_EXCEPTIONS | The number of times a loader threw an exception on a cache miss for the `local_diff` persistent cache. | exceptions per second
PersistentCache.NodeCache.local_diff.PUT_REJECTED_SECONDARY_CACHE | The number of rejected put operations on the `local_diff` persistent cache because the configuration excludes the key. | rejects per second
PersistentCache.NodeCache.local_diff.REQUESTS | The number of lookup requests made on the `local_diff` persistent cache. This does not include lookup operations for keys that have been excluded by configuration. | requests per second
PersistentCache.NodeCache.node.BROADCAST_RECV | The number of broadcast messages received by the `node` persistent cache. | messages per second 
PersistentCache.NodeCache.node.CACHE_PUT | The number of entries put into the `node` persistent cache. | entries per second
PersistentCache.NodeCache.node.HITS | The number of hits for the `node` persistent cache. | hits per second
PersistentCache.NodeCache.node.INVALIDATE_ALL | The number of invalidate all operations on the `node` persistent cache. | events per second
PersistentCache.NodeCache.node.INVALIDATE_ONE | The number of times an entry from the `node` persistent cache was explicitly invalidated. | entries per second
PersistentCache.NodeCache.node.LOAD_EXCEPTIONS | The number of times a loader threw an exception on a cache miss for the `node` persistent cache. | exceptions per second
PersistentCache.NodeCache.node.PUT_REJECTED_SECONDARY_CACHE | The number of rejected put operations on the `node` persistent cache because the configuration excludes the key. | rejects per second
PersistentCache.NodeCache.node.REQUESTS | The number of lookup requests made on the `node` persistent cache. This does not include lookup operations for keys that have been excluded by configuration. | requests per second
PersistentCache.NodeCache.prev_document.BROADCAST_RECV | The number of broadcast messages received by the `prev_document` persistent cache. | messages per second 
PersistentCache.NodeCache.prev_document.CACHE_PUT | The number of entries put into the `prev_document` persistent cache. | entries per second
PersistentCache.NodeCache.prev_document.HITS | The number of hits for the `prev_document` persistent cache. | hits per second
PersistentCache.NodeCache.prev_document.INVALIDATE_ALL | The number of invalidate all operations on the `prev_document` persistent cache. | events per second
PersistentCache.NodeCache.prev_document.INVALIDATE_ONE | The number of times an entry from the `prev_document` persistent cache was explicitly invalidated. | entries per second
PersistentCache.NodeCache.prev_document.LOAD_EXCEPTIONS | The number of times a loader threw an exception on a cache miss for the `prev_document` persistent cache. | exceptions per second
PersistentCache.NodeCache.prev_document.PUT_REJECTED_SECONDARY_CACHE | The number of rejected put operations on the `prev_document` persistent cache because the configuration excludes the key. | rejects per second
PersistentCache.NodeCache.prev_document.REQUESTS | The number of lookup requests made on the `prev_document` persistent cache. This does not include lookup operations for keys that have been excluded by configuration. | requests per second

> Note: the broadcast feature of the persistent cache is disabled by default,
which means the number of received broadcast messages will be zero in that case.

## Revision GC

Basic Revision GC can be monitored with the following Counters:

Name | Description
-----|------------
RevisionGC.COUNTER | The number of Revision GC cycles performed by this `DocumentNodeStore`. In a cluster only one of the `DocumentNodeStore` instances will perform Revision GC and show an increasing value for this counter.
RevisionGC.FAILURE | The number of times the Revision GC failed.

More detailed insight is provided by these Meters, each exposing a cumulative
count and various rates for the event (mean, one minute, five minute, fifteen
minute):

Name | Description
-----|------------
RevisionGC.DELETE_DOC | The total number of documents deleted by the Revision GC on this `DocumentNodeStore`. This include all other types of documents deleted by the Revision GC.
RevisionGC.DELETE_INT_SPLIT_DOC | The number of intermediate split documents deleted by the Revision GC on this `DocumentNodeStore`.
RevisionGC.DELETE_LEAF_DOC | The number of leaf documents deleted by the Revision GC on this `DocumentNodeStore`.
RevisionGC.DELETE_SPLIT_DOC | The number of split/previous documents deleted by the Revision GC on this `DocumentNodeStore`.
RevisionGC.READ_DOC | The number of documents read by the Revision GC on this `DocumentNodeStore`.
RevisionGC.RESET_DELETED_FLAG | The number of documents where the Revision GC reset the `_deletedOnce` flag.

The following Timers are available for the Revision GC, with different
percentiles for the duration of an operation. The timings include the latency
between Oak and a backend store like MongoDB.

Name | Description
-----|------------
RevisionGC.ACTIVE_TIMER | The total time the Revision GC was active in one cycle.
RevisionGC.CHECK_DELETED_TIMER | The time it takes to check documents whether they can be deleted by the Revision GC in one cycle.
RevisionGC.DELETE_DOC_TIMER | The time it takes to delete documents in one Revision GC cycle. This includes all types of documents to delete.
RevisionGC.DELETE_SPLIT_DOC_TIMER | The time it takes to collect and delete split/previous documents in one Revision GC cycle.
RevisionGC.READ_DOC_TIMER | The time it takes to read candidate documents from the `DocumentStore` in one Revision GC cycle.
RevisionGC.RESET_DELETED_FLAG_TIMER | The time it takes to reset the `_deletedOnce` flag on documents in on Revision GC cycle.
RevisionGC.SORT_IDS_TIMER | The time it takes to sort `_id` values of documents to delete in one Revision GC cycle.

> Note: Oak tracks the duration for above timers at microsecond accuracy, but
many tools will translate the duration percentiles to milliseconds.

## DocumentStore

Every `DocumentStore` exposes the following Meters with a cumulative count and
various rates for each of the operations (mean, one minute, five minute, fifteen
minute):

Name | Description | Rate unit
-----|-------------|----------
DOCUMENT_JOURNAL_CREATE | The number of documents created in the `journal` collection by this `DocumentStore`. | documents per second
DOCUMENT_JOURNAL_QUERY | The number of documents read from the `journal` collection by this `DocumentStore`. | documents per second
DOCUMENT_NODES_CREATE | The number of documents created in the `nodes` collection by this `DocumentStore`. | documents per second 
DOCUMENT_NODES_CREATE_SPLIT | The number of split/previous documents created in the `nodes` collection by this `DocumentStore`. | documents per second
DOCUMENT_NODES_CREATE_UPSERT | The number of documents upserted in the `nodes` collection by this `DocumentStore`. | documents per second
DOCUMENT_NODES_FIND_CACHED | The number of documents from the `nodes` collection served from the cache. | documents per second
DOCUMENT_NODES_FIND_MISSING | The number of document reads from the `nodes` collection that did not return a document. | reads per second
DOCUMENT_NODES_FIND_PRIMARY | The number of document reads from the `nodes` collection served by the primary. | reads per second
DOCUMENT_NODES_FIND_SLAVE | The number of document reads from the `nodes` collection served by the slave/secondary. | reads per second
DOCUMENT_NODES_FIND_SPLIT | The number of split/previous document reads from the `nodes` collection. | reads per second
DOCUMENT_NODES_QUERY_FIND | The number of documents returned by queries on the `nodes` collection. | documents per second
DOCUMENT_NODES_QUERY_LOCK | The number of times queries on the `nodes` collection had to acquire a lock in the `DocumentStore`. | locks per second
DOCUMENT_NODES_QUERY_PRIMARY | The number of queries on the `nodes` collection targeting the primary. | queries per second
DOCUMENT_NODES_QUERY_SLAVE | The number of queries on the `nodes` collection targeting the slave/secondary. | queries per second
DOCUMENT_NODES_REMOVE | The number of documents removed from the `nodes` collection by this `DocumentStore`. | documents per second
DOCUMENT_NODES_UPDATE | The number of documents updated in the `nodes` collection by this `DocumentStore`. | documents per second
DOCUMENT_NODES_UPDATE_FAILURE | The number of failed updates of a document in the `nodes` collection by this `DocumentStore`. | failures per second
DOCUMENT_NODES_UPDATE_RETRY | The number of times an update of a document in the `nodes` collection was retried by this `DocumentStore`. | retries per second

> Note: the terms _primary_ and _slave_ are only relevant for a `DocumentStore`
implementation that works with replicated data, like MongoDB. Implementations
that do not make this distinction will report all calls for _primary_.

> Note: update failures and retries are only relevant for the RDB DocumentStore.
On a MongoDB DocumentStore, these values will always be zero.

In addition to above Meters, a `DocumentStore` also exposes the following
Timers with different percentiles for the duration of an operation. The timings
include the latency between Oak and a backend store like MongoDB.

Name | Description
-----|------------
DOCUMENT_JOURNAL_CREATE_TIMER | The time it takes to create an entry in the `journal` collection.
DOCUMENT_JOURNAL_QUERY_TIMER | The time it takes to perform a query on the `journal` collection.
DOCUMENT_NODES_CREATE_TIMER | The time it takes to create a document in the `nodes` collection.
DOCUMENT_NODES_CREATE_UPSERT_TIMER | The time it takes to upsert a document in the `nodes` collection.
DOCUMENT_NODES_FIND | The time it takes to lookup a document in the `nodes` collection. This does not include lookups for documents that do not exist.
DOCUMENT_NODES_FIND_MISSING_TIMER | The time it takes to lookup a document in the `nodes` collection that does not exist.
DOCUMENT_NODES_QUERY | The time it takes to perform a query on the `nodes` collection without additional filters. These are queries that only put a constraint on the `_id` field.
DOCUMENT_NODES_QUERY_FILTER | The time it takes to perform a query on the `nodes` collection with additional filters. Additional filters could e.g. include a constraint on the `_modified` field.
DOCUMENT_NODES_QUERY_LOCK_TIMER | The time a query spent in the `DocumentStore` waiting on a lock before it is executed.
DOCUMENT_NODES_REMOVE_TIMER | The time it takes to remove a document from the `nodes` collection.
DOCUMENT_NODES_UPDATE_TIMER | The time it takes to update a document in the `nodes` collection.

> Note: Oak tracks the duration for above timers at nanosecond accuracy, but
many tools will translate the duration percentiles to milliseconds.

## MongoDocumentStore

In addition to the general metrics of a `DocumentStore`, a `MongoDocumentStore`
also exposes the following Counters:

Name | Description | Unit
-----|-------------|-----
MongoDB.blobs.count | The number of documents in the `blobs` collection. | |
MongoDB.blobs.size | The total size of the documents in the `blobs` collection without indexes. | bytes |
MongoDB.blobs.storageSize | The total amount of storage allocated to the `blobs` collection. This value may be smaller than `size` because MongoDB uses compression when it stores documents on disk. | bytes |
MongoDB.blobs.totalIndexSize | The total size of all indexes in the `blobs` collection. | bytes |
MongoDB.clusterNodes.count | The number of documents in the `clusterNodes` collection. | | 
MongoDB.clusterNodes.size | The total size of the documents in the `clusterNodes` collection without indexes. | bytes |
MongoDB.clusterNodes.storageSize | The total amount of storage allocated to the `clusterNodes` collection. This value may be smaller than `size` because MongoDB uses compression when it stores documents on disk. | bytes |
MongoDB.clusterNodes.totalIndexSize | The total size of all indexes in the `clusterNodes` collection. | bytes |
MongoDB.journal.count | The number of documents in the `journal` collection. | |
MongoDB.journal.size | The total size of the documents in the `journal` collection without indexes. | bytes |
MongoDB.journal.storageSize | The total amount of storage allocated to the `journal` collection. This value may be smaller than `size` because MongoDB uses compression when it stores documents on disk. | bytes |
MongoDB.journal.totalIndexSize | The total size of all indexes in the `journal` collection. | bytes |
MongoDB.nodes.count | The number of documents in the `nodes` collection. | |
MongoDB.nodes.size | The total size of the documents in the `nodes` collection without indexes. | bytes |
MongoDB.nodes.storageSize | The total amount of storage allocated to the `nodes` collection. This value may be smaller than `size` because MongoDB uses compression when it stores documents on disk. | bytes |
MongoDB.nodes.totalIndexSize | The total size of all indexes in the `nodes` collection. | bytes |
MongoDB.settings.count | The number of documents in the `settings` collection. | |
MongoDB.settings.size | The total size of the documents in the `settings` collection without indexes. | bytes |
MongoDB.settings.storageSize | The total amount of storage allocated to the `settings` collection. This value may be smaller than `size` because MongoDB uses compression when it stores documents on disk. | bytes |
MongoDB.settings.totalIndexSize | The total size of all indexes in the `settings` collection. | bytes |

## Monitoring

While not all metrics described on this page are necessary to monitor a
`DocumentNodeStore`, it is still advised to collect data for all metrics. In
case of a problem, metrics can be a valuable source of historical data to
analyze the problem and gain insight how the system behaved over time.

Some essential metrics that should be monitored are:

- Background read and write operations. These operations are scheduled to run
every second. If this interval gets too long, changes from other cluster nodes
will get picked up with a delay, which can result in failed merges because of
conflicts. As a general rule of thumb, background operations that take longer
than ten seconds should be analyzed. Possible reasons for an unusually long
background operation may be increased load on the system with many changes to
write to or read from the `DocumentStore`, general slow down of the JVM because
of increased Java GC activity or an overloaded backend store.
- Background read lag rate. This is similar to the schedule of the background
read and write operations. The one minute rate is usually below 1000
milliseconds and should be analyzed when this rate is higher than ten seconds.
- Lease update rate and duration. The `DocumentNodeStore` updates the lease
roughly every 10 seconds. A lease update is a lightweight operation and should
usually complete quickly. An increased update time may indicate a network
problem or an overloaded backend store. A system should be analyzed when the
lease update rate drops below 3 updates per minute and requires immediate
attention when the rate is at one update per minute or lower. Please note,
the `DocumentNodeStore` will shut itself down when the lease expires (the
default lease time is two minutes).
- Revision GC rate and duration. Starting with Oak 1.8, Revision GC on MongoDB
runs every five seconds. This is also known as Continuous Revision GC. This rate
and the duration of a Revision GC cycle should be monitored and analyzed if
the duration is more than five seconds for a couple of cycles.
- Revision GC failure. These should not occur and require immediate attention.
The log files should provide details in this case why the Revision GC failed.
- Number of merges under an exclusive lock or that were suspended. Periods of
increased merges with exclusive locks or suspended merges indicate conflicts
usually caused by the application when multiple sessions try to write
conflicting changes. The application should be analyzed to prevent such write
patterns.
