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

Memoirs in Garbage Collection
=============================
This is a brief outline of the history of Online Revision Garbage Collection in Oak. By linking to further details where necessary this historical context helps making sense of the various bits of information that are scattered across Jira Issues, Wikis, source code etc. 

Refer to [Oak Segment Tar](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html) on the Jackrabbit Oak Wiki for a general overview of the segment store, its design, data structures and inner workings.


Background
----------
Online Revision Cleanup (OnRC) refers to a technique employed by the segment store to reclaim disk space that is no longer in use. The implementation is structured in three phases:

* Estimation: a heuristic to determine whether enough garbage has accumulated to warrant running garbage collection at all. 

* Compaction: all records of the segment store's current head state are rewritten into a new, structurally equal head state. The records of the rewritten head state are compact within their segment as rewriting skips all records that are not reachable from the root node state. 

* Cleanup: reclaimable segments are removed. Reclaimability is determined either by reachability through the segment graph or by the age of the segment depending on the version of Oak.


Oak 1.4
-------
Online Revision Garbage Collection did not work up to and including Oak 1.4 as it was not able to collect any garbage. In these version OnRC relied upon false premises on one hand and was further impacted by a bug [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348) on the other hand. 

The record graph of a repository grows very large very quickly. To avoid traversing large record graphs, the cleanup phase would instead operate on the segment graph induced by the record graph. That is, for any two segments s1 and s2, there would be an edge from s1 to s2 if and only if s1 contains a record that references a record in s2. By construction the segment graph contains far less vertices than the record graph. To speed up traversal it is pre-calculated and cached in the segment headers. While the segment graph is sufficiently small for efficient traversal, it is also extremely dense. In fact it turned out that its reflexive, transitive closure is the entire graph most of the time. The reason for this can be seen when looking at an example where a segment contains just a single reachable record and *n* unreachable records. In this case the single reachable record makes the segment reachable preventing it from being reclaimed along with the *n* non reachable records. To make matters worse, all segments referenced from this segment will also stay in the reachable set, although the single reachable record might not have any outgoing references at all. 

During online compaction all records of the current head state of the repository are rewritten to form a new and compact representation of the repository's head in a separate set of segments. However, the presence of open sessions referencing older revisions for a while prevents those from being reclaimed. 
In addition [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348) could cause some of the segments of the compacted revision to still reference segments from the pre-compacted revision. Together with the segment graph being very dense this prevented almost any garbage collection from happening in Oak 1.4 and earlier. 

See also the annotated slides [Into the tar pit: a TarMK deep dive](https://adapt.to/2016/en/schedule/into-the-tar-pit--a-tarmk-deep-dive.html) for further illustrations on this topic. 


Oak 1.6
-------
Oak 1.6 was the first version with workable OnRC overcoming the problems with with open sessions keeping references to previous revisions and fixing [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348). This required changes in the persistence format forcing existing customers to [migrate](https://helpx.adobe.com/experience-manager/6-3/sites/deploying/using/revision-cleanup.html#OnlineRevisionCleanupFrequentlyAskedQuestions) their deployments. 

### Generational garbage collection
Oak 1.6 changed the mechanism used to determine reclaimability of segments. Previous versions used reachability through the segment graph starting from a set of GC roots consisting of the segment containing the current head node state and all segments containing records currently referenced by the JVM (i.e. by open sessions). 

Oak 1.6 introduced the concept of a GC generation. GC generations are numbered starting at 0 and increasing with each run of OnRC. Each segment records the current GC generation from the time the segment was created in its segment header. The current GC generation of the repository is just the GC generation of the segment containing the current head state. The compactor reads the current GC generation of the repository and rewrites the head state using the next GC generation number for the segments created in the process. Once the compactor finished rewriting the current head state the newly created, compact head state is atomically set as the new head state of the repository, implicitly and atomically increasing the GC generation of the repository at the same time. 

In its default configuration the cleanup phase retains all segments from the current GC generation and the previous one reclaiming all older segments. With the default daily OnRC execution this results in a minimal segment retention time of 24 hours. Sessions that are open at the point in time where OnRC runs will automatically [refresh](https://issues.apache.org/jira/browse/OAK-2407) at next access to reduce the risk for them to reference content from segments that were reclaimed.

### Preventing references between segments with different GC generations
The generation based garbage collection approach disallows references between segments from different GC generations as otherwise reclaiming an older generation would render a newer one incomplete potentially causing [`SegmentNotFoundException`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentNotFoundException.java#L27)s subsequently. Unfortunately up to Oak 1.4 references between segments of different GC generations could be introduced by sessions that were acquired before an OnRC cycle completed. Such sessions would reference records in segments of the previous GC generations through their base state. When such a session subsequently saves its changes are written to segments of the new GC generation effectively creating references from this GC generation to the previous one. See [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348) for the full story. 

To prevent reference between segments of different GC generations such references need to be detected and the affected records rewritten into the current GC generation. That is, whenever a node state is written by a [`SegmentWriter`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L85) all references to existing records are [checked](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1188) and the records are rewritten if they do not refer to the current generation. Rewriting is potentially expensive as a base state of a [`NodeBuilder`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/state/NodeBuilder.java) might cover (a previous revision) of the whole repository. Expensive both in terms of CPU and IO cycles and in term of extra disk space to hold the rewritten base state. However when this situation occurs most records of that base state have likely already been rewritten by OnRC: the most recent compacted head state was rewritten by OnRC from a node state of a more recent revision than our base state, both of which are likely sharing many records. To avoid rewriting the same records multiple times the `SegmentWriter` employs [deduplication caches](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L128) for [node records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L108), [string records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L94) and [template records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L101).

The deduplication caches are indexed by the GC generation such that records can be [evicted](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L441) from the caches by generation. Such an eviction happens for [older generations](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L448) that are not needed any more after [OnRC succeeded](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L935). An eviction happens for the [generation created](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L457) by OnRC in the case of a [failure](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L834).

For string records and template records the [deduplication caches](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordCache.java) are ultimately backed by a [hash map](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordCache.java#L171-L181) with an LRU eviction policy. For node records the situation is more complicated as determining structural equality for those means traversal of the potentially large subtree rooted at the record. Also relying on the [`RecordId`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordId.java) does not work as equality of `RecordId`s in only a necessary condition for structural equality of node records but not a sufficient one. That is, when a node record is rewritten during compaction its clone will be structurally equal to the original but yet have a different `RecordId`. To overcome this problem a [stable id](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentNodeState.java#L119) that would not change when rewriting a node state was introduced. This stable id is used by the `SegmentWriter` as [cache key](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1157) when deduplicating node records. Finally the node deduplication cache is backed by the custom [PriorityCache](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/PriorityCache.java#L60) implementation. This cache uses efficient rehashing into an array to resolve hash clashes while at the same time using a [cost measure](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/PriorityCache.java#L200-L230) to avoid evicting expensive items. The [cost](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1004) of a node increases with its number of child nodes, which increases the chance for such nodes to stay in the cache.

To put everything together OnRC in Oak 1.6 uses a different approach for [compaction](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L983) than OffRC. While the latter uses the  [`Compactor`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java) class like in previous versions of Oak, the former passes the current head state to [`SegmentWriter.writeNode`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L343) for on a `SegmentWriter` for the next GC generation. This causes the head state to be rewritten into segments of the next GC generation filling the deduplication caches in the process.


Oak 1.8
-------
TODO

