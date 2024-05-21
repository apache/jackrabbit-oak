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
This is a brief outline of the history of Online Revision Garbage Collection in Oak. By linking to
further details where necessary this historical context helps making sense of the various bits of
information that are scattered across Jira Issues, Wikis, source code etc.

Refer to [Oak Segment Tar](http://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html) on
the Jackrabbit Oak Wiki for a general overview of the segment store, its design, data structures and
inner workings.


Background
----------
Online Revision Cleanup (OnRC) refers to a technique employed by the segment store to reclaim disk
space that is no longer in use. The implementation is structured in three phases:

* Estimation: a heuristic to determine whether enough garbage has accumulated to warrant running
  garbage collection at all.

* Compaction: all records of the segment store's current head state are rewritten into a new,
  structurally equal head state. The records of the rewritten head state are compact within their
  segment as rewriting skips all records that are not reachable from the root node state.

* Cleanup: reclaimable segments are removed. Reclaimability is determined either by reachability
  through the segment graph or by the age of the segment depending on the version of Oak.

Oak 1.0 - 1.4
-------------
Online Revision Garbage Collection did not work up to and including Oak 1.4 as it was not able to
collect any garbage. In these version OnRC relied upon false premises on one hand and was further
impacted by a bug [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348) on the other hand.

The [compaction](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L1045)
phase uses an instance of
the [`Compactor`](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/Compactor.java#L54)
class
for [rewriting](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L1061)
the current head state of the repository. The `Compactor` itself works
by [comparing](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/Compactor.java#L160)
a `before` state to an `after` state applying the differences to an `onto` state. In an initial pass
the current head state of the repository is passed for the `after` state and
an [empty node](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/memory/EmptyNodeState.java#L37)
is passed for both `before` and `onto`. Once the initial phase completes an attempt is made
to [set](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L1075)
the repository's head state to the resulting node state via an atomic compare and set operation.
This fails in the case when concurrent write operations to the repository changed its head state in
the meanwhile. In that case a retry loop is entered where these additional changes
are [compacted](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L1082)
on top of the previously compacted head state. After a configurable (default 5) numbers of retires a
final attempt is made
to [force compact](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L1099)
any remaining changes while blocking writes to the repository for a configurable (default 1 minute)
time. Only if force compacting also fails is the compaction considered failed.

Once compaction finished, either successfully or
not, [`cleanup`](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L869)
tries to remove content that is not reachable any more. However, the record graph of a repository
grows very large very quickly. To avoid traversing large record graphs, the cleanup phase operates
on the segment graph induced by the record graph. That is, for any two segments `s1` and `s2`, there
is an edge from `s1` to `s2` if and only if `s1` contains a record that references a record in `s2`.
By construction the segment graph contains far less vertices than the record graph. To speed up
traversal it is pre-calculated and cached in the segment headers and in an graph entry of each tar
file. While the segment graph is sufficiently small for efficient traversal, it is also extremely
dense. In fact it turned out that its reflexive, transitive closure is the entire graph most of the
time. The reason for this can be seen when looking at an example where a segment contains just a
single reachable record and `n` unreachable records. In this case the single reachable record makes
the segment reachable preventing it from being reclaimed along with the `n` non reachable records.
To make matters worse, all segments referenced from this segment will also stay in the reachable
set, although the single reachable record might not have any outgoing references at all.

Traversing the segment graph starts with a set
of [root segments](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L887).
These are all the segments that are currently references from the JVM (i.e. ultimately in use by
some code either within Oak or within its client). From there the set of all referenced segments is
determined for
each [`TarReader`](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/TarReader.java#L58)
and each of
them [cleaned up](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/TarReader.java#L752)
individually. That is a new tar file containing only the still referenced segments is generated
unless [space saving is not worth it](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/TarReader.java#L789).

[OAK-1828](https://issues.apache.org/jira/browse/OAK-1828) complicated the process for determining
the segment graph as the improvements done with this issue inadvertently introduced the potential
for cycles in the segment graph. The idea was to introduce
a [pool of `SegmentBufferWriter`](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/SegmentWriter.java#L796)
instances to avoid contention on a single instance when concurrently writing to the segment store.
However, depending on how writes are interleaved and scheduled to `SegmentBufferWriter` instances
from the pool this opened up the possibility
for [cycles in the segment graph](https://issues.apache.org/jira/browse/OAK-3864), which `cleanup`
needs to take
into [consideration](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/file/FileStore.java#L900)
when determining the the segment graph.

Even though online compaction rewrites all records of the current head state into a new and compact
representation of the repository's head in a separate set of segments, the presence of open sessions
referencing older revisions for a while prevents those from being reclaimed. In
addition [OAK-3348](https://issues.apache.org/jira/browse/OAK-3348) could cause some of the segments
of the compacted revision to still reference segments from the pre-compacted revision. Together with
the segment graph being very dense this prevented almost any segment from being reclaimed in Oak 1.4
and earlier.

See also the annotated
slides [Into the tar pit: a TarMK deep dive](https://adapt.to/2016/en/schedule/into-the-tar-pit--a-tarmk-deep-dive.html)
for further illustrations on this topic.


Oak 1.6
-------
Oak 1.6 was the first version with workable OnRC overcoming the problems with with open sessions
keeping references to previous revisions and
fixing [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348). This required changes in the
persistence format forcing existing customers
to [migrate](https://helpx.adobe.com/experience-manager/6-3/sites/deploying/using/revision-cleanup.html#OnlineRevisionCleanupFrequentlyAskedQuestions)
their deployments.

### Generational garbage collection

Oak 1.6 changed the mechanism used to determine reclaimability of segments. Previous versions used
reachability through the segment graph starting from a set of GC roots consisting of the segment
containing the current head node state and all segments containing records currently referenced by
the JVM (i.e. by open sessions).

Oak 1.6 introduced the concept of
a [GC generation](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Segment.java#L380).
A GC generation is an integer starting at 0 and increasing with each run of OnRC. Each segment
records the current GC generation from the time the segment was created in
its [segment header](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentBufferWriter.java#L200-L203).
The current GC generation of the repository is just the GC generation of the segment containing the
current head state. The
compactor [reads](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L845)
the current GC generation of the repository
and [rewrites](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L864)
the head state using the next GC generation number for the segments created in the process. Once the
compactor finished rewriting the current head state the newly created, compact head state
is [atomically set](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L876)
as the new head state of the repository, implicitly and atomically increasing the GC generation of
the repository at the same time.

In its default configuration
the [cleanup](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L1055)
phase retains all segments from the current GC generation and the previous one reclaiming all older
segments. With the default daily OnRC execution this results in a minimal segment retention time of
24 hours. Sessions that are open at the point in time where OnRC runs will
automatically [refresh](https://issues.apache.org/jira/browse/OAK-2407) at next access to reduce the
risk for them to reference content from segments that were reclaimed.

Since [bulk segments](http://jackrabbit.apache.org/oak/docs/nodestore/segment/records.html#Bulk_segments)
do not have a segment header and thus cannot record their GC generation, the cleanup phase still
uses [reachability](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/TarReader.java#L754)
through the segment graph to determine whether a bulk segment is reclaimable. That is, a bulk
segment is reclaimable if and only if it is not reachable through the segment graph of the non
reclaimable data segments starting from an initial set
of [root segment ids](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L1076).

### Preventing references between segments with different GC generations

The generation based garbage collection approach disallows references between segments from
different GC generations as otherwise reclaiming an older generation would render a newer one
incomplete potentially
causing [`SegmentNotFoundException`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentNotFoundException.java#L27)
s subsequently. Unfortunately up to Oak 1.4 references between segments of different GC generations
could be introduced by sessions that were acquired before an OnRC cycle completed. Such sessions
would reference records in segments of the previous GC generations through their base state. When
such a session subsequently saves its changes are written to segments of the new GC generation
effectively creating references from this GC generation to the previous one.
See [(OAK-3348)](https://issues.apache.org/jira/browse/OAK-3348) for the full story.

To prevent reference between segments of different GC generations such references need to be
detected and the affected records rewritten into the current GC generation. That is, whenever a node
state is written by
a [`SegmentWriter`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L85)
all references to existing records
are [checked](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1188)
and the records are rewritten if they do not refer to the current generation. Rewriting is
potentially expensive as a base state of
a [`NodeBuilder`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/state/NodeBuilder.java)
might cover (a previous revision) of the whole repository. Expensive both in terms of CPU and IO
cycles and in term of extra disk space to hold the rewritten base state. However when this situation
occurs most records of that base state have likely already been rewritten by OnRC: the most recent
compacted head state was rewritten by OnRC from a node state of a more recent revision than our base
state, both of which are likely sharing many records. To avoid rewriting the same records multiple
times the `SegmentWriter`
employs [deduplication caches](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L128)
for [node records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L108), [string records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L94)
and [template records](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/WriterCacheManager.java#L101).

The deduplication caches are indexed by the GC generation such that records can
be [evicted](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L441)
from the caches by generation. Such an eviction happens
for [older generations](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L448)
that are not needed any more
after [OnRC succeeded](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L935).
An eviction happens for
the [generation created](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStoreBuilder.java#L457)
by OnRC in the case of
a [failure](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L834).

For string records and template records
the [deduplication caches](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordCache.java)
are ultimately backed by
a [hash map](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordCache.java#L171-L181)
with an LRU eviction policy. For node records the situation is more complicated as determining
structural equality for those means traversal of the potentially large subtree rooted at the record.
Also relying on
the [`RecordId`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/RecordId.java)
does not work as equality of `RecordId`s in only a necessary condition for structural equality of
node records but not a sufficient one. That is, when a node record is rewritten during compaction
its clone will be structurally equal to the original but yet have a different `RecordId`. To
overcome this problem
a [stable id](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentNodeState.java#L119)
that would not change when rewriting a node state was introduced. This stable id is used by
the `SegmentWriter`
as [cache key](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1157)
when deduplicating node records. Finally the node deduplication cache is backed by the
custom [PriorityCache](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/PriorityCache.java#L60)
implementation. This cache uses efficient rehashing into an array to resolve hash clashes while at
the same time using
a [cost measure](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/PriorityCache.java#L200-L230)
to avoid evicting expensive items.
The [cost](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L1004)
of a node increases with its number of child nodes, which increases the chance for such nodes to
stay in the cache.

To put everything together OnRC in Oak 1.6 uses a different approach
for [compaction](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L983)
than OffRC. While the latter uses
the  [`Compactor`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java)
class like in previous versions of Oak, the former passes the current head state
to [`SegmentWriter.writeNode`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L343)
for on a `SegmentWriter` for the next GC generation. This causes the head state to be rewritten into
segments of the next GC generation filling the deduplication caches in the process.


Oak 1.8
-------
Oak 1.8 introduced two main improvements on top of the GC generation based garbage collection
approach from Oak 1.6: sequential rebasing of checkpoints on top of each other and tail compaction,
a lighter variant of compaction roughly comparable to the JVM's young generation garbage collection.

### Sequential checkpoint rebasing

The segment store implements checkpoints as links to (previous) root node states from
a [child node](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/scheduler/LockBasedScheduler.java#L383)
under the `checkpoints` node of the super root. In Oak 1.6 compaction was not concerned with
checkpoints but rather treated them as regular nodes solely relying on
the [node deduplication cache](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/DefaultSegmentWriter.java#L967)
to prevent them from being exploded. This approach did not scale well and could lead
to [high IO rates](https://issues.apache.org/jira/browse/OAK-6984).

Oak 1.8 improved this aspect by considering checkpoints during compaction instead of relying on the
node deduplication cache. This is done by using
a [`Compactor`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L56)
to sequentially
rebase [checkpoints and the repository root](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/CheckpointCompactor.java#L119)
on [top of each other](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/CheckpointCompactor.java#L160)
and subsequently reassembling them into the
right [super-root structure](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/CheckpointCompactor.java#L128-L138).
As an additional optimisation already compacted checkpoints
are [cached](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/CheckpointCompactor.java#L249)
to prevent them from being passed to the compactor again in the common case where there are no
changes between checkpoints.

### Tail compaction

Full compaction of the whole repository is the most effective way to remove any accumulated garbage.
To achieve this, full compaction rewrites all content regardless whether there have been updates
causing fragmentation or not. As a result this approach is very resource intensive (mainly wrt. IO)
and can take a lot of time to complete. Oak 1.8 introduced another compaction mode
termed [tail compaction](https://issues.apache.org/jira/browse/OAK-3349). In contrast to full
compaction, tail compaction does not compact the whole repository but only those revisions that have
been updated since the last compaction.

Tail compaction is implemented by compacting the current head state of the repository on top of
the [previously compacted head state](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L730).
The range of revisions starting from the previously compacted head state up to the current head
state covers all updates since the last compaction, effectively making tail compaction cover only
those revisions that have been updated since the last compaction. Tail compaction can also be
regarded as a generalisation of full compaction. With the latter the current head state
is [compacted on top of an empty node state](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L723).
With the former the current head state
is [compacted on top of a previously compacted head state](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/FileStore.java#L730).

Since tail compaction relies on the previously compacted head state the subsequent cleanup phase
needs to take this into consideration. That is, cleanup needs to be aware of what type of compaction
created a certain segment to determine its reclaimability. This required generalising the GC
generation from a simple integer into
a [`GCGeneration` class](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/tar/GCGeneration.java#L52)
that captures the concept of full and tail compaction. Instances of this class represent the garbage
collection generation related information of a segment. It consists of the segment's generation, its
full generation and its compaction flag. The segment's generation records the number of garbage
collection cycles a segment went through and is incremented with every garbage collection regardless
of its type. The segment's full generation records the number of full garbage collection cycles a
segment went through. It is only incremented on full garbage collection cycles. The segment's
compaction flag is set for those segments that have been created by a compaction operation. It is
never set for segments created by normal write operations. Segments written by normal repository
writes will inherit the generation and full generation of the segment written by the previous
compaction process with the compacted flag cleared.

The information recorded in this way allows to determine the reclaimability status of a segment by
just looking at the `GCGeneration` instances of that segment and of the segment containing the
repository head: Let `S` be a segment, `H` be the segment containing the current repository head
and `n` be the number of retained generations:

* `S` is old if and only if `H.generation - S.generation >= n`
* `S` is in the same compaction tail like `H` if and only
  if `S.isCompacted && S.fullGeneration == H.fullGeneration`
* `S` is reclaimable if and only if `S` is old and `S` is not in the same compaction tail like `H`.

This logic is captured in the respective implementations of
the [reclaim predicate](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/Reclaimers.java#L61)
in
the [`Reclaimers` class](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/Reclaimers.java#L33).

> *Note:* Oak 1.8.0 had a [bug](https://issues.apache.org/jira/browse/OAK-7132) in the
> implementation of the reclamation mechanism described. The bug was fixed with Oak 1.8.1, which is
> the version of Oak this section is referring to.

### The Compactor strikes back

Oak 1.6 removed
the [`Compactor` class](https://github.com/apache/jackrabbit-oak/blob/1.4/oak-segment/src/main/java/org/apache/jackrabbit/oak/plugins/segment/Compactor.java#L54)
in favour of directly rewriting node states with
the [`SegmentWriter`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.6.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L85)
solely relying on deduplication caches for deduplicating records. To implement sequential checkpoint
rebasing and tail compaction Oak 1.8 reintroduced a new implementation of
a [`Compactor` class](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L56).
This implementation has to deal with two additional requirements compared to the previous
implementation: tracking and assigning stable ids and being able to cope with a large number of
direct child nodes of a node. This is done by tracking changes with
a [`MemoryNodeBuilder`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L163)
instead of using a `NodeBuilder` acquired through calling `NodeState.builder`. The
new [`SegmentWriter.write`](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/SegmentWriter.java#L130)
method with an extra argument for the stable is then used
to [write](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L175)
compacted node states including their stable id. In addition the number of updates to
the `MemoryNodeBuilder`
are [tracked](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L154-L160)
and
an [intermediate node is written](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L156)
to avoid keeping to many updates in memory once
an [update limit](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L62)
is exceeded. Further updates are tracked in a fresh `MemoryNodeBuilder` instance that uses
this [intermediate node as its base](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.8.1/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/Compactor.java#L157).


Oak 1.10
--------
With Oak 1.6 and Oak 1.8 it was observed that running compaction first increases the repository
until cleanup runs and subsequently removes the generation that has become reclaimable. Oak 1.10
improved this aspect by running cleanup *before* compaction thus levelling out the bump in
repository size cause by the compaction phase.

The effort included a few refactorings making garbage collection more modular:

* [OAK-7377](https://issues.apache.org/jira/browse/OAK-7377) generalised
  the [garbage collector](https://github.com/apache/jackrabbit-oak/blob/jackrabbit-oak-1.10.0/oak-segment-tar/src/main/java/org/apache/jackrabbit/oak/segment/file/GarbageCollector.java#L46)
  allowing multiple implementations.
* [OAK-7440](https://issues.apache.org/jira/browse/OAK-7440), [OAK-7434](https://issues.apache.org/jira/browse/OAK-7434)
  and [OAK-7436](https://issues.apache.org/jira/browse/OAK-7436) factored estimation, compaction and
  cleanup into independent components.
* [OAK-7445](https://issues.apache.org/jira/browse/OAK-7445) introduced the new cleanup before
  compaction garbage collection strategy.
* [OAK-7550](https://issues.apache.org/jira/browse/OAK-7550) eventually set the cleanup before
  compaction strategy as the new default for Oak 1.10.

