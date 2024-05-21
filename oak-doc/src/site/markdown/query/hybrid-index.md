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

## Hybrid Index

* [New in 1.6](#new-1.6)
* [Synchronous Index Usecases](#synchronous-index-usecases)
    * [Unique Indexes](#unique-indexes)
    * [Property Indexes](#property-indexes)
* [Drawbacks of current property indexes](#drawbacks-of-current-property-indexes)
* [Proposal](#proposal)
    * [Index Definition](#index-definition)
    * [Index Storage](#index-storage)
        * [Unique Indexes](#unique-indexes-definition)
        * [Property Indexes](#property-indexes-definition)
    * [Index Pruner](#index-pruner)
* [Query Evaluation](#query-evaluation)

### <a name="new-1.6"></a>New in 1.6

In Oak 1.6 ([OAK-4412](#OAK-4412)), we add support for near real time (NRT) and limited support for
sync indexes. In [OAK-6535](#OAK-6535), we add support for sync property indexes. See
also [Oakathon August 2017 PresentationHybrid Index v2.pdf](#hybrid-index-v2.pdf)

### <a name="synchronous-index-usecases"></a>Synchronous Index Usecases

Synchronous indexes are required in the following use-cases:

#### <a name="unique-indexes"></a>Unique Indexes

For unique indexes like the uuid index, and the principal name index, we need to be ensured that an
indexed value is unique across the whole repository on commit. If the indexed value already exists,
e.g. principal with same name already exist, then the commit should fail. For this, we need a
synchronous index, which get updated as part of commit itself.

#### <a name="property-indexes"></a>Property Indexes

Depending on application requirements the query results may be:

* Eventually Consistent - Any changes eventually gets reflected in query results.
* Consistent - Any change immediately gets reflected in query results.

For most cases, for example user-driven search, eventual consistent search result work fine, and
hence async indexes can be used. With NRT indexes (OAK-4412), changes done by user get reflected
very soon in search results.

However, for some cases we need to support fully consistent search results. Assume there is
component which maintains a cache for nodes of type app:Component, and uses a observation listener
to listen for changes in nodes of type app:Component, and upon finding any changes, it rebuilds the
cache by queriying for all such nodes. For this cache to be correct, it needs to be ensured query
results are consistent with the session associated with the listener. Otherwise it may miss a new
component, and a later request to the cache for such component would fail.

For such use-cases, it's required that indexes are synchronous and results provided by index are
consistent.

### <a name="drawbacks-of-current-property-indexes"></a>Drawbacks of current property indexes

Oak has support for synchronous property indexes, which are used to meet above use-cases. However
the current implementation has certain drawbacks:

* Slow reads over remote storage - The property indexes are stores as normal NodeState and hence
  reading them over remote storage like Mongo performs poorly (with Prefetch, OAK-9780, this is
  improved).
* Storage overhead - The final storage overhead is larger, specially for remote storage where each
  NodeState is mapped to 1 document. (On the other hand, temporary disk usage for Lucene indexes
  might be higher than for node stores, due to write amplification with Lucene.)

 ---

### <a name="proposal"></a>Proposal

To overcome the drawbacks and still meet the synchronous index requirements, we can implement a
hybrid index where the indexes content is stored using both property index (for recent entries) and
Lucene indexes (for older entries):

* Store recently added index content as a normal property index.
* As part of async indexer, store the content in the Lucene index.
* Later prune the property index content (parts that have been indexed in Lucene).
* Any query is a union of query results from both property index and Lucene indexes (with some
  caveats).

#### <a name="index-definition"></a>Index Definition

The synchronous index support needs to be enabled via index definition:

* Set sync to true for each property definition which needs to be indexed in a sync way

```
/oak:index/assetType
- jcr:primaryType = "oak:QueryIndexDefinition"
- type = "lucene"
- async = ["async"]
+ indexRules
    + nt:base
        + properties
            + resourceType
                - propertyIndex = true
                - name = "assetType"
                - sync = true
```

For unique indexes set unique i.e. true:

```
/oak:index/uuid
- jcr:primaryType = "oak:QueryIndexDefinition"
- type = "lucene"
- async = ["async"]
+ indexRules
    + nt:base
        + properties
            + uuid
                - propertyIndex = true
                - name = "jcr:uuid"
                - unique = true
```

### <a name="index-storage"></a>Index Storage

The property index content is stored as hidden nodes under the index definition nodes. The storage
structure is similar to property indexes with some changes.

#### <a name="unique-indexes-definition"></a>Unique Indexes

```
/oak:index/assetType
+ :data   //Stores the lucene index files
+ :property-index
    + uuid
        + <value 1>
            - entry = [/indexed-content-path]
            - jcr:created = 1502274302 //creation time in millis
        + 49652b7e-becd-4534-b104-f867d14c7b6c
            - entry = [/jcr:system/jcr:versionStorage/63/36/f8/6336f8f5-f155-4cbc-89a4-a87e2f798260/jcr:rootVersion]
```

Here:

* :property-index - hidden node under which property indexes is stored for various properties which
  are marked as sync.
* For unique indexes, each entry also has a timestamp which is later used for pruning.

#### <a name="property-indexes-definition"></a>Property Indexes

```
/oak:index/assetType
+ :data   //Stores the lucene index files
+ :property-index
    + resourceType
        - head = 2
        - previous = 1
        + 1
            - jcr:created = 1502274302 //creation time in millis
            - lastUpdated = 1502284302
            + type1
                + libs
                    + login
                        + core
                            - match = true
            + <value>
              + <mirror of indexed path>
        + 2
            - jcr:created = 1502454302
            + type1
                + ...
```

Here we create new buckets of index values which simplifies pruning. New buckets get created after
each successful async indexer run, and older buckets get removed. The bucket have a structure
similar to tje content mirror store strategy.

For each indexed property, we keep a head property which refers to the current active bucket. This
is changed by IndexPruner. In addition, there is a previous bucket to refer to the last active
bucket.

On each run of IndexPruner:

* Check if IndexStatsMBean#LastIndexedTime is changed from last known time.
* If changed then:
    * Create a new bucket by incrementing the current head value.
    * Set previous to current head.
    * Set head to new head value.
    * Set lastUpdated on previous bucket to now.
* Remove all other buckets.

We require both head and previous bucket as there is some overlap between content in previous.

#### <a name="index-pruner"></a>Index Pruner

Index Pruner is a periodic task prunes the index content. It uses the
IndexStatsMBean#LastIndexedTime to determine upto which time the async indexer has indexed the
repository, and then removes entries from the property index which are older than that time.

* Property index - here pruning is done by creating a new bucket and then removing the older bucket.
* Unique index - Here prunining is done by iterating over current indexed content and removing the
  older ones.

### <a name="query-evaluation"></a>Query Evaluation

On the query side, we perform a union query over the 2 index types: A union cursor is created which
consist of:

* LucenePathCursor - Primary cursor backed by Lucene index.
* PropertyIndexCursor - A union of path cursor from current head and previous bucket.

#### Open Points

If there are multiple property definition in a Lucene index marked with sync and a query involves
constraints on more than 1, then which property index should be picked is not clear.

## Attachments:

[Hybrid Index v2.pdf (application/pdf)](#hybrid-index-v2.pdf)

[OAK-4412]: https://issues.apache.org/jira/browse/OAK-4412

[hybrid-index-v2.pdf]:https://jackrabbit.apache.org/archive/wiki/JCR/attachments/115513516/115513517.pdf
