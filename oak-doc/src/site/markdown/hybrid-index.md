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

Oak 1.6 added support for Lucene Hybrid Index ([OAK-4412](#OAK-4412)). That enables near real time (NRT) support for Lucene based indexes. It also had a limited support for sync indexes. This feature aims to improve that to next level and enable support for sync property indexes.

* JIRA Issue - [OAK-6535](#OAK-6535)
* [Oakathon August 2017 PresentationHybrid Index v2.pdf](#hybrid-index-v2.pdf)

### <a name="synchronous-index-usecases"></a>Synchronous Index Usecases

Synchronous indexes are required in following usecases

#### <a name="unique-indexes"></a>Unique Indexes
For unique indexes like uuid index, principal name index it needs to be ensured that indexed value is unique across whole of the repository at time of commit itself. If the indexed value already exists e.g. principal with same name already exist then that commit should fail. To meet this requirement we need synchronous index which get updated as part of commit itself.

#### <a name="property-indexes"></a>Property Indexes
Depending on application requirements the query results may be

* Eventually Consistent - Any changes done get eventually reflected in query results.
* Consistent - Any change done gets immediately reflected in query result

For most cases like user driven search eventual consistent search result work fine and hence async indexes can be used. With recent support for NRT indexes (OAK-4412) the user experience get better and changes done by user get reflected very soon in search result.

However for some cases we need to support fully consistent search results. For e.g. assume there is component which maintains a cache for nodes of type app:Component and uses a observation listener to listen for changes in nodes of type app:Component and upon finding any changes it rebuilds the cache by queriying for all such nodes. For this cache to be correct it needs to be ensured query results are consistent wrt session state associated with the listener. Otherwise it may miss on picking a new component and later request to cache for such component would fail.

For such usecases its required that indexes are synchronous and results provided by index are consistent

### <a name="drawbacks-of-current-property-indexes"></a>Drawbacks of current property indexes
Oak currently has support for synchronous property indexes which are used to meet above usecases. However the current implementation has certain drawbacks

* Perform poorly over remote storage - The property indexes are stores as normal NodeState and hence reading them over remote storage like Mongo performs poorly
* Prone to conflicts - The content mirror store strategy is prone to conflict if the index content is volatile
* Storage overhead - The storage over head is large specially for remote storage as each NodeState is mapped to 1 Document.
 ---
### <a name="proposal"></a>Proposal
To overcome the drawbacks and still meet the synchronous index requirements we can implement a hybrid index where the indexes content is stored using both property index (for recent enrties) and lucene indexes (for older entries). At high level flow would be

* Store recently added index content as normal property index
* As part of async indexer run index the same content as part of lucene index
* Later prune the property index content which would have been indexed as part of lucene index
* Any query would result in union of query results from both property index and lucene indexes (with some caveats)

#### <a name="index-definition"></a>Index Definition
The synchronous index support would need to be enabled via index definition

* async - This needs to have an entry sync
* Set sync to true for each property definition which needs to be indexed in a sync way
```
/oak:index/assetType
- jcr:primaryType = "oak:QueryIndexDefinition"
- type = "lucene"
- async = ["async", "sync"]
+ indexRules
    + nt:base
        + properties
            + resourceType
                - propertyIndex = true
                - name = "assetType"
                - sync = true
```

For unique indexes set unique i.e. true
```
/oak:index/uuid
- jcr:primaryType = "oak:QueryIndexDefinition"
- type = "lucene"
- async = ["async", "sync"]
+ indexRules
    + nt:base
        + properties
            + uuid
                - propertyIndex = true
                - name = "jcr:uuid"
                - unique = true
```

### <a name="index-storage"></a>Index Storage
The property index content would be stored as hidden nodes under the index definition nodes. The storage structure would be similar to existing format for property index with some changes

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

Here

* :property-index - hidden node under which property indexes would be stored for various properties which are marked as sync
* For unique index entry each entry would also have a time stamp which would later used for pruning

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

Here we create new buckets of index values which simplifies the pruning. New buckets would get created after each successful async indexer run and older buckets would get removed. The bucket would in turn have structure similar to content mirror store strategy

For each property being index keep a head property which refers to the current active bucket. This would be changed by IndexPruner. In addition there would be a previous bucket to refer to the last active bucket.

On each run of IndexPruner

* Check if IndexStatsMBean#LastIndexedTime is changed from last known time
* If changed then
  * Create a new bucket by incrementing the current head value
  * Set previous to current head
  * Set head to new head value
  * Set lastUpdated on previous bucket to now
* Remove all other buckets

We require both head and previous bucket as there would be some overlap between content in previous

#### <a name="index-pruner"></a>Index Pruner
Index Pruner is a periodic task which would be responsible for pruning the index content. It would make use of IndexStatsMBean#LastIndexedTime to determine upto which time async indexer has indexed the repository and then remove entries from the property index which are older than that time

* Property index - here pruning would be done by creating a new bucket and then removing the older bucket.
* Unique index - Here prunining would be done by iterating over current indexed content and removing the older ones

### <a name="query-evaluation"></a>Query Evaluation
On the query side we would be performing a union query over the 2 index types. A union cursor would be created which would consist of

* LucenePathCursor - Primary cursor backed by Lucene index
* PropertyIndexCursor - A union of path cursor from current head and previous bucket
#### Open Points
If there are multiple property definition in Lucene index marked with sync and query involves constraints on more than 1 then which property index should be picked

## Attachments:
[Hybrid Index v2.pdf (application/pdf)](#hybrid-index-v2.pdf)

[OAK-4412]: https://issues.apache.org/jira/browse/OAK-4412
[hybrid-index-v2.pdf]:https://jackrabbit.apache.org/archive/wiki/JCR/attachments/115513516/115513517.pdf
