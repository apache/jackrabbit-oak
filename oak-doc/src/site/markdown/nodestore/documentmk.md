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

Oak Document Storage
==============

One of the plugins in Oak stores data in a document oriented format. 
The plugin implements the low level `NodeStore` interface.

The document storage optionally uses the [persistent cache](persistent-cache.html) to reduce read operations on the backend storage.

Backend implementations
-----------------------

DocumentMK supports a number of backends, with a storage abstraction called `DocumentStore`:

* `MongoDocumentStore`: stores documents in a MongoDB. Oak requires MongoDB 2.6.x or higher.
* `MemoryDocumentStore`: keeps documents in memory. This implementation should only be used for testing purposes.
* `RDBDocumentStore`: stores documents in a relational data base.

The remaining part of the document will focus on the `MongoDocumentStore` to explain and illustrate concepts of the DocumentMK.

Content Model
-------------

The repository data is stored in two collections: the `nodes` collection for node data,
and the `blobs` collection for binaries. There is a third collection, `clusterNodes`,
which contains metadata of all cluster nodes. The data can be viewed using the 
MongoDB shell:

    > show collections
    blobs
    clusterNodes
    nodes

Node Content Model
------------------

The `DocumentMK` stores each node in a separate MongoDB document and updates to
a node are stored by adding new revision/value pairs to the document. This way
the previous state of a node is preserved and can still be retrieved by a
session looking at a given snapshot (revision) of the repository.

The basic MongoDB document of a node in Oak looks like this:

    {
        "_id" : "1:/node",
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_lastRev" : {
            "r0-0-1" : "r13f3875b5d1-0-1"
        },
        "_modified" : NumberLong(274208361),
        "_modCount" : NumberLong(1),
        "_children" : Boolean(true),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c"
        }
    }

All fields in the above document are metadata and are not exposed through the
Oak API. DocumentMK has two types of fields. Simple fields are key/value pairs
like the `_id` or `_modified` field. Versioned fields are kept in sub-documents
where the key is a revision paired with the value at this revision.

The `_id` field is used as the primary key and consists of a combination of the
depth of the path and the path itself. This is an optimization to align sibling
keys in the index.

The `_deleted` sub-document contains the revision this node was created in. In
the above example the root node was created in revision `r13f3875b5d1-0-1`. If
the node is later deleted, the `_deleted` sub-document will get a new field with
the revision the node was deleted in.

The sub-document `_lastRev` contains the last revision written to this node by
each cluster node. In the above example the DocumentMK cluster node with id `1`
modified the node the last time in revision `r13f3875b5d1-0-1`, when it created
the node. The revision key in the `_lastRev` sub-document is synthetic and the
only information actually used by DocumentMK is the clusterId. The `_lastRev`
sub-document is only updated for non-branch commits or on merge, when changes
become visible to all readers.

The `_modified` field contains an indexed low-resolution timestamp when the node
was last modified. The time resolution is five seconds. This field is also updated
when a branch commit modifies a node.

The `_modCount` field contains a modification counter, which is incremented with
every change to the document. This field allows DocumentMK to perform conditional updates
without requesting the whole document.

The `_children` field is a boolean flag to indicate if this node has child nodes. By
default a node would not have this field. If any node gets added as child of this node
then it would be set to true. It is used to optimize access to child nodes and allows DocumentMK to omit calls to fetch child nodes for leaf nodes.

Finally, the `_revisions` sub-document contains commit information about changes
marked with a revision. E.g. the single entry in the above document tells us
that everything marked with revision `r13f3875b5d1-0-1` is committed and
therefore valid. In case the change is done in a branch then the value would be the
base revision. It is only added for those nodes which happen to be the commit root
for any given commit.

Adding a property `prop` with value `foo` to the node in a next step will
result in the following document:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "r0-0-1" : "r13f38818ab6-0-1"
        },
        "_modified" : NumberLong(274208516),
        "_modCount" : NumberLong(2),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c",
            "r13f38818ab6-0-1" : "c"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

Now the document contains a new sub-document with the name of the new property.
The value of the property is annotated with the revision the property was set.
With each successful commit to this node, a new field is added to the
`_revisions` sub-document. Similarly the `_lastRev` sub-document and `_modified`
field are updated.

After the node is deleted the document looks like this:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false",
            "r13f38835063-2-1" : "true"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "r0-0-1" : "r13f38835063-2-1"
        },
        "_modified" : NumberLong(274208539),
        "_modCount" : NumberLong(3),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "c",
            "r13f38818ab6-0-1" : "c",
            "r13f38835063-2-1" : "c"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

The `_deleted` sub-document now contains a `r13f38835063-2-1` field marking the
node as deleted in this revision.

Reading the node in previous revisions is still possible, even if it is now
marked as deleted as of revision `r13f38835063-2-1`.

Revisions
---------

As seen in the examples above, a revision is a String and may look like this:
`r13f38835063-2-1`. It consists of three parts:

* A timestamp derived from the system time of the machine it was generated on: `13f38835063`
* A counter to distinguish revisions created with the same timestamp: `-2`
* The cluster node id where this revision was created: `-1`

Clock requirements
------------------

Revisions are used by the DocumentMK to identify the sequence of changes done
on items in the repository. This is also done across cluster nodes for revisions
with different cluster node ids. This requires the system clocks on the machines
running Oak and the backend system to approximately in sync. It is recommended
to run an NTP daemon or some similar service to keep the clock synchronized.
Oak allows clock differences up to 2 seconds between the machine where Oak is
running and the machine where the backend store (MongoDB or RDBMS) is running.
Oak may refuse to start if it detects a larger clock difference. Clock
differences between the machines running in an Oak cluster will result in
delayed propagation of changes between cluster nodes and warnings in the log
files.

Branches
--------

DocumentMK implementations support branches, which allows a client to stage
multiple commits and make them visible with a single merge call. In DocumentMK
a branch commit looks very similar to a regular commit, but instead of setting
the value of an entry in `_revisions` to `c` (committed), it marks it with
the base revision of the branch commit. In contrast to regular commits where
the commit root is the common ancestor of all nodes modified in a commit, the
commit root of a branch commit is always the root node. This is because a
branch will likely have multiple commits and a commit root must already be
known when the first commit happens on a branch. To make sure the following
branch commits can use the same commit root, DocumentMK simply picks the root
node, which always works in this case.

A root node may look like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91720-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(2),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\""
        }
    }

The root node was created in revision `r13fcda88ac0-0-1` and later
in revision `r13fcda91720-0-1` property `prop` was set to `foo`.
To keep the example simple, we now assume a branch is created based
on the revision the root node was last modified and a branch commit
is done to modify the existing property. After the branch commit
the root node looks like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91720-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(3),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "r13fcda91720-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

At this point the modified property is only visible to a reader
when it reads with the branch revision `r13fcda919eb-0-1` because
the revision is marked with the base version of this commit in
the `_revisions` sub-document. Note, the `_lastRev` is not updated
for branch commits but only when a branch is merged.

When the branch is later merged, the root node will look like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91b12-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(4),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "c-r13fcda91b12-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

Now, the changed property is visible to readers with a revision equal or
newer than `r13fcda91b12-0-1`.

The same logic is used for changes to other nodes that belong to a branch
commit. DocumentMK internally resolves the commit revision for a modification
before it decides whether a reader is able to see a given change.

Previous Documents
------------------

Over time the size of a document grows because DocumentMK adds data to the document
with every modification, but never deletes anything to keep the history. Old data
is moved when there are 1000 commits to be moved or the document is bigger than
1 MB. A document with a reference to old data looks like this:

    {
        "_deleted" : {
            "r13fcda88ac0-0-1" : "false",
        },
        "_id" : "0:/",
        "_lastRev" : {
            "r0-0-1" : "r13fcda91b12-0-1"
        },
        "_modified" : NumberLong(274708995),
        "_modCount" : NumberLong(1004),
        "_revisions" : {
            "r13fcda88ac0-0-1" : "c",
            "r13fcda91720-0-1" : "c",
			"r13fcda919eb-0-1" : "c-r13fcda91b12-0-1"
        },
        "_prev" : {
            "r13fcda88ae0-0-1" : "r13fcda91710-0-1"
        },
        "prop" : {
            "r13fcda91720-0-1" : "\"foo\"",
			"r13fcda919eb-0-1" : "\"bar\"",
        }
    }

The optional sub-document `_prev` contains a list of revision pairs, each
indicating the range of commit revisions a previous document contains. In
the above example there is one document with previous commits from
`r13fcda88ae0-0-1` to `r13fcda91710-0-1`. The id of the previous document
is derived from the upper bound of the range and the id/path of the current
document. The id of the previous document for `r13fcda88ae0-0-1` and `0:/`
is `1:p/r13fcda88ae0-0-1` and may looks like this:

    {
        "_id" : "1:p/r13fcda88ae0-0-1",
        "_modCount" : NumberLong(1),
        "_revisions" : {
            "r13fcda88ae0-0-1" : "c",
            "r13fcda88af0-0-1" : "c",
            ...  
			"r13fcda91710-0-1" : "c"
        },
        "prop" : {
            "r13fcda88ae0-0-1" : "\"foo\"",
            "r13fcda88af0-0-1" : "\"bar\"",
            ...
			"r13fcda91710-0-1" : "\"baz\""
        }
    }

Previous documents only contain immutable data, which means it only contains
committed and merged `_revisions`. This also means the previous ranges of
committed data may overlap because branch commits are not moved to previous
documents until the branch is merged.
 

Background Operations
---------------------
Each DocumentMK instance connecting to same database in Mongo server performs certain background task.

### Renew Cluster Id Lease

Each cluster node uses a unique cluster node id, which is the last part of the revision id.
Each cluster node has a lease on the cluster node id, as described in the section
[Cluster Node Metadata](#Cluster_Node_Metadata).

### Background Document Split

DocumentMK periodically checks documents for their size and if necessary splits them up and
moves old data to a previous document. This is done in the background by each DocumentMK
instance for the data it created.

### Background Writes

While performing commits there are certain nodes which are modified but do not become part
of commit. For example when a node under /a/b/c is updated then the `_lastRev` property
of all ancestors also need to be updated to the commit revision. Such changes are accumulated
and flushed periodically through a asynchronous job.

<a name="bg-read"></a>
### Background Reads

DocumentMK periodically picks up changes from other DocumentMK instances by polling the root node
for changes of `_lastRev`. This happens once every second.

Pending Topics
--------------

### Conflict Detection and Handling

Cluster Node Metadata
---------------------

Cluster node metadata is stored in the `clusterNodes` collection. There is one entry
for each cluster node that is running, and there are entries for cluster nodes that were
ran. Old entries are kept so that if a cluster node is started again, it gets the same 
cluster node id as before (which is not strictly needed for consistency, but nice for
support, if one would want to find out which change originated from which cluster node).

Each running cluster node updates the lease time of the cluster node id once every minute,
to ensure each cluster node uses a different cluster node id.

    > db.clusterNodes.find().pretty()
    
    {
		"_id" : "1",
		"_modCount" : NumberLong(2),
		"leaseEnd" : NumberLong("1390465250135"),
		"instance" : "/Users/test/jackrabbit/oak/trunk/oak-jcr",
		"machine" : "mac:20c9d043f141",
		"info" : "...pid: 11483, uuid: 6b6e8e4f-8322-4b19-a2b2-de0c573620b9 ..."
	}
	{
		"_id" : "2",
		"_modCount" : NumberLong(2),
		"leaseEnd" : NumberLong("1390465252206"),
		"instance" : "/Users/mueller/jackrabbit/oak/trunk/oak-jcr",
		"machine" : "mac:20c9d043f141",
		"info" : "...pid: 11483, uuid: 28ada13d-ec9c-4d48-aeb9-cef53aa4bb9e ..."
	}

The `_id` is the cluster node id of the node, which is the last part of the revision id.
The `leaseEnd` is updated once per minute by running cluster nodes. It is the number
of milliseconds since 1970.
The `instance` is the current working directory.
The `machine` is the lowest number of the network addresses, 
or a random uuid if this is not available.
The `info` contains the same info as a string, plus additionally the process id
and the uuid.

<a name="rw-preference"></a>
### Specifying the Read Preference and Write Concern

With `MongoDocumentStore` you can specify the the [read preference][1] and [write concern][2]. 
This can be enabled in Oak via two modes. 

Note that `MongoDocumentStore` might still use a pre defined read preference like primary 
where ever required. So if for some code path like reading latest `_lastRev` of root node 
its required that read is performed from primary (for consistency) then code would explicitly 
use the readPreference primary for that operation. For all other operation Mongo Java Driver would
use default settings where read preference is set to `Primary` and write concern is set to `Acknowledged`. 
Via using one of the two modes below a user can tune the default settings as per its need

#### Via Configuration

In this mode the config is specified as part of the Mongo URI (See [configuration](../osgi_config.html#document-node-store)). 
So if a user wants that reads from secondaries should prefer secondary with tag _dc:ny,rack:1_ 
otherwise they go to other secondary then he can specify that via following mongouri

    mongodb://example1.com,example2.com,example3.com/?readPreference=secondary&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags= 

Refer to [Read Preference Options][3] and [Write Concern Options][4] for more details.  
 
#### Changing at Runtime

The read preference and write concern of all cluster nodes can be changed at runtime
without having to restart the instances, by setting the property `readWriteMode` of
this collection. All cluster nodes will pick up the change within one minute 
(when they renew the lease of the cluster node id). This is a string property with the
format `'readPreference=<preference>&w=<writeConcern>'` similar to the way it is used in mongouri. 
Just that it does not include other option details. The following shell command will
set the read preference to `primary` and the write concern to `majority` for all
cluster nodes:

    > db.clusterNodes.update({}, 
      {$set: {readWriteMode:'readPreference=primary&w=majority'}}, 
      {multi: true})    

<a name="cache"></a>
Caching
-------

`DocumentNodeStore` maintains multiple caches to provide an optimum performance. 
By default the cached instances are kept in heap memory but some of the caches 
can be backed by [persistent cache](persistent-cache.html).

1. `documentCache` - Document cache is used for caching the `NodeDocument` 
    instance. These are in memory representation of the persistent state. For 
    example in case of Mongo it maps to the Mongo document in `nodes` collection 
    and for RDB its maps to the row in `NODES` table. There is a class of `NodeDocument`
    (leaf level split documents) which, since `1.3.15` are cached under
    `prevDocCache` (see below)
    
    Depending on the `DocumentStore` implementation different heuristics are 
    applied for invalidating the cache entries based on changes in backend  
    
2. `prevDocCache` - Previous document cache is used for caching the `NodeDocument` 
    instance representing leaf level split documents. Unlike other type of
    `NodeDocument`, these are immutable and hence don't require invalidation.
    If configured, this cache can exploit persistent cache as well.
    Similar to other `NodeDocument` these are also in memory representation of
    the persistent state. (since `1.3.15`)
    
    Depending on the `DocumentStore` implementation different heuristics are 
    applied for invalidating the cache entries based on changes in backend  
    
3. `docChildrenCache` - Document Children cache is used to cache the children 
    state for a given parent node. This is invalidated completely upon every 
    background read. This cache was removed in 1.5.6.
    
4. `nodeCache` - Node cache is used to cache the `DocumentNodeState` instances.
    These are **immutable** view of `NodeDocument` as seen at a given revision
    hence no consistency checks are to be performed for them
     
5. `childrenCache` - Children cache is used to cache the children for a given
    node. These are also **immutable** and represent the state of children for
    a given parent at certain revision
    
5. `diffCache` - Caches the diff for the changes done between successive revision.
   For local changes done the diff is add to the cache upon commit while for 
   external changes the diff entries are added upon computation of diff as part 
   of observation call
   
All the above caches are managed on heap. For the last 3 `nodeCache`, 
`childrenCache` and `diffCache` Oak provides support for [persistent cache]
(persistent-cache.html). By enabling the persistent cache feature Oak can manage
a much larger cache off heap and thus avoid freeing up heap memory for application
usage.

### Cache Invalidation

`documentCache` and `docChildrenCache` are containing mutable state which requires
consistency checks to be performed to keep them in sync with the backend persisted
state. Oak uses a MVCC model under which it maintains a consistent view of a given
Node at a given revision. This allows using local cache instead of using a global
clustered cache where changes made by any other cluster node need not be instantly
reflected on all other nodes. 

Each cluster node periodically performs [background reads](#bg-read) to pickup 
changes done by other cluster nodes. At that time it performs [consistency check]
[OAK-1156] to ensure that cached instance state reflect the state in the backend 
persisted state. Performing the check would take some time would be proportional 
number of entries present in the cache. 
    
For repository to work properly its important to ensure that such background reads 
do not consume much time and [work is underway][OAK-2646] to improve current 
approach. _To ensure that such background operation (which include the cache 
invalidation checks) perform quickly one should not set a large size for 
these caches_.

All other caches consist of immutable state and hence no cache invalidation needs
to be performed for them. For that reason those caches can be backed by persistent
cache and even having large number of entries in such caches would not be a matter
of concern. 

### Cache Configuration

In a default setup the [DocumentNodeStoreService][osgi-config]
takes a single config for `cache` which is internally distributed among the 
various caches above in following way

1. `nodeCache` - 35% (was 25% until 1.5.14)
2. `prevDocCache` - 4%
3. `childrenCache` - 15% (was 10% until 1.5.14)
4. `diffCache` - 30% (was 4% until 1.5.14)
5. `documentCache` - Is given the rest i.e. 16%
6. `docChildrenCache` - 0% (removed in 1.5.6, default was 3%)

Lately [options are provided][OAK-2546] to have a fine grained control over the 
distribution. See [Cache Allocation][cache-allocation]

While distributing ensure that cache left for `documentCache` is not very large
i.e. prefer to keep that ~500 MB max or lower. As a large `documentCache` can 
lead to increase in the time taken to perform cache invalidation.

Further make use of the persistent cache. This reduces pressure on GC by keeping
instances off heap with slight decrease in performance compared to keeping them
on heap.

### Unlock upgrade <a name="unlockUpgrade"/>

On startup the DocumentNodeStore checks if its version is compatible with the
format version currently in use. A read-only DocumentNodeStore can read the 
current version as well as older versions. A read-write DocumentNodeStore on the
other hand can only write to the DocumentStore when the format version matches 
its own version. The DocumentNodeStore maintains this format version in the
`settings` collection accessible to all cluster nodes.

Upgrading to a newer Oak version may therefore first require an update of the
format version before a newer version of a DocumentNodeStore can be started on
existing data. The oak-run tools contains an `unlockUpgrade` mode to perform
this operation. Use the oak-run tool with the version matching the target 
upgrade version to unlock an upgrade with the following command. The below
example unlocks an upgrade to 1.8 with a DocumentNodeStore on MongoDB:

    > java -jar oak-run-1.8.0.jar unlockUpgrade mongodb://example.com:27017/oak

Please note that unlocking an upgrade is only possible when all cluster nodes
are inactive, otherwise the command will refuse to change the format version.

[1]: http://docs.mongodb.org/manual/core/read-preference/
[2]: http://docs.mongodb.org/manual/core/write-concern/
[3]: http://docs.mongodb.org/manual/reference/connection-string/#read-preference-options
[4]: http://docs.mongodb.org/manual/reference/connection-string/#write-concern-options
[OAK-1156]: https://issues.apache.org/jira/browse/OAK-1156
[OAK-2646]: https://issues.apache.org/jira/browse/OAK-2646
[OAK-2546]: https://issues.apache.org/jira/browse/OAK-2546
[osgi-config]: ../osgi_config.html#document-node-store
[cache-allocation]: ../osgi_config.html#cache-allocation

