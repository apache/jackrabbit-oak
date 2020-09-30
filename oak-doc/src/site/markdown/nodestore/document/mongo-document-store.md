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
# MongoDB DocumentStore

The `MongoDocumentStore` is one of the backend implementations for the
[DocumentNodeStore](../documentmk.html). The MongoDocumentStore use MongoDB
to persist nodes as documents.

## <a name="recommendations"></a> Recommendations

Before deploying a MongoDocumentStore on MongoDB, make sure recommendations
described in [production notes](https://docs.mongodb.com/manual/administration/production-notes/)
have been applied.

The recommended MongoDB version depends on the Oak release. Below table lists
the recommended MongoDB version for each Oak release. More recent MongoDB
versions may also work, but are untested.

Oak Release | MongoDB version
------------|----------------
1.0.x | 2.6.x
1.2.x | 3.2.x
1.4.0 - 1.4.22 | 3.2.x
1.4.23 or newer | 3.6.x
1.6.0 - 1.6.13 | 3.2.x
1.6.14 or newer | 3.6.x
1.8.0 - 1.8.6 | 3.4.x
1.8.7 or newer | 3.6.x
1.10.x or newer | 4.0.x

For production deployments use a replica-set with at least three mongod
instances and a majority write concern. Fewer than three instances (e.g. two
instances and an arbiter) may lead to data loss when the primary fails.

When using MongoDB 3.4 or newer, set the [maxStalenessSeconds](https://docs.mongodb.com/manual/core/read-preference/#maxstalenessseconds)
option in the MongoDB URI to `90`. This is an additional safeguard and will
prevent reads from a secondary that is too far behind.

Initializing a DocumentNodeStore on MongoDB with default values will also use
MongoDB to store blobs. While this is convenient for development and tests, the
use of MongoDB as a blob store in production is not recommended. MongoDB
replicates all changes through a single op-log. Large blobs can lead to a
significantly reduced op-log window and cause delay in replicating other changes
between the replica-set members. See available [blob stores](../../plugins/blobstore.html)
alternatives for production use.

## <a name="initialization"></a> Initialization

The recommended method to initialize a `DocumentNodeStore` with a
`MongoDocumentStore` is using an OSGi container and configure the
`DocumentNodeStoreService`. See corresponding [Repository OSGi Configuration](../../osgi_config.html).

Alternatively a MongoDB based DocumentNodeStore can be created with the help of
a `MongoDocumentNodeStoreBuilder`.

    DocumentNodeStore store = newMongoDocumentNodeStoreBuilder()
        .setMongoDB("mongodb://localhost:27017", "oak", 0).build();
    // do something with the store
    NodeState root = store.getRoot();

    // dispose it when done
    store.dispose();

Please note, even though the default is to store blobs in MongoDB, this is not
a recommended setup. See also [recommendations](#recommendations).

## <a name="read-preference"></a> Read preference

Without any read preference specified in the MongoDB URI, most read operations
will be directed to the MongoDB primary to ensure consistent reads.
Functionality like Revision Garbage Collection does not have such a strict
requirement and will therefore scan (read) for garbage with a read preference
of `secondaryPreferred`. This takes pressure off the primary.

When the `MongoDocumentStore` is configured with an explicit read preference via
the MongoDB URI, the read preference is considered a hint. The implementation
may still read from the primary when it cannot guarantee a consistent read from
a secondary. This may be the case when a secondary lags behind and a read
happens for a document that was recently modified.

A common use case is setting a read preference for a nearby secondary. This can
be achieved with `readPreferenceTags` in the MongoDB URI.

The below example will prefer a secondary with tag _dc:ny,rack:1_. If no such
secondary is available, the read operation will target to a secondary with tag
_dc:ny_ and if no such secondary is available either, any available secondary is
chosen. As a final fallback the read will be served from the primary.

    mongodb://example1.com,example2.com,example3.com/?readPreference=secondaryPreferred&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=

Refer to [Read Preference Options][3] and [Write Concern Options][4] for more details.

Starting with Oak 1.9.3 the `MongoDocumentStore` automatically uses client
sessions with causal consistency when running on MongoDB 3.6 or newer. This
feature can be disabled by setting a system property: `-Doak.mongo.clientSession=false`.

Causal consistent client sessions allows `MongoDocumentStore` to always read
from a secondary when configured with the relevant read preference and at the
same time guarantee the required consistency. This is most useful in a
distributed deployment with multiple Oak processes connected to a MongoDB
replica-set. Putting a MongoDB secondary close to an Oak process ensures low
read latency when Oak is configured to always read from the nearby MongoDB
secondary.

The `MongoDocumentStore` periodically estimates the replication lag of the
secondaries in the replica-set and may decide to read from the primary even when
the read preference is configured otherwise. This is to prevent high query times
when the secondary must wait for the replication to catch up with the state
required by the client session. The estimate is rather rough and will switch
over to the primary when the lag is estimated to be more than five seconds.

## <a name="configuration"></a> Configuration

Independent of whether the DocumentNodeStore is initialized via the OSGi service
or the builder, the implementation will automatically take care of some
appropriate default MongoDB client parameters. This includes the write concern,
which controls how many MongoDB replica-set members must acknowledge a write
operation. Without an explicit write concern specified in the MongoDB URI, the
implementation will set a write concern based on the MongoDB topology. If MongoDB
is setup as a replica-set, then Oak will use a `majority` write concern. When
running on a single standalone MongoDB instance, the write concern is set to
'acknowledged'.

Similarly, a read concern is set automatically, unless explicitly specified in
the MongoDB URI. A `majority` read concern is set when supported, enabled on
the MongoDB server and a majority write concern is in use. Otherwise the read
concern is set to `local`.

Oak will log WARN messages if it deems the read and write concerns given via the
MongoDB URI insufficient.

In addition to specifying the [read preference][1] and [write concern][2] in the
MongoDB URI, those two parameters can also be set and changed at runtime by
setting the property `readWriteMode` in the cluster node metadata. A cluster
node will pick up the change within ten seconds (when it renews the lease of the
cluster node id). This is a string property with the format
`'readPreference=<preference>&w=<writeConcern>'` similar to the way it is used
in MongoDB URI. Just that it does not include other option details.

The following MongoDB shell command will set the read preference to `primary`
and the write concern to `majority` for all cluster nodes:

    > db.clusterNodes.update({},
      {$set: {readWriteMode:'readPreference=primary&w=majority'}},
      {multi: true})

[1]: http://docs.mongodb.org/manual/core/read-preference/
[2]: http://docs.mongodb.org/manual/core/write-concern/
[3]: http://docs.mongodb.org/manual/reference/connection-string/#read-preference-options
[4]: http://docs.mongodb.org/manual/reference/connection-string/#write-concern-options
