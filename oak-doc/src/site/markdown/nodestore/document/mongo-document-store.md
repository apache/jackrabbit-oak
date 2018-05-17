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
1.4.x | 3.2.x
1.6.x | 3.2.x
1.8.x | 3.4.x

For production deployments use a replica-set with at least three mongod
instances and a majority write concern. Fewer than three instances (e.g. two
instances and an arbiter) may lead to data loss when the primary fails.

When using MongoDB 3.4 or newer, set the [maxStalenessSeconds](https://docs.mongodb.com/manual/core/read-preference/#maxstalenessseconds)
option in the MongoDB URI to `90`. This is an additional safeguard and will
prevent reads from a secondary that are too far behind.

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

## <a name="configuration"></a> Configuration

Independent of whether the DocumentNodeStore is initialized via the OSGi service
or the builder, the implementation will automatically take care of some
appropriate default MongoDB client parameters. This includes the write concern,
which controls how many MongoDB replica-set members must acknowledge a write
operation. Without an explicit write concern specified in the MongoDB URI, the
implementation will set write concern based on the MongoDB topology. If MongoDB
is setup as a replica-set, then Oak will use a `majority` write concern. When
running on a single standalone MongoDB instance, the write concern is set to
'acknowledged'.

Similarly, a read concern is set automatically, unless explicitly specified in
the MongoDB URI. A `majority` read concern is set when supported, enabled on
the MongoDB server and a majority write concern is in use. Otherwise the read
concern is set to `local`.

Oak will log WARN messages if it deems the read and write concerns given via the
MongoDB URI insufficient.

