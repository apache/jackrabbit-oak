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

# Repository migration

Oak offers a few tools which can be used to migrate the old, Jackrabbit 2 repository and also to copy content between Oak instances. `oak-upgrade` is a Swiss army knife for copying content between virtually any repositories. On the other hand, the `SplitBlobStore` allows to copy the binaries gradually, from one blob store to another, without having a long repository downtime.

## Offline migration using oak-upgrade

The `oak-upgrade` module allows to do an upgrade from the classic Jackrabbit 2.0 repository to the Oak node store and also to sidegrade from one nodestore type to another. Besides from that it has a number of features that can be useful in everyday system maintenance:

* copying only a selcted subtree from one repository to another,
* precise control over version histories migration,
* migrating binaries from one blobstore to another.

### Sidegrade

`oak-upgrade` module creates an executable jar file. It can be invoked like this:

    java -jar oak-upgrade-*.jar [options] source destination

The `source` and `destination` are the node store paths/URIs. Following node stores are supported:

* `SegmentNodeStore` - use a path to the `repository` directory,
* `DocumentNodeStore` with MongoDB - `mongodb://host:port/database`,
* `DocumentNodeStore` with a RDB - `jdbc:...`. It requires passing user and password with separate parameters.

Following parameters should be used for the JDBC node store:

* Source database: `--src-password=... --src-user=...`
* Desination database: `--user=... --password=...`

Examples:

    java -jar oak-upgrade-*.jar \
        path/to/the/repository \
        mongodb://localhost:27017/oak
    
    java -jar oak-upgrade-*.jar \
        --user=sa --password=sa \
        mongodb://localhost:27017/oak \
        jdbc:h2:path/to/repo

### Upgrade

In order to upgrade Jackrabbit 2 repository to the new node store, pass the path to the `repository` directory as the `source` parameter. Optionally, you may also pass the path to the `repository.xml` file as a separate parameter. Examples:

    java -jar oak-upgrade-*.jar \
        path/to/the/jr2/repository \
        path/to/repository.xml \
        path/to/the/new/repository

    java -jar oak-upgrade-*.jar \
        path/to/the/jr2/repository \
        mongodb://localhost:27017/oak

### Migrating blob store

By default, the `oak-upgrade` only copies the binary references, so you need to reuse the same blob/data store in the new repository. However, it's also possible to migrate binaries as well using the `--copy-binaries` parameter. Following migration paths are possible for the binaries. The *internal* means that the binaries are stored inside the segment or document node store:

| From ↓ To →   | Internal | FileBlobStore | FileDataStore | S3   |
| :-----------: | :------: | :-----------: | :-----------: | :--: |
| Internal      | Yes²     | Yes           | Yes           | Yes  |
| FileBlobStore | Yes      | Yes²          | Yes           | Yes  |
| FileDataStore | Yes      | Yes           | Yes²          | Yes¹ |
| S3            | \-       | \-            | \-            | Yes² |

¹ Online migration is available as well using Granite Amazon S3DataStore Connector <br/>
² The storage might be simple cloned without using oak2oak

Following parameters can be used to define the source and the destination blob stores:

| Blob store type | Source parameter                   | Destination           |
| :-------------: | :--------------------------------: | :-------------------: |
| FileBlobStore   | `--src-fileblobstore=...`          | `--fileblobstore=...` |
| FileDataStore   | `--src-datastore=...`              | `--datastore=...`     |
| S3              | `--s3config=... --s3datastore=...` | \-                    |

Use the `--copy-binaries` parameter to instruct the `oak-upgrade` to copy binaries:

Example:

    java -jar oak-upgrade-*.jar \
        --copy-binaries \
        --src-datastore=/old/repository/datastore \
        --fileblobstore=/new/repository/datastore \
        /old/repository \
        /new/repository

### Migrating a subtree

It's possible to define a list of content subtrees to include or exclude during the migration. By default, the whole repository gets copied. In order to copy only a subtree, use the `--include-paths`. For example, the following command will copy only the `/content/site` and `/content/other_site` subtrees:

    java -jar oak-upgrade-*.jar \
        --include-paths=/content/site,/content/other_site \
        /old/repository \
        /new/repository

You may also exclude specific paths from being copied. Following command will copy the whole content without the `/content/redundant_site`:

    java -jar oak-upgrade-*.jar \
        --exclude-paths=/content/redundant_site \
        /old/repository \
        /new/repository

By default, the source repository replaces the destination repository (if there's one). For instance, in the first example if the `/content/site` node already exists in the destination repository, it'll be removed and replaced by the source node. It's also possible to merge content from the source repository with `--merge-paths`:

    java -jar oak-upgrade-*.jar \
        --merge-paths=/content/site \
        /old/repository \
        /new/repository

### Version history copying

By default, the whole version storage is migrated. This includes referenced version histories (their versionable node still exists in the repository) and orphaned ones (their versionable node no longer exists). `oak-upgrade` allows to skip the orphaned version histories to make the migration faster and the destination repository smaller. It's also possible to define a maximum age for the version histories (both referenced and orphaned) to be copied.

There are two parameters: `--copy-orphaned-versions` and `--copy-versions`. Both accepts boolean values or a `YYYY-MM-DD` date. Examples:

    # only copy referenced versions
    java -jar oak-upgrade-*.jar \
        --copy-orphaned-versions=false \
        /old/repository /new/repository

    # don't copy any versions at all
    java -jar oak-upgrade-*.jar \
        --copy-versions=false \
        /old/repository /new/repository

    # copy referenced versions created after 2010
    # and orphaned version created after 2011
    java -jar oak-upgrade-*.jar \
        --copy-versions=2010-01-01 \
        --copy-orphaned-versions=2011-01-01 \
        /old/repository /new/repository

### Other parameters

The full list of supported parameters can be displayed using `--help` switch.

## Online blob migration with SplitBlobStore

Oak offers one more way to migrate blob store, without turning off the instance (a few restarts might be required, but the migration process is done during normal repository operation).

There is a `SplitBlobStore` implementation, that takes two blob stores: the old (already existing) and the new (empty) one. After configuring Oak to use it, all write requests are proxied to the new repository. The read requests uses the old or the new repository, depending on the blob id (`SplitBlobStore` saves all the new blob ids).

Besides from the new blob store implementation, there is a process (controlled by JMX) which migrates binaries between stores. When all binaries are migrated, the `SplitBlobStore` can be disabled as well as the old store.

#### Requirements

* An OSGi-based Oak installation (eg. Sling or AEM).

#### Enabling SplitBlobStore - external blob store case

These steps should be followed for migration from `FileBlobStore`, `FileDataStore` or `S3DataStore`.

1. Add `split.blobstore=old` OSGi property to the source blob store.
2. Configure the destination blob store and add `split.blobstore=new` property to its OSGi configuration.
3. Create a configuration for the `org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService`.

        split.old.blobstore.type=INTERNAL
        # optional:
        repository.home=crx-quickstart/repository

    * The directory is used to save the `migrated_blobs.txt` file.
4. Restart the instance

#### Enabling SplitBlobStore - internal blob store case

These steps should be followed for migration from `MongoBlobStore` or for blobs embedded in the `SegmentNodeStore`.

1. Configure the destination blob store and add `split.blobstore=new` property to its OSGi configuration.
2. Create a configuration for the `org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService`.

        # use DOCUMENT or SEGMENT, depending on the NodeStore type:
        split.old.blobstore.type=SEGMENT
        # optional:
        repository.home=crx-quickstart/repository

    * The directory is used to save the migrated_blobs.txt file.
4. Restart the instance

After starting the instance, the `SplitBlobStoreService` will wait until blob stores with `split.blobstore` properties (the `old` and the `new`) are available. They will be bound and the `SplitBlobStore` will be registered in the OSGi. On the other hand, the `NodeStoreService` will ignore blob stores configured with the `split.blobstore` property and will wait until the `SplitBlobStore` is available.

From this point, all the new blobs will be saved in the new blob store. Binaries from the old blob store will be available to read.

The `split.blobstore` property support was added to `FileBlobStore`, `AbstractDataStoreService` (handling all Jackrabbit data stores), `DocumentNodeStoreService` and `SegmentNodeStoreService`.

#### Migration

1. Find `BlobMigration` JMX bean in the Felix console.
2. Run `startBlobMigration(false)` operation

The migration can be stopped using `stopBlobMigration()` and then resumed with `startBlobMigration(true)`. The current stats are available via the JMX as well:

* last processed path,
* number of migrated nodes.

#### Switching to the new blob store

When the migration is finished, it's possible to completely switch to the new blob store:

1. Remove the configuration for the old blob store.
2. Remove the configuration for the `SplitBlobStoreService`
3. Remove the `split.blobstore=new` OSGi property from the new blob store, so it can be find by the `NodeStoreService`.
4. Restart the instance, so there are no JCR sessions bound to the old NodeState.

Migration is now complete!
