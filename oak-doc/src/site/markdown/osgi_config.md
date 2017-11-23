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

# Repository OSGi Configuration

Oak comes with a simple mechanism for constructing content repositories
for use in embedded deployments and test cases. Details regarding that are provided
as part of [Repository Construction](construct.html). When used in OSGi environment
then various Oak components can be configured using OSGi Configuration Support.

Depending on component the configuration can be modified at runtime or needs to be
specified before the initial system setup.

Static Configuration
: Such configuration settings cannot be changed once a repository
  is initialized. For example choosing a `DataStore` or specifying the path of User Home.
  Such properties should not be changed once a system is initialized.

Dynamic Configuration
: Some of the configuration settings like thread pool size, cache size etc can be changed
  at runtime or after initial system setup

Each OSGi configuration is referred via a PID i.e. persistent identifier. Sections below
provide details around various PID used in Oak

### NodeStore

#### SegmentNodeStore

This section describes the configuration for the Segment Node Store, an implementation of the Node Store that saves repository data on disk.

The first of these configurations, identified by `org.apache.jackrabbit.oak.segment.SegmentNodeStoreService`, refers to the implementation provided by the `oak-segment-tar` bundle.
This implementation is the newest and the only actively maintained one, and should be used unless some legacy use case needs to be supported.

The second and last configuration, identified by `org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStoreService`, refers to the old implementation of the Node Store provided by the `oak-segment` bundle.
This implementation has been deprecated, will not receive any further improvements and should not be used, if possible.

##### <a name="config-SegmentNodeStoreService"></a> org.apache.jackrabbit.oak.segment.SegmentNodeStoreService

repository.home (string) - repository
: A path on the file system where repository data will be stored.
The Segment Store persists its data in a subdirectory of `repository.home` named `segmentstore`.
The provided path can be relative or absolute.
If a relative path is provided, it is handled according to the definition of relative path as specified by `java.io.File`.
Placeholders of any kind in the path are not supported.

tarmk.mode (string) - 64
: The architecture model.
It can assume only two values, `32` or `64`.
If this property is set to `64`, data files used by the Segment Store are memory-mapped.
Any other value will instruct the Segment Store not to use memory-mapped files.

tarmk.size (int) - 256
: The maximum size of TAR files on disk in MB.

segmentCache.size (int) - 256
: The maximum size of the segment cache in MB.
The segment cache keeps a subset of the segments in memory and avoids performing I/O operations when those segments are used.

stringCache.size (int) - 256
: The maximum size of the strings cache in MB.
The string cache keeps a subset of the string records in memory and avoids performing I/O operations when those strings are used.

templateCache.size (int) - 256
: The maximum size of the template cache in MB.
The template cache keeps a subset of the template records in memory and avoids performing I/O operations when those templates are used.

stringDeduplicationCache.size (int) - 15000
: The maximum size of the string deduplication cache in number of items.
The string deduplication cache tracks string records across different GC generations. It avoids duplicating a string record to the current GC generation if it was already duplicated in the past.

templateDeduplicationCache.size (int) - 3000
: The maximum size of the template deduplication cache in number of items.
The template deduplication cache tracks template records across different GC generations. It avoids duplicating a template record to the current GC generation if it was already duplicated in the past.

nodeDeduplicationCache.size (int) - 1048576
: The maximum size of the node deduplication cache in number of items.
The node deduplication cache tracks node records across different GC generations. It avoids duplicating a node record to the current generation if it was already duplicated in the past.

pauseCompaction (boolean) - false
: Determines if online compaction should be executed.
If this property is `true`, both the estimation and compaction phases of the online compaction process are not executed.

compaction.retryCount (int) - 5
: The number of commit attempts the online compaction process should try before giving up.
This property determines how many times the online compaction process should try to merge the compacted repository state with the user-generated state produced by commits executed concurrently during compaction.

compaction.force.timeout (int) - 60
: The amount of time the online compaction process is allowed to exclusively lock the store, in seconds.
If this property is set to a positive value, if the compaction process fails to commit the compacted state concurrently with other commits, it will acquire an exclusive lock on the Node Store.
The exclusive lock prevents other commits for completion, giving the compaction process a possibility to commit the compacted state.
This property determines how long the compaction process is allowed to use the Node Store in exclusive mode.
If this property is set to zero or to a negative value, the compaction process will not acquire an exclusive lock on the Node Store and will just give up if too many concurrent commits are detected.

compaction.sizeDeltaEstimation (long) - 10737418240
: The increase in size of the Node Store (in bytes) since the last successful compaction that will trigger another execution of the compaction phase.

compaction.disableEstimation (boolean) - false
: Disables the estimation phase of the online compaction process.
If this property is set to `true`, the estimation phase of the compaction process will never run, and compaction will always be triggered for any amount of garbage in the Node Store.

compaction.retainedGenerations (int) - 2
: The number of generations to retain the Node Store.

compaction.memoryThreshold (int) - 15
: The percentage of heap memory that should always be free while compaction runs.
If the available heap memory falls below the specified percentage, compaction will not be started or it will be aborted if it is already running.

compaction.progressLog (long) - -1
: Enables compaction progress logging at each set of compacted nodes. A value of `-1` disables the log.

standby (boolean) - false
: Determines if this Node Store is supposed to be used in standby mode.

customBlobStore (boolean) - false
: Determines if this Node Store is supposed to use a custom Blob Store.
If this property is `true`, a Data Store or a Blob Store needs to be configured for the Segment Store to pick it up (see below).
If this property is `false`, binaries will be stored in the Segment Store.

repository.backup.dir (string)
: The path on the file system where backups of this Node Store should be stored.
It defaults to a subdirectory of `repository.home` named `segmentstore-backup`.
If a relative path is provided, it is handled according to the definition of relative path as specified by `java.io.File`.
Placeholders of any kind in the path are not supported.

##### org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStoreService

**This configuration is deprecated.
It belongs to the legacy Node Store implementation provided by the oak-segment bundle.
This implementation should not be used anymore.
Instead, rely on the Node Store implementation provided by the oak-segment-tar bundle, whose configuration is described above.**

repository.home (string) - tarmk
: A path on the file system where repository data will be stored.
The Segment Store persists its data in a subdirectory of `repository.home` named `segmentstore`.
The provided path can be relative or absolute.
If a relative path is provided, it is handled according to the definition of relative path as specified by `java.io.File`.
Placeholders of any kind in the path are not supported.

tarmk.mode (string) - 64
: The architecture model.
It can assume only two values, `32` or `64`.
If this property is set to `64`, data files used by the Segment Store are memory-mapped.
Any other value will instruct the Segment Store not to use memory-mapped files.

tarmk.size (int) - 256
: Maximum file size (in MB).

cache (int) - 256
: Size of the segment cache in MB.

compaction.cloneBinaries (boolean) - false
: Determines if binaries should be cloned when performing online compaction.

compaction.cleanup (string) - CLEAN_OLD
: Determines how in-memory references to persisted data might influence the outcome of online compaction.
It can only assume the values `CLEAN_ALL`, `CLEAN_OLD` and `CLEAN_NONE`.
If this property is `CLEAN_ALL`, every memory reference is ignored.
If this property is `CLEAN_OLD`, only memory references newer than a certain timestamp are considered (see `compaction.cleanup.timestamp`).
If this property is `CLEAN_NONE`, no data pointed to by an in-memory reference will be cleaned up.

compaction.cleanup.timestamp (long) - 36000000
: The expiry time for in-memory references in ms.
If `CLEAN_OLD` is used as the value of the `compaction.cleanup` property, every piece of data pointed to by a reference newer than this time interval will not be considered eligible for compaction.

compaction.memoryThreshold (byte) - 5
: A multiplier to help in estimating the amount of memory needed for compaction.
Setting this property to `0` indirectly skips compaction because not enough memory is available.

compaction.gainThreshold (byte) - 10.
: A percentage, expressed as an integer between `0` and `100`, representing the expected gain of online compaction.
If the amount of garbage is not equals to or greater than this percentage of the total disk space occupied by the Node Store, compaction will not run.
Setting this property to `0` or to a negative value indirectly disables the estimation phase of the online compaction process.

pauseCompaction (boolean) - true
: Determines if online compaction should be executed.
If this property is `true`, both the estimation and compaction phases of the online compaction process are not executed.

compaction.retryCount (int) - 5
: The number of commit attempts the online compaction process should try before giving up.
It defaults to `5`.
This property determines how many times the online compaction process should try to merge the compacted repository state with the user-generated state produced by commits executed concurrently during compaction.

compaction.forceAfterFail (boolean) - false
: Determines if online compaction should force its commit after a certain amount of attempts.

compaction.lockWaitTime (int) - 60
: Determines how long (in seconds) online compaction can use the Node Store in exclusive mode.
It defaults to `60`.
This value determines the validity of the exclusive lock on the Node Store acquired by the online compaction process when forcing its commit.

persistCompactionMap (boolean) - true
: Determines if the compaction map should be persisted on disk.

standby (boolean) - false
: Determines if this Node Store should be started in standby mode.

secondary (boolean) - false
: Determines if this Node Store should be started as a secondary store.

customBlobStore (boolean) - false
: Determines if this Node Store is supposed to use a custom Blob Store.
If this property is `true`, a Data Store or a Blob Store needs to be configured for the Segment Store to pick it up (see below).
If this property is `false`, binaries will be stored in the Segment Store.

blobGcMaxAgeInSecs (long) - 86400
: BLOB Garbage Collector (GC) logic would only consider those BLOBs for GC which are not accessed recently (currentTime - lastModifiedTime > blobGcMaxAgeInSecs).
For example, as per default, only those BLOBs which have been created 24 hours in the past would be considered for GC. 
 It is strongly advised to not set this property to a very low value of say a few minutes but only set it to a hour at a
  minimum. This is to ensure that the NodeStore(s) have had the time to flush out its internal data structures to 
  persistence and the references to recently added blobs are accounted.

blobTrackSnapshotIntervalInSecs (long) - 43200
: The blob ids cached/tracked locally are synchronized with the DataStore at this interval. Any additions and 
deletions will be visible to other cluster nodes or repositories connected to the shared DatStore after this. This 
should be less than the blobGcMaxAgeInSecs parameter above and the frequency of blob gc. See [Blob 
tracker][blobtracker].

<a name="document-node-store"></a>
#### DocumentNodeStore

_PID `org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService`_

mongouri
: Default - mongodb://localhost:27017
: Specifies the [MongoURI][1] required to connect to Mongo Database

db
: Default - oak
: Name of the database in Mongo

socketKeepAlive
: Default - false
: Enables socket keep-alive for MongoDB connections
: Since 1.8.0, 1.6.2, 1.4.16

cache
: Default - 256
: Cache size in MB. This is distributed among various caches used in DocumentNodeStore

changesSize
: Default - 256
: Size in MB of capped collection used in Mongo for caching the diff output.

customBlobStore
: Default false
: Boolean value indicating that custom `BlobStore` to use. By default it uses `MongoBlobStore`.

maxReplicationLagInSecs
: Default 21600 (6 hours)
: Determines the duration beyond which it can be safely assumed that state on secondary would be consistent 
  with primary and its safe to read from them. (See [OAK-1645][OAK-1645])
  
blobGcMaxAgeInSecs
: Default 86400 (24 hrs)
: Blob Garbage Collector (GC) logic would only consider those blobs for GC which are not accessed recently 
  (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). For example as per default only those blobs which have
  been created 24 hrs ago would be considered for GC. It is strongly advised to not set this property to a very low 
  value of say a few minutes but only set it to a hour at a minimum. This is to ensure that the NodeStore(s) have had the 
  time to flush out its internal data structures to persistence and the references to recently added blobs are 
  accounted.
  
versionGcMaxAgeInSecs
: Default 86400 (24 hrs)
: Oak uses MVCC model to store the data. So each update to a node results in new version getting created. This 
  duration controls how much old revision data should be kept. For example if a node is deleted at time T1 then its
  content would only be marked deleted at revision for T1 but its content would not be removed. Only when a Revision
  GC is run then its content would removed and that too only after (currentTime -T1 > versionGcMaxAgeInSecs)

versionGCExpression
: Default ""
: A cron expression that defines when the Revision GC is scheduled. If this
  configuration entry is left empty, the default behaviour depends on the
  `documentStoreType`. For `MONGO` the default is to schedule a run every five
  seconds (also known as Continuous Revision Garbage Collection). For `RDB` the
  default is no scheduled GC. It must be enabled explicitly with a cron 
  expression. E.g. the following expression triggers a GC run every night at
  2 AM: `0 0 2 * * ?`.
: Since 1.7.11

versionGCTimeLimitInSecs
: Default 10800
: A Revision GC run is canceled after this number of seconds. The default is
  three hours.
: Since 1.7.11

journalGCMaxAge
: Default 86400000 (24 hrs, was 6 hrs until 1.7.4)
: Journal entries older than `journalGCMaxAge` can be removed by the journal
  garbage collector. The maximum age is specified in milliseconds.
: Since 1.0.19, 1.2.3, 1.4

journalGCInterval
: Default 300000 (5 min)
: The interval in milliseconds with which the journal garbage collector removes
  old journal entries.
: Since 1.0.19, 1.2.3, 1.4

blobCacheSize
: Default 16 (MB)
: DocumentNodeStore when running with Mongo would use `MongoBlobStore` by default unless a custom `BlobStore` is 
  configured. In such scenario the size of in memory cache for the frequently used blobs can be configured via 
  `blobCacheSize`. 
  
persistentCache
: Default "cache,binary=0" (prior to 1.6, the persistent cache was disabled by default)
: The [persistent cache][persistent-cache], which is stored in the local file system.

<a name="cache-allocation"></a>
nodeCachePercentage
: Default 35 (was 25 until 1.5.14)
: Percentage of `cache` allocated for `nodeCache`. See [Caching][doc-cache]

prevDocCachePercentage
: Default 4
: Percentage of `cache` allocated for `prevDocCache`. See [Caching][doc-cache]
: Since 1.3.15

childrenCachePercentage
: Default 15 (was 10 until 1.5.14)
: Percentage of `cache` allocated for `childrenCache`. See [Caching][doc-cache]

diffCachePercentage
: Default 30 (was 5 until 1.5.14)
: Percentage of `cache` allocated for `diffCache`. See [Caching][doc-cache]

docChildrenCachePercentage
: Default 3
: Percentage of `cache` allocated for `docChildrenCache`. See [Caching][doc-cache]
: Removed since 1.5.6

cacheSegmentCount
: Default 16
: The number of segments in the LIRS cache
: Since 1.0.15, 1.2.3, 1.3.0

cacheStackMoveDistance
: Default 16
: The delay to move entries to the head of the queue in the LIRS cache
: Since 1.0.15, 1.2.3, 1.3.0

sharedDSRepoId (From Oak 1.2.11, valid for Oak 1.2.x)
: Default ""
: Custom SharedDataStore repositoryId. Used when custom blobstore configured. Should be unique among the repositories sharing the datastore.

blobTrackSnapshotIntervalInSecs
: Default 43200 (12 hrs)
: The blob ids cached/tracked locally are synchronized with the DataStore at this interval. Any additions and 
deletions will be visible to other cluster nodes or repositories connected to the shared DatStore after this. This 
should be less than the blobGcMaxAgeInSecs parameter above and the frequency of blob gc. See [Blob 
tracker][blobtracker].

Example config file

    mongouri=mongodb://localhost:27017
    db=oak
    
##### Mongo Configuration

All the configuration related to Mongo can be specified via [Mongo URI][1]

* **Authentication** - Username and password should be specified as part of uri e.g. the following 
  connects and logs in to the admin database as user sysop with the password moon:
  
        mongodb://sysop:moon@localhost
    
* **Read Preferences and Write Concern** - These also can be spcified as part of Mongo URI. Refer to 
  [Read Preference and Write Concern](./nodestore/documentmk.html#rw-preference) section for more details. For
  e.g. following would set _readPreference_ to _secondary_ and prefer replica with tag _dc:ny,rack:1_.
  It would also specify the write timeout to 10 sec
  
        mongodb://db1.example.net,db2.example.com?readPreference=secondary&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=&w=1&wtimeoutMS=10000    
  
One can also specify the connection pool size, socket timeout etc. For complete details about various 
possible option refer to [Mongo URI][1]  

<a name="config-blobstore"></a>  
### Configuring DataStore/BlobStore
BlobStores are used to store the binary content. Support for Jackrabbit 2 `DataStore` is also provided via
a `DataStoreBlobStore` wrapper. To use a specific BlobStore implementation following two steps need to be
performed

1. Configure NodeStore - NodeStore config need to be modified to enable use of custom BlobStore via setting
`customBlobStore` to true
2. Configure BlobStore - Create config for the required BlobStore by using the PID for that BlobStore.

Refer to [Config steps in Apache Sling](#config-sling) for an example on how to configure a `FileDataStore`
with `DocumentNodeStore`

#### Jackrabbit 2 - FileDataStore
Jackrabbit 2 [FileDataStore][2] can be configured via following _pid_

_PID `org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore`_

path
: Default - Not specified
: Path to the directory under which the files would be stored. 

minRecordLength
: Default - 100
: Size in bytes. Binary content less than `minRecordLength` would be inlined i.e. the data store id is
the data itself).

maxCachedBinarySize
: Default - 17408 (17 KB)
: Size in bytes. Binaries with size less than or equal to this size would be stored in in memory cache

cacheSizeInMB
: Default - 16
: Size in MB. In memory cache for storing small files whose size is less than `maxCachedBinarySize`. This
  helps in better performance when lots of small binaries are accessed frequently.

cacheSize
: Default - 0
: Size in bytes of FileDataStore cache. Cache is enabled when cacheSize > 0.  Default is disabled.

cachePath
: Default - ${home.dir}/repository/datastore
: Path of local file system cache


#### Jackrabbit 2 - S3DataStore

_PID `org.apache.jackrabbit.oak.plugins.blob.datastore.S3DataStore`_

maxCachedBinarySize
: Default - 17408 (17 KB)
: Size in bytes. Binaries with size less than or equal to this size would be stored in in memory cache

cacheSizeInMB
: Default - 16
: Size in MB. In memory cache for storing small files whose size is less than `maxCachedBinarySize`. This
  helps in better performance when lots of small binaries are accessed frequently.

#### Oak - SharedS3DataStore (Since Oak 1.2.0)

Supports shared S3 DataStore

_PID `org.apache.jackrabbit.oak.plugins.blob.datastore.SharedS3DataStore`_

maxCachedBinarySize
: Default - 17408 (17 KB)
: Size in bytes. Binaries with size less than or equal to this size would be stored in in memory cache

cacheSizeInMB
: Default - 16
: Size in MB. In memory cache for storing small files whose size is less than `maxCachedBinarySize`. This
  helps in better performance when lots of small binaries are accessed frequently.

#### Oak - AbstractSharedCachingDataStore (OAK 1.6.x)
All the above data stores enable local file system caching with the following parameters

* _PID `org.apache.jackrabbit.oak.plugins.blob.datastore.SharedS3DataStore`_
* _PID `org.apache.jackrabbit.oak.plugins.blob.datastore.S3DataStore`_
* _PID `org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore`_
* _PID `org.apache.jackrabbit.oak.plugins.blob.datastore.AzureDataStore`_

cacheSize
: Default - 68719476736
: Size in bytes of DataStore cache. Cache is disabled when cacheSize <= 0.

stagingSplitPercentage
: Default - 10
: Percentage of cache earmarked for asynchronous upload staging. The rest would be used for caching the downloaded 
  files.

uploadThreads
: Default - 10
: The number of background threads used for asynchronous uploads.

stagingPurgeInterval
: Default - 300
: Interval in seconds for the background purge job to clean up uploaded entries from the upload staging cache.

stagingRetryInterval
: Default - 600
: Interval in seconds for the background retry job for retrying previously failed asynchronous uploads.

### System properties and Framework properties

Following properties are supported by Oak. They are grouped in two parts _Stable_ and
_Experimental_. The stable properties would be supported in future version but the experimental properties
would _might_ not be supported in future versions

#### Stable 

oak.mongo.uri
: Type - System property and Framework Property
: Specifies the [MongoURI][1] required to connect to Mongo Database

oak.mongo.db
: Type - System property and Framework Property
: Name of the database in Mongo

#### Experimental

### Configuration Steps for Apache Sling

The OSGi Configuration Admin service defines a mechanism for passing configuration settings
to an OSGi bundle. How a configuration is registered with the OSGi system varies depending
on the application.

<a name="config-sling"></a>
For example to configure `DocumentNodeStore` to use `FileDataStore` in Apache Sling

1. Create a config file with name _org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.cfg_ under
_${sling.home}/install_ folder with content

        #Mongo server details
        mongouri=mongodb://localhost:27017

        #Name of Mongo database to use
        db=aem-author

        #Store binaries in custom BlobStore e.g. FileDataStore
        customBlobStore=true
2. Create a config file with name _org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore.cfg_ under
   _${sling.home}/install_ folder with content

        #The minimum size of an object that should be stored in this data store.
        minRecordLength=4096

        #path to the DataStore
        path=./sling/repository/datastore

        #cache for storing small binaries in-memory
        cacheSizeInMB=128

#### Framework Properties vs OSGi Configuration
OSGi components can read config data from two sources.

1. ConfigurationAdmin - These are configured via placing the *.cfg files under _${sling.home}/install_ folder.
   These can also be modified at runtime via Felix WebConsole typically available at http://localhost:8080/system/console
2. Framework Properties - An OSGi framework can be configured to start with some framework properties. These
   properties cannot be changed at runtime. In Apache Sling these can be specified in _${sling.home}/sling.properties_
   or _${sling.home}/conf/sling.properties_

In Oak some of the config properties are also read from _framework properties_. If a value is specified
in both config file and framework properties then framework property takes precedence.

For example by default Sling sets **repository.home** to _${sling.home}/repository_. So this value
need not be specified in config files

[1]: http://docs.mongodb.org/manual/reference/connection-string/
[2]: http://jackrabbit.apache.org/api/2.4/org/apache/jackrabbit/core/data/FileDataStore.html
[OAK-1645]: https://issues.apache.org/jira/browse/OAK-1645
[doc-cache]: ./nodestore/documentmk.html#cache
[persistent-cache]: ./nodestore/persistent-cache.html
[blobtracker]: ./plugins/blobstore.html#blobid-tracker
