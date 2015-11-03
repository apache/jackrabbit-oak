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

## The Blob Store

The Oak BlobStore is similar to the Jackrabbit 2.x DataStore. However, there are a 
few minor problems the BlobStore tries to address.
Because, for the Jackrabbit DataStore:

* a temporary file is created when adding a large binary, 
  even if the binary already exists

* sharding is slow and complicated because the hash needs to be calculated
  first, before the binary is stored in the target shard (the FileDataStore
  still doesn't support sharding the directory currently)

* file handles are kept open until the consumer is done reading, which
  complicates the code, and we could potentially get "too many open files"
  when the consumer doesn't close the stream

* for database based data stores, there is a similar (even worse) problem
  that streams are kept open, which means we need to use connection pooling,
  and if the user doesn't close the stream we could run out of connections

* for database based data stores, for some databases (MySQL), binaries are
  fully read in memory, which results in out-of-memory

* binaries that are similar are always stored separately no matter what

Those problems are solved in Oak BlobStores, because binaries are split
into blocks of 2 MB. This is similar to how [DropBox works internally][1]. 
Blocks are processed in memory so that temp files are never
needed, and blocks are cached. File handles don't need to be kept open.
Sharding is trivial because each block is processed separately.

Binaries that are similar: in the BlobStore, currently, they are stored
separately except if some of the 2 MB blocks match. However, the algorithm
in the BlobStore would allow to re-use all matching parts, because in the
BlobStore, concatenating blob ids means concatenating the data.

Another change was that most DataStore implementations use SHA-1, while
the BlobStore uses SHA-256. Using SHA-256 will be a requirement at some
point, see also http://en.wikipedia.org/wiki/SHA-2 "Federal agencies ... 
must use the SHA-2 family of hash functions for these applications
after 2010". This might affect some potential users.

### Support for Jackrabbit 2 DataStore

Jackrabbit 2 used [DataStore][2] to store blobs. Oak supports usage of such 
DataStore via `DataStoreBlobStore` wrapper. This allows usage of `FileDataStore` 
and `S3DataStore` with Oak NodeStore implementations. 

### NodeStore and BlobStore

Currently Oak provides two NodeStore implementations i.e. `SegmentNodeStore` and `DocumentNodeStore`.
Further Oak ships with multiple BlobStore implementations

1. `FileBlobStore` - Stores the file contents in chunks on file system
2. `MongoBlobStore` - Stores the file content in chunks in Mongo. Typically used with
   `DocumentNodeStore` when running on Mongo by default
3. `FileDataStore` (with wrapper) - Stores the file on file system without breaking it into
   chunks. Mostly used when blobs have to shared between multiple repositories. Also used by 
   default when migrating Jackrabbit 2 repositories to Oak
4. `S3DataStore` (with wrapper) - Stores the file in Amazon S3
5. `RDBBlobStore` - Store the file contents in chunks in a relational databases. Typically used with
   `DocumentNodeStore`when using a relational DB persistence

In addition there are some more implementations which are considered **experimental**

1. `CloudBlobStore` - Stores the file file chunks in cloud storage using the [JClouds BlobStore API][3].
2. `MongoGridFSBlobStore` - Stores the file chunks in Mongo using GridFS support


Depending on NodeStore type and usage requirement these can be configured to use 
a particular BlobStore implementation. For OSGi env refer to [Configuring DataStore/BlobStore]
(../osgi_config.html#config-blobstore)

#### SegmentNodeStore

By default SegmentNodeStore does not require a BlobStore. Instead the binary content is
directly stored as part of segment blob itself. Depending on requirements one of the following 
can be used  
 
* FileDataStore - This should be used if the blobs/binaries have to be shared between multiple
  repositories. This would also be used when a JR2 repository is migrated to Oak
* S3DataStore - This should be used when binaries are stored in Amazon S3 

#### DocumentNodeStore

By default DocumentNodeStore when running on Mongo uses `MongoBlobStore`. Depending on requirements 
one of the following can be used  
                  
* MongoBlobStore - Used by default
* FileDataStore - This should be used if the binaries have to be stored on the file system. This 
  would also be used when a JR2 repository is migrated to Oak
* S3DataStore - This should be used when binaries are stored in Amazon S3. Typically used when running
  in Amazon AWS

### Blob Garbage Collection

Blob Garbage Collection(GC) is applicable for the following blob stores:

* DocumentNodeStore 
    * MongoBlobStore/RDBBlobStore (Default blob stores for RDB & Mongo)
    * FileDataStore
    * S3DataStore
    * SharedS3DataStore (since Oak 1.2.0)
    
* SegmentNodeStore 
    * FileDataStore
    * S3DataStore
    * SharedS3DataStore (since Oak 1.2.0)

Oak implements a Mark and Sweep based Garbage Collection logic. 
 
1. Mark Phase - In this phase the binary references are marked in both
   BlobStore and NodeStore
    1. Mark BlobStore - GC logic would make a record of all the blobs
       present in the BlobStore. 
    2. Mark NodeStore - GC logic would make a record of all the blob
       references which are referred by any node present in NodeStore.
       Note that any blob references from old revisions of node would also be 
       considered as a valid references. 
2. Sweep Phase - In this phase all blob references form Mark BlobStore phase 
    which were not found in Mark NodeStore part would considered as GC candidates.
    It would only delete blobs which are older than a specified time interval 
    (last modified say 24 hrs (default) ago).

The garbage collection can be triggered by calling:

* `MarkSweepGarbageCollector#collectGarbage()` (Oak 1.0.x)
* `MarkSweepGarbageCollector#collectGarbage(false)` (Oak 1.2.x)
 
#### Shared DataStore Blob Garbage Collection (Since 1.2.0)

On start of a repository configured with a shared DataStore, a unique repository id is registered. 
In the DataStore this repository id is registered as an empty file with the format `repository-[repository-id]` 
(e.g. repository-988373a0-3efb-451e-ab4c-f7e794189273).
The high-level process for garbage collection is still the same as described above. 
But to support blob garbage collection in a shared DataStore the Mark and Sweep phase can be
run independently.

The details of the process are as follows:

* The Mark NodeStore phase has to be executed for each of the repositories sharing the DataStore.
    * This can be executed by running `MarkSweepGarbageCollector#collectGarbage(true)`, where true indicates mark only.
    * All the references are collected in the DataStore in a file with the format `references-[repository-id]` 
    (e.g. references-988373a0-3efb-451e-ab4c-f7e794189273).
* One completion of the above process on all repositories, the sweep phase needs to be triggered.
    * This can be executed by running `MarkSweepGarbageCollector#collectGarbage(false)` on one of the repositories, 
    where false indicates to run sweep also. 
    * The sweep process checks for availability of the references file from all registered repositories and aborts otherwise.
    * All the references available are collected.
    * All the blobs available in the DataStore are collected and deletion candidates identified by calculating all the 
    blobs available not appearing in the blobs referenced. Only blobs older than a specified time interval from the 
    earliest available references file are deleted. (last modified say 24 hrs (default)).
    
The shared DataStore garbage collection is applicable for the following DataStore(s):

* FileDataStore
* SharedS3DataStore - Extends the S3DataStore to enable sharing of the data store with
                        multiple repositories                        
 

[1]: http://serverfault.com/questions/52861/how-does-dropbox-version-upload-large-files
[2]: http://wiki.apache.org/jackrabbit/DataStore
[3]: http://jclouds.apache.org/start/blobstore/
