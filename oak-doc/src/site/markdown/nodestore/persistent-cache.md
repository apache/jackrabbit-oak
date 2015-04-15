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

## Persistent Cache

The document storage optionally uses the persistent cache.
The cache acts like an in-memory cache for old revisions, 
but in addition to keeping the most recently used nodes in memory, 
it also stores them to disk. That way, many reads from the storage backend (for example MongoDB)
are replaced by reads from the local disk. This is specially useful if reads from the local disk
are faster than reads from the storage backend. In addition to that, the persistent cache reduces 
the load on the storage backend.

### OSGi Configuration

The OSGi configuration of the persistent cache is:

    org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService
        persistentCache=./cache

This will enable the persistent cache, and set the configuration option to "./cache".

### Configuration Options 

The persistent cache configuration setting is string with a number of comma separated elements. 
The first element is the directory where the cache is stored. Example:

    ./cache

In this case, the data is stored in the directory "cache", 
relative to the current working directory.
By default, there are at most two files (two generations) with the name "cache-x.data", 
where x is an incrementing number (0, 1,...). 
A file is at most 1 GB by default. 
If the file is larger, the next file is created, and if there are more than two files, 
the oldest one is removed.
If data from the older file is accessed, it is copied to the latest file.
That way, data that is not recently read will eventually be removed.

The following other configuration options are available:

* Size. A file is at most 1 GB by default. To change maximum size of a file,
use "size=x", where x is the size in MB.

* Binary caching. When using the BlobStore, 
binaries smaller than 1 MB are stored in the persistent cache by default. 
The maximum size can be changed using the setting "binary=x",
where x is the size in bytes. To disable the binary cache, use "binary=0".

* Node caching. By default, nodes at all revisions are cached.
To disable this option, use "-nodes".

* Children caching. By default, the list of children of a node is cached.
To disable this option, use "-children".

* Diff caching. By default, the list of differences between two
revisions is cached. To disable this option, use "-diff".

* Compaction. By default, when a cache file is closed, 
it is compacted and compressed (at a rate of around 100 MB per second).
That way, the disk space is used more efficiently. 
To disable this option, use "-compact".

* Compression. By default, the cache is compressed, saving space. 
To disable this option, use "-compress".

Those setting can be appended to the persistent cache configuration string.
An example configuration is:

    ./cache,size=2048,binary=0,-compact,-compress

### Dependencies

Internally, the persistent cache uses a key-value store 
(basically a java.util.Map), which is persisted to disk.
The current key-value store backend is the [H2 MVStore](http://www.h2database.com/html/mvstore.html).
This library is only needed if the persistent cache is configured.
Version 1.4.185 or newer is needed.

    <dependency>
        <groupId>com.h2database</groupId>
        <artifactId>h2-mvstore</artifactId>
        <version>1.4.185</version>
    </dependency>



