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

The default OSGi configuration of the persistent cache is:

    org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService
        persistentCache="cache,binary\=0"

Oak version up to 1.4 have the persistent cache disabled by default, which
is equivalent with a configuration entry set to an empty String. Starting with
Oak 1.6, the persistent cache is enabled by default and can be disabled by
setting the configuration entry to `"-"`.

### Configuration Options

The persistent cache configuration setting is string with a number of comma separated elements. 
The first element is the directory where the cache is stored. Example:

    "cache"

In this case, the data is stored in the directory "cache", 
relative to the `repository.home` directory. If no repository home directory is
configured, the directory is relative to the current working directory. Oak
versions prior to 1.6 always resolve to the current working directory and ignore
the `repository.home` configuration.
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

* Compaction. The cache file can be compacted and compressed (at a rate of
around 100 MB per second) when it is closed.
That way, the disk space is used more efficiently. 
To enable this option, use "+compact".
(Please note this feature was enabled by default in versions 1.2.1, 1.0.13, and older.)

* Compression. By default, the cache is compressed, saving space. 
To disable this option, use "-compress".

Those setting can be appended to the persistent cache configuration string.
An example configuration is:

    "cache,size\=2048,binary\=0,-compact,-compress"

To disable the persistent cache entirely in Oak 1.6 and newer, use the following
configuration:

    org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService
        persistentCache="-"

Up to Oak version 1.4, either omit the persistentCache entry or set it to an
empty String to disable the persistent cache.

### Journal cache

Since Oak 1.6.

Diff cache entries can also are stored in a separate persistent cache and
configured independently if needed. This can be done in the OSGi
configuration like in the following example:

    org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService
        persistentCache="cache,size\=2048"
        journalCache="diff-cache,size\=1024"

The configuration options are the same as for the `persistentCache`, but options
unrelated to the diff cache type are ignored. The default configuration is
`journalCache="diff-cache"` and can be disabled the same way as the
regular persistent cache with a dash: `journalCache="-"`.

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



