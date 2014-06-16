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
into blocks of 2 MB. This is similar to how DropBox works internally:
http://serverfault.com/questions/52861/how-does-dropbox-version-upload-large-files - 
blocks are processed in memory so that temp files are never
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

