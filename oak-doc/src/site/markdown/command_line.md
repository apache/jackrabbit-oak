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

## Command Line Tools

### Oak Run

This tool supports maintenance operations such as backup and compaction.

#### Compatibility

We recommend to use the same version of `oak-run` as the one of `oak-core`
deployed on your system.

#### Backup

To back up a repository, use:

    java -mx4g -jar oak-run-*.jar backup <repository> <backup>

When using the (default) Tar storage, the `<repository>` setting is the path to the directory
that contains the segment  (data*.tar) files.
The `<backup>` option is the target directory.

#### Checkpoint

To list the checkpoints of a repository, use:

    java -mx4g -jar oak-run-*.jar checkpoint <repository>

When using the (default) Tar storage, the `<repository>` setting is the path to the directory
that contains the segment  (data*.tar) files.

#### Compaction

The oak-run compact operation may be used to perform an offline compaction:

    java -mx4g -jar oak-run-*.jar compact <repository>

It makes sense to find and remove the old checkpoints, using the `checkpoint`
command described above.

#### Index Management

The oak-run index operation is used to [manage indexes](./query/index-management.html).

#### System properties supported by the oak-run

When performing operations on the SegmentNodeStore (eg. backup, checkpoint, compact), it's
possible to tweak various system properties to get the optimal performance. These are:

* `cache` - cache size for the SegmentNodeStore (default: `256`),
* `compaction-progress-log` - how many entries should be compacted between log messages (default: `150000`),
* `update.limit` - threshold for the flush of a temporary transaction to disk (default: `10000`),
* `tar.memoryMapped` - allow to use memory mapped files to reduce memory usage (default: `false`),

Example:

    java -Dtar.memoryMapped=true -mx4g -jar oak-run-*.jar checkpoint <repository>

#### Analyzing Thread Dumps ####

To analyze thread dumps, filter out idle threads, and get profiling data, use the following, there the directory contains thread dump files:

    java -jar oak-run-*.jar threaddump --filter --profile <directory>


### MongoDB Shell with oak-mongo.js

MongoDB comes with a command line shell that can be extended with custom
JavaScript. Oak provides a oak-mongo.js file with MongoMK specific extensions.
You first need to download the JavaScript file and then start the MongoDB shell
with the file.

    wget https://s.apache.org/oak-mongo.js
    mongo --shell oak-mongo.js
    
Now the MongoDB shell has an `oak` namespace with [additional functions](oak-mongo-js/oak.html).
