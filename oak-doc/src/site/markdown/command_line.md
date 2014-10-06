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

#### Using Memory Mapped Files

When using the Tar storage, some operations, for example backup and checkpoint, 
allow to use memory mapped files to reduce memory usage.
To use this option, set the system property `tar.memoryMapped` to `true`.
Example:

    java -Dtar.memoryMapped=true -mx4g -jar oak-run-*.jar checkpoint <repository>

