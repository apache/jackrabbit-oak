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
  
# Version Manager

The [`javax.jcr.version.VersionManager`][jcr-versionmanager.javadoc] is implemented in Oak in [`ReadWriteVersionManager`][oak-github-rwversionmanager]

TODO: Simple Versioning vs Full Versioning

## Representation in Repository

The root node of the versioned nodes is `/jcr:system/jcr:versionStorage`. 
Below that follows the 128 bit UUID which is separated into a 3-level node hierarchy where each level covers 8 bit (=2 hexadecimal digits).
This limits the number of sibling nodes to 256 in those levels. Below follows a node with the name being equal to the `jcr:versionableUuid` property of the node.

*This is not equal to is `jcr:uuid` property value!*

![Version Storage](../versionStorage.svg)

## Relevant node types

Node Type | Is Mixin | Description
--- | --- | ---
`mix:simpleVersionable` | yes | Used on nodes which should be versioned (prerequisite for checkin)
`mix:versionable` | yes | Used on nodes which should be versioned (prerequisite for checkin)
`nt:versionHistory` | no | Contains child nodes `jcr:versionLabels` and `jcr:rootVersion`
`nt:version` | no | Contains child node `jcr:frozenNode`
`nt:versionLabels` | no
`nt:frozenNode` | no | node type of the node containing the actual versioned node's content. 

[jcr-versionmanager.javadoc]: https://s.apache.org/jcr-2.0-javadoc/javax/jcr/version/VersionManager.html
[oak-github-rwversionmanager]: https://github.com/apache/jackrabbit-oak/blob/trunk/oak-jcr/src/main/java/org/apache/jackrabbit/oak/jcr/version/ReadWriteVersionManager.java