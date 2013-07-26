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

This module contains performance tests for microkernel instances.

Usage
-----

The tests can be launched locally by calling directly the remote profile from the pom file using the
following commands:

    mvn clean test -Premote -Poakmk   - for launching the tests against the oak microkernel or
    mvn clean test -Premote -Pmongomk - for launching the tests against the mongodb microkernel.
        
More than that the tests can be launched remotely, for example on an mongodb cluster. In this case
the pom file uploads the tests to the remote machine, runs them and collects the results. Use the
following commands to remotely run the tests:
    
    mvn clean process-test-classes -Plocal [-Pmongomk | Poakmk]  \
    -Dremotehost=<remotehost> -Dpass=<ssh-password for the remote machine>

The test environment (mongodb cluster)
--------------------------------------
    
For measuring the performance of the microkernel, I created a mongodb cluster in amazon cloud with
the following components:

* 2 shards ( in the same time, replica sets) - each of it with 2 nodes installed on different
  platforms
* 3 configuration servers all of them installed on one platform
* 1 mongos instance
        
The sharding is enabled and I'm using the following sharding key : {"path" :1, "revId":1}. I
changed also the chunk size from 64 to 8 (MB).


Tests
-----

All the tests bellow were launched remotely on amazon cloud for both types of microkernel. The tests
are all executed on the platform where the mongos instance is installed.
        
* `MkAddNodesDifferentStructuresTest.testWriteNodesSameLevel`: Creates 100000 nodes, all having the
  same parent node. All the nodes are added in a single microkernel commit.
* `MkAddNodesDifferentStructuresTest.testWriteNodes10Children`: Creates 100000 nodes, in a pyramid
  tree structure. All of the nodes have 10 children. All the nodes are added in a single microkernel
  commit.
* `MkAddNodesDifferentStructuresTest.testWriteNodes100Children`: Creates 100000 nodes, in a pyramid
  tree structure. All of the nodes have 100 children. All the nodes are added in a single
  microkernel commit.
* `MkAddNodesDifferentStructuresTest.testWriteNodes1000Children`: Creates 100000 nodes, in a pyramid
  tree structure. All of the nodes have 1000 children.All the nodes are added in a single
  microkernel commit.
* `MkAddNodesDifferentStructuresTest.testWriteNodes1Child`: Creates 100 nodes, each node has only
  one child, so each of it is on a different tree level. All the nodes are added in a single
  microkernel commit.
* `MkAddNodesMultipleCommitsTest.testWriteNodesAllNodes1Commit`: Create 10000 nodes, in a pyramid
  tree structure. All of the nodes have 100 children. Only one microkernel commit is performed for
  adding the nodes.
* `MkAddNodesMultipleCommitsTest.testWriteNodes50NodesPerCommit`: Create 10000 nodes, in a pyramid
  tree structure. All of the nodes have 100 children. The nodes are added in chunks of 50 nodes per
  commit.
* `MkAddNodesMultipleCommitsTest.testWriteNodes1000NodesPerCommit`: Create 10000 nodes, in a pyramid
  tree structure. All of the nodes have 100 children. The nodes are added in chunks of 1000 nodes
  per commit.
* `MkAddNodesMultipleCommitsTest.testWriteNodes1NodePerCommit`: Create 10000 nodes, in a pyramid
  tree structure. All of the nodes have 100 children. Each node is individually added.
* `MKAddNodesRelativePathTest.testWriteNodesSameLevel`: Create 1000 nodes, all on the same level.
  Each node is individually added (in a separate commit). Each node is added using the relative
  paths in the microkernel commit method.
* `MKAddNodesRelativePathTest.testWriteNodes10Children`: Create 1000 nodes, each of them having
  exactly 10 children. Each node is individually added (in a separate commit). Each node is added
  using the relative paths in the microkernel commit method.
* `MKAddNodesRelativePathTest.testWriteNodes100Children`: Create 1000 nodes, each of them having
  exactly 100 children. Each node is individually added (in a separate commit). Each node is added
  using the relative paths in the microkernel commit method.
