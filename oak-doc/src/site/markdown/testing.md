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

Testing using different NodeStore fixtures
---------------

In Jackrabbit Oak unit and integration tests are run using different fixtures which represent configurations of the NodeStore underlying storage for the content repository.
This ensures that there is a fixed environment in which tests are run and the results are consistent and repeatable across different runs.

The default fixture used when running `mvn clean install` is `SEGMENT_TAR` which uses the SegmentNodeStore with the local storage with TAR files. 

To run the tests using a different fixture, you can use the following command: `mvn clean install -Dfixture=<FIXTURE>`. 
You can also update the `pom.xml` file to use a different fixture by updating the `fixture` property in the `oak-parent/pom.xml` file. If you want to run using multiple fixtures, provide a space separated list of fixtures.

These are the possible fixtures that can be used:
- `SEGMENT_TAR` - Uses the SegmentNodeStore with the local storage with TAR files.
- `SEGMENT_AWS` - Uses the SegmentNodeStore with the AWS backend.
- `SEGMENT_AZURE` - Uses the SegmentNodeStore with the Azure backend.
- `DOCUMENT_NS` - Uses the DocumentNodeStore with MongoDB as the underlying storage engine. This requires a running instance of MongoDB. 
- `DOCUMENT_RDB` - Uses the DocumentNodeStore with the relational databases such as PostgreSQL, MySQL as underlying storage engine.
- `DOCUMENT_MEM` - Uses the DocumentNodeStore with the in-memory backend. This can be used for running tests that do not require persistence.
- `MEMORY_NS` - Uses the MemoryNodeStore with the in-memory backend which is more lightweight than DocumentNodeStore but is not as feature-rich.
- `COMPOSITE_SEGMENT` - Uses the CompositeNodeStore which allows combining multiple underlying segment stores into a single logical repository. It is useful in scenarios where different parts of the content tree have different storage requirements. For example, frequently accessed content can be stored in a high-performance segment store, while less frequently accessed content can be stored in a more cost-effective storage backend.
- `COMPOSITE_MEM` - Uses the CompositeNodeStore setup which is combining multiple in-memory node stores. 
- `COW_DOCUMENT` - Uses the COWNodeStore setup in which all the changes are stored in a volatile storage. 

Note that the [Jenkins job](https://github.com/apache/jackrabbit-oak/blob/trunk/Jenkinsfile) that is run as part of the CI flow use both `SEGMENT_TAR` and `DOCUMENT_NS` fixtures to run the tests.



