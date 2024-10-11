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

# Testing using different NodeStore fixtures

In Jackrabbit Oak unit and integration tests are run using different fixtures which represent configurations of the NodeStore underlying storage for the content repository.
This ensures that there is a fixed environment in which tests are run and the results are consistent and repeatable across different runs.

The default fixture used when running `mvn clean install` is `SEGMENT_TAR` which uses the SegmentNodeStore with the local storage with TAR files. 

To run the tests using a different fixture, you can use the following command: `mvn clean install -Dnsfixtures=<FIXTURE>`.
You can also update the `pom.xml` file to use a different fixture by updating the `fixture` property in the `oak-parent/pom.xml` file. If you want to run using multiple fixtures, provide a comma separated list of fixtures.

These are the possible fixtures that can be used:
- `SEGMENT_TAR` - Uses the SegmentNodeStore with the local storage with TAR files.
- `SEGMENT_AWS` - Uses the SegmentNodeStore with the AWS backend.
- `SEGMENT_AZURE` - Uses the SegmentNodeStore with the Azure backend.
- `DOCUMENT_NS` - Uses the DocumentNodeStore with MongoDB as the underlying storage engine. This requires a running instance of MongoDB. 
- `DOCUMENT_RDB` - Uses the DocumentNodeStore with the relational databases such as PostgreSQL, MySQL as underlying storage engine. Note that in general, this requires adding JDBC drivers to the class path (see the `rdb-` profiles in the parent POM), and running the specified database engine in the local network. A notable exception is Derby, which uses the local file system. Also, JDBC URL, user name and password need to be specified as system properties `rdb.jdbc-url`, `rdb.jdbc-user` (default: "sa") and `rdb.jdbc-passwd` (default: empty).
- `DOCUMENT_MEM` - Uses the DocumentNodeStore with the in-memory backend. This can be used for running tests that do not require persistence.
- `MEMORY_NS` - Uses the MemoryNodeStore with the in-memory backend which is more lightweight than DocumentNodeStore but is not as feature-rich.
- `COMPOSITE_SEGMENT` - Uses the CompositeNodeStore which allows combining multiple underlying segment stores into a single logical repository. It is useful in scenarios where different parts of the content tree have different storage requirements. For example, frequently accessed content can be stored in a high-performance segment store, while less frequently accessed content can be stored in a more cost-effective storage backend.
- `COMPOSITE_MEM` - Uses the CompositeNodeStore setup which is combining multiple in-memory node stores. 
- `COW_DOCUMENT` - Uses the COWNodeStore setup in which all the changes are stored in a volatile storage. 

Note that the [Jenkins job](https://github.com/apache/jackrabbit-oak/blob/trunk/Jenkinsfile) that is run as part of the CI flow use both `SEGMENT_TAR` and `DOCUMENT_NS` fixtures to run the tests.

## Special case: DocumentNodeStore tests

For these tests, the base test class attempts to instantiate _all_ potential implementations. Thus, the tests usually run minimally with `DOCUMENT_NS` and `DOCUMENT_RDB` for H2DB (which is embeded).

## Special case: MongoDB tests

The test fixture for MongoDB attempts to access a local MongoDB instance, and otherwise attempts to start a Docker image (the latter case will make things slower due to startup times). If both fail, tests will not run.

## Special case: RDB tests

When adding database drivers using the test profiles, such as `-Prdb-derby`, size checks on the "oak-run" project may fail. As a workaround, just specify a larger limit, such as with `-Dmax.jar.size=200000000`.

Example:

`mvn clean install -PintegrationTesting -Prdb-derby -Dnsfixtures=DOCUMENT_RDB "-Drdb.jdbc-url=jdbc:derby:./target/derby-test;create=true" -Dmax.jar.size=200000000`

Note that Derby supports a local filesytem-based persistence, and thus does not require a standalone database server.

### Running a database in a Docker container

Even if a database is not installed locally, it still can be used using Docker.

#### PostgreSQL

Here's an example how to configure PostgreSQL using Docker (assuming Docker is already installed):

`docker run -p 8080:5432 --name oak-postgres -e POSTGRES_PASSWORD=geheim -e POSTGRES_DB=oak -d postgres:13-alpine`

This pulls the docker image "postgres:13-alpine", specifies a system password and a database to be created, and maps the default PostgreSQL port (5432) to the local port 8080.

To run tests, the following parameters would be used:

`mvn clean install -PintegrationTesting -Prdb-postgres -Dnsfixtures=DOCUMENT_RDB -Drdb.jdbc-url=jdbc:postgresql://localhost:8080/oak -Drdb.jdbc-user=postgres -Drdb.jdbc-passwd=geheim -Dmax.jar.size=200000000`

#### IBM DB2

Simlilarily, DB2 can be run using Docker:

`docker run -h db2server --name db2server --privileged=true -p 50000:50000 baedke/db2-community-oak:11.5.9.0`

The Docker image baedke/db2-community-oak:11.5.9.0 may also be built from the official DB2 community image `icr.io/db2_community/db2:11.5.9.0` using the Dockerfile https://github.com/apache/jackrabbit-oak/blob/trunk/oak-store-document/src/test/resources/db2/Dockerfile.

Note that on first run, initalization will be slow as the image does not come preconfigured.

To run tests, the following parameters would be used:

`mvn clean install -PintegrationTesting -Prdb-db2 -Dnsfixtures=DOCUMENT_RDB -Drdb.jdbc-url=jdbc:db2://localhost:50000/OAK -Drdb.jdbc-user=oak -Drdb.jdbc-passwd=geheim -Dmax.jar.size=200000000`
