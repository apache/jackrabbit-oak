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

Getting Started
---------------

Many parts of Oak are still under construction, so it may be a bit difficult to find your way around
the codebase. The [README files](https://github.com/apache/jackrabbit-oak/blob/trunk/README.md),
this documentation, and the [Oak mailing list](http://oak.markmail.org/) archives are good places
to start learning about Oak.

There is also the [Jackrabbit 3 wiki page](http://wiki.apache.org/jackrabbit/Jackrabbit%203), which
is mostly outdated though and should only be consulted for historical research.

To get started developing Oak, checkout the sources from [svn](https://svn.apache.org/repos/asf/jackrabbit/oak/trunk), 
or [fork them](https://github.com/apache/jackrabbit-oak) on GitHub. Then build the latest sources with Maven 3 and 
Java 7 (or higher) like this:

    mvn clean install

To enable all integration tests, including the JCR TCK, use:

    mvn clean install -PintegrationTesting

Before committing changes or submitting a patch, please make sure that the above integration testing
build passes without errors. If you like, you can enable integration tests by default by setting the
`OAK_INTEGRATION_TESTING` environment variable.

MongoDB integration
-------------------

Parts of the Oak build expects a MongoDB instance to be available for testing. By default a MongoDB
instance running on localhost is expected, and the relevant tests are simply skipped if such an
instance is not found. You can also configure the build to use custom MongoDB settings with the
following properties (shown with their default values):

    -Dmongo.host=127.0.0.1
    -Dmongo.port=27017
    -Dmongo.db=MongoMKDB
    -Dmongo.db2=MongoMKDB2

Note that the configured test databases will be *dropped* by the test cases.

Components
----------

The build consists of the following components:

  - oak-parent        - parent POM
  - oak-doc           - Oak documentation
  - oak-commons       - shared utility code
  
  - [oak-api][1]      - Oak repository API   
  - oak-core          - Oak repository implementation
  - oak-core-spi      - Oak repository extensions
  
  - oak-jcr           - JCR binding for the Oak repository

  - oak-solr-core     - Apache Solr indexing and search
  - oak-solr-osgi
  - oak-lucene        - Lucene-based query index
  
  - oak-auth-external - External authentication support
  - oak-auth-ldap     - LDAP implementation of external authentication
  - oak-authorization-cug - Authorization model for closed user groups (CUG)
     
  - oak-blob          - Oak Blob Store API
  - oak-blob-plugins  - Oak Blob : Extensions and Base Implementations
  - oak-blob-cloud    - S3 cloud blob store implementation
  - oak-blob-cloud-azure - Azure cloud blob store implementation 

  - oak-store-spi     - Oak NodeStore and Commit SPI 
  - oak-segment-tar   - TarMK API and nodestore implementation
  - oak-store-document - Oak DocumentNodeStore implementation on MongoDB and RDB

  - oak-upgrade       - tooling for upgrading Jackrabbit repositories to Oak and sidegrading Oak to Oak
   
  - oak-run           - runnable jar packaging
  - oak-run-commons   - utilities shared by oak-run and oak-benchmarks
  
  - oak-benchmarks    - benchmark tests
  - oak-it            - integration tests
  - oak-it-osgi       - integration tests for OSGi
  
  - oak-http          - HTTP binding for Oak
  - oak-pojosr  
  
  - [oak-exercise][3] - Oak training material
  - oak-examples      - Oak examples (webapp and standalone)


Archive
-------

The following components have been moved to the Jackrabbit Attic:

  - oak-mk-api        - MicroKernel API (_deprecated_, OAK-2701)
  - oak-mk            - MicroKernel implementation (see OAK-2702)
  - oak-mk-remote     - MicroKernel remoting  (see [OAK-2693][2])
  - oak-it/mk         - integration tests for MicroKernel
  - oak-remote        - Oak Remote API (see OAK-7035)



  [1]: https://github.com/apache/jackrabbit-oak/blob/trunk/oak-core/README.md
  [2]: https://issues.apache.org/jira/browse/OAK-2693
  [3]: https://github.com/apache/jackrabbit-oak/blob/trunk/oak-exercise/README.md

