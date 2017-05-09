Jackrabbit Oak - the next generation content repository
=======================================================

Jackrabbit Oak is a scalable, high-performance hierarchical content
repository designed for use as the foundation of modern world-class
web sites and other demanding content applications.

The Oak effort is a part of the Apache Jackrabbit project.
Apache Jackrabbit is a project of the Apache Software Foundation.

Getting Started
---------------

To get started with Oak, build the latest sources with
Maven 3 and Java 7 (or higher) like this:

    mvn clean install

To enable all integration tests, including the JCR TCK, use:

    mvn clean install -PintegrationTesting

Before committing changes or submitting a patch, please make sure that
the above integration testing build passes without errors. If you like,
you can enable integration tests by default by setting the
`OAK_INTEGRATION_TESTING` environment variable.

MongoDB integration
-------------------

Parts of the Oak build expects a MongoDB instance to be available for
testing. By default a MongoDB instance running on localhost is expected,
and the relevant tests are simply skipped if such an instance is not found.
You can also configure the build to use custom MongoDB settings with the
following properties (shown with their default values):

    -Dmongo.host=127.0.0.1
    -Dmongo.port=27017
    -Dmongo.db=MongoMKDB
    -Dmongo.db2=MongoMKDB2

Note that the configured test databases will be *dropped* by the test cases.

Components
----------

The build consists of the following main components:

  - oak-parent        - parent POM
  - oak-doc           - Oak documentation
  - oak-commons       - shared utility code
  - [oak-core][1]     - Oak repository API and implementation
  - oak-jcr           - JCR binding for the Oak repository
  - oak-sling         - integration with Apache Sling
  - oak-solr-core     - Apache Solr indexing and search
  - oak-solr-embedded - Apache Solr on an embedded Solr instance
  - oak-solr-remote   - Apache Solr on an remote (HTTP) Solr instance
  - oak-http          - HTTP binding for Oak
  - oak-lucene        - Lucene-based query index
  - oak-run           - runnable jar packaging
  - oak-pojosr        - integration with PojoSR
  - oak-segment-tar   - TarMK API and implementation
  - oak-upgrade       - tooling for upgrading Jackrabbit repositories to Oak
  - oak-it            - integration tests
    - oak-it/osgi     - integration tests for OSGi
  - [oak-exercise][2] - Oak training material

  [1]: oak-core/README.md
  [2]: oak-exercise/README.md

Archive
-------

The following components have been moved to the Jackrabbit Attic:

  - oak-mk-api        - MicroKernel API (_deprecated_, see OAK-2701)
  - oak-mk            - MicroKernel implementation  (see OAK-2702)
  - oak-mk-remote     - MicroKernel remoting  (see OAK-2693)
  - oak-it/mk         - integration tests for MicroKernel



License
-------

(see [LICENSE.txt](LICENSE.txt) for full license details)

Collective work: Copyright 2014 The Apache Software Foundation.

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
