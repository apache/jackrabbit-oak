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

Oak Mongot search connector
===========================

This module implements a native Oak search backend with index type `mongot`.
It uses Oak's public search and query-index SPIs, the MongoDB Java driver, and
Mongot. It does not use Elasticsearch code, clients, protocols,
containers, or dependencies. The complete analyzer-compatibility suite
requires the custom Mongot build pinned to the provisioned Atlas cluster. That
build exposes Lucene primitives missing from the current public Search analyzer
schema; the connector still accesses MongoDB only through the Java driver.

The connector has two paths:

    Oak content update
      -> MongotIndexEditorProvider
      -> typed MongoDB documents
      -> one collection and Search index per Oak index definition

    JCR-SQL2 or XPath
      -> MongotIndexProvider
      -> structured aggregation pipeline
      -> Mongot search / MongoDB match and sort
      -> Oak cursor

The detailed, evidence-backed query verdicts are in
[COMPATIBILITY.md](COMPATIBILITY.md).

What is implemented
-------------------

- deterministic collection and Search-index names;
- typed, analyzed, ordered, facet, relative-full-text, suggestion, spellcheck,
  structural, node-type, and dynamic-boost document fields;
- upsert, exact delete, subtree delete, and idempotent Search-index creation;
- a remote no-op `IndexImporterProvider` matching the Elasticsearch import
  model and the portable Oak reindex-state contract;
- async Oak indexing through the shared full-text editor SPI;
- synchronous `sync-mode=rt` commits that wait until every affected Search
  index observes the commit marker;
- blue/green collection generations for reindex, with an atomic Oak-definition
  pointer handoff only after the replacement Search index is queryable;
- property- and node-scoped terms, phrases, Boolean expressions, negation,
  wildcard, fuzzy matching, and boost;
- property restrictions, paths, node types, mixins, ordering, pagination,
  relative properties, aggregates, functions, unions, and a child-node join;
- SQL2/XPath `rep:similar`, text more-like-this, current Search vector fields,
  reference-vector lookup, vector ranking, and lexical-plus-vector filtering;
- property-only queries that use ordinary MongoDB aggregation without a
  synthetic `$search` stage;
- lazy normal-result retrieval using a bounded Mongo aggregation cursor,
  `queryFetchSizes`, and `queryTimeoutMs`;
- repository property updates and removals, exact and subtree deletion,
  moves, aggregate refresh, and availability-preserving full reindex;
- Oak common-suite coverage for property indexes, full text, transformed path
  restrictions, nested/binary aggregates, relative-node aggregates, and
  aggregate refresh after updates;
- explicit planner decline for unsupported enriched result shapes;
- an OSGi Declarative Services component providing `Observer`,
  `QueryIndexProvider`, and `IndexEditorProvider` with `type=mongot`.

Prerequisites
-------------

- Java 17 or newer
- Maven 3
- an existing Atlas test cluster with the compatible custom Mongot build
  already pinned by MongoDB
- Atlas network access for the evaluator
- database credentials and the Atlas `mongodb+srv://` connection string

No Elasticsearch environment is required.

Build and test
--------------

From the repository root:

    mvn -pl oak-search-mongot -am -DskipTests install

Run against the existing Atlas cluster
--------------------------------------

The qualification suite creates and drops isolated `oak_search_test_*`
databases. The supplied test user must be permitted to create and drop those
databases and collections and to create, update, and drop their Search indexes.
Do not use credentials whose access is restricted to a pre-existing application
database.

Run the clean qualification gate with the Atlas SRV connection string:

    mvn -pl oak-search-mongot clean verify \
      -DmongoSearchConnectionString='mongodb+srv://<user>:<password>@<cluster>/'

Use the Atlas-generated SRV connection string without
`directConnection=true`.

The clean gate reports 468 tests, 0 failures, 0 errors, and 4 inherited Oak
skips, including the importer, commit, generation, and streaming contracts.
There are no connector-owned ignored tests or expected-failure lane.

Focused end-to-end query evidence:

    mvn -pl oak-search-mongot \
      -DmongoSearchConnectionString='mongodb+srv://<user>:<password>@<cluster>/' \
      -Dtest=MongotFullTextQueryTest,MongotCoreQueryCompatibilityTest,MongotAdvancedQueryCompatibilityTest \
      test

Focused mutation and reindex evidence:

    mvn -pl oak-search-mongot \
      -DmongoSearchConnectionString='mongodb+srv://<user>:<password>@<cluster>/' \
      -Dtest=MongotIndexWriterTest,MongotMutationCompatibilityTest,MongotReindexCompatibilityTest \
      test

Focused backend-neutral Oak contract evidence:

    mvn -pl oak-search-mongot \
      -DmongoSearchConnectionString='mongodb+srv://<user>:<password>@<cluster>/' \
      -Dtest=MongotPropertyIndexCommonTest,MongotFullTextIndexTest,MongotIndexPathRestrictionCommonTest,MongotIndexAggregationTest,MongotIndexAggregation2Test \
      test

Each test class uses an isolated database and removes only that database when
finished. Percent-encode special characters in the connection string. Do not
commit credentials or put them in shared command logs.

Build and install the connector bundle
--------------------------------------

The module is packaged as an OSGi bundle:

    mvn -pl oak-search-mongot -am -DskipTests package

The deployable artifact is:

    oak-search-mongot/target/oak-search-mongot-2.5-SNAPSHOT.jar

A prebuilt copy from this source tree is checked in at
[dist/oak-search-mongot-2.5-SNAPSHOT.jar](dist/oak-search-mongot-2.5-SNAPSHOT.jar).
Its bundle identity, checksum, and runtime requirements are recorded in the
[distribution notes](dist/README.md).

Install that bundle into a compatible Oak OSGi runtime, together with the
runtime bundles it imports. The current build expects the matching Oak API and
`oak-search` packages plus the MongoDB Java Driver 5.3 bundle set
(`mongodb-driver-sync`, `mongodb-driver-core`, `bson`, and
`bson-record-codec`); those dependencies are not embedded in the connector
JAR. Do not install the `*-tests.jar` artifact.

Configure the component PID after installing the bundle:

    org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexProviderService

Its properties are:

- `connectionString`: MongoDB connection string;
- `databaseName`: database holding the per-index collections.

For example:

    {
      "connectionString": "mongodb+srv://<user>:<password>@<cluster>/",
      "databaseName": "oak_search"
    }

Oak index definitions select the provider with `type=mongot`. Existing index
definitions should be copied or migrated to that type and reindexed before
queries are evaluated against the connector.

The generated bundle metadata registers `Observer`, `QueryIndexProvider`, and
`IndexEditorProvider`, plus the remote `IndexImporterProvider`, with the service
property `type=mongot`.

Evaluator delivery
------------------

This repository includes both the [connector source](src/main/java/) and a
prebuilt, version-matched
[OSGi bundle](dist/oak-search-mongot-2.5-SNAPSHOT.jar), so the initial
evaluation does not require packaging the connector. The source is available
for inspection and for rebuilding against a different Oak baseline.
MongoDB provisions the compatible custom Mongot build behind the Atlas cluster;
no Mongot binary or configuration is distributed to the evaluator. The source
qualification suite requires the repository and Atlas access. Runtime use
requires the connector bundle, the compatible Oak and MongoDB driver bundles,
the OSGi configuration, and the provisioned Atlas connection string.
