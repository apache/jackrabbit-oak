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

## Elastic Index

Oak supports Elasticsearch (Elastic for short) based indexes for both property constraint and full text constraints. 
Elastic indexes support similar features as [Lucene][lucene] indexes, 
however there are differences:

* The `type` is `elasticsearch`.
* The index definition needs to be under `/oak:index`.
  Other locations are not supported.
* The `async` property needs to be set to `elastic-async`. 
  Synchronous, `nrt` or other lanes are not supported.
  Indexes are updated asynchronously.
* `refresh` is ignored.
  Changes take effect immediately after changing them.
  Existing documents in Elasticsearch are not changed.
* Indexes are NOT automatically built when needed: 
  They can be built by setting the `reindex` property to `true` or by using the `oak-run` tool.
  We recommend to build them using the `oak-run` tool.
* `evaluatePathRestrictions` cannot be disabled. The parent paths are always indexed. Queries with path restrictions are 
  evaluated at index level when possible, otherwise they are evaluated at repository level.
* `codec` is ignored.
* `compatVersion` is ignored.
* `useIfExists` is ignored.
* `blobSize` is ignored.
* `name` is ignored.
* `indexPath` is ignored.
* `analyzers` support the Lucene configuration plus Elasticsearch specific [options][options]. Since Elasticsearch uses
  a more recent version of Lucene compared to the one in `oak-lucene` module, there might be differences in configuration options
  that could require changes when migrating from Lucene to Elasticsearch.
  The `HunspellStem` filter is not supported since dictionary files are required in the Elasticsearch cluster filesystem.
* `useInExcerpt` does not support regexp relative properties.
* For property definitions, `sync` and `unique` are ignored.
  Synchronous indexing, and enforcing uniqueness constraints is not currently supported in elastic indexes.
* The behavior for `dynamicBoost` is slightly different: 
  For Lucene indexes, boosting is done in indexing, while for Elastic it is done at query time.
* The behavior for `suggest` is slightly different:
  For Lucene indexes, the suggestor is updated every 10 minutes by default and the frequency
  can be changed by `suggestUpdateFrequencyMinutes` property in suggestion node under the index definition node.
  In Elastic indexes, there is no such delay and thus no need for the above config property. This is an improvement in ES over lucene.

[lucene]: https://jackrabbit.apache.org/oak/docs/query/lucene.html
[options]: https://www.elastic.co/guide/en/elasticsearch/reference/current/configure-text-analysis.html
