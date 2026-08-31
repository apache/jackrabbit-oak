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
* Unlike Lucene, where there is no limit on the number of indexed fields (which is a frequent source of issues since Lucene indexes are sparse and each unset field consumes no space, masking the real field count), Elasticsearch enforces a default limit of **1000 fields** per index
  (`index.mapping.total_fields.limit`, configurable via the `limitTotalFields` index definition property — increasing this value is not recommended as it can cause memory issues in Elasticsearch).
  Regex property definitions that match a large number of properties can easily exhaust this limit.
  In such cases, set `isFlattened` to `true` on the property definition: all properties matched by the regex are then stored under a single
  [flattened][flattened] field type in Elasticsearch instead of creating one field per matched property.
  Note that `isFlattened` is `false` by default, and is automatically overridden to `false` when `analyzed` is `true` on the same property
  definition, because flattened fields do not support full-text queries.
  Flattened fields come with the following limitations (see [Elasticsearch documentation][flattened]):
    * Only filtered queries are supported.
    * All values are treated as string keywords regardless of their actual type; in particular, `range` queries use lexicographic comparison, not numeric ordering.
    * Wildcard key references are not supported (e.g. `labels.time*`).
    * Highlighting is not supported.
* For property definitions, `sync` and `unique` are ignored.
  Synchronous indexing, and enforcing uniqueness constraints is not currently supported in elastic indexes.
* The behavior of `dynamicBoost` differs slightly between Lucene and Elasticsearch:  
  - **Lucene**: Boosting is applied at indexing time.  
  - **Elasticsearch**: Boosting is applied at query time.  

Full-text queries automatically use dynamically boosted values to match relevant results, but this behavior may not always be desirable.
To use these values exclusively for influencing relevance without affecting matching, configure the property definition as follows:
```json
{
  "dynamicBoost": true,
  "useInFullTextQuery": false
}
```
* The behavior of `suggest` is slightly different:
  For Lucene indexes, the suggestor is updated every 10 minutes by default and the frequency
  can be changed by `suggestUpdateFrequencyMinutes` property in suggestion node under the index definition node.
  In Elastic indexes, there is no such delay and thus no need for the above config property. This is an improvement in ES over lucene.

### Per-Property Analyzer Support

`@since Oak 2.5, [OAK-12360]`

Elasticsearch supports per-property analyzer configuration, allowing different properties to use different analyzers within the same index.
This feature works identically to the Lucene equivalent for syntax and backward compatibility: set the `analyzer` attribute on a property definition node to specify a custom analyzer.
For detailed configuration syntax and examples, see the [Lucene per-property analyzer documentation][lucene-per-property-analyzer].

**Known Limitations**

1. **Regular-expression properties:** Per-property analyzers are not supported for properties with `isRegexp = true`.
   The default analyzer is used with a logged warning, and indexing proceeds normally.

**Aggregated Fulltext Field Behavior**

Unlike Lucene, where the aggregated `:fulltext` field always uses the default analyzer regardless of per-property settings, Elasticsearch supports more nuanced behavior.
When a property is the sole analyzed `nodeScopeIndex` contributor (the only property with a custom analyzer; other properties, if any, use the default),
that property's custom analyzer applies to aggregated fulltext queries (`jcr:contains(., ...)`) as well as property-specific queries.
This is intentional and desirable: the query term is analyzed the same way the content was indexed, providing more accurate fulltext results.
When multiple properties contribute to the aggregate field, standard ranking and term matching apply.

[lucene]: https://jackrabbit.apache.org/oak/docs/query/lucene.html
[lucene-per-property-analyzer]: https://jackrabbit.apache.org/oak/docs/query/lucene.html#per-property-analyzer
[options]: https://www.elastic.co/guide/en/elasticsearch/reference/current/configure-text-analysis.html
[flattened]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/flattened#supported-operations
