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

# Mongot connector POC compatibility

## Goal and result

The acceptance bar for this POC is strict: every executable, portable Apache
Oak search contract must pass against the Mongot plugin. A failed contract is
not moved outside a compatibility boundary. If the connector cannot express a
required primitive through Mongot's public Search schema, the prototype
patches Mongot and proves that primitive at both the Mongot and Oak layers.

The clean release gate is:

```text
mvn -pl oak-search-mongot clean verify
```

The qualified build reports **468 tests, 0 failures, 0 errors, and 4 inherited
skips**. There are no connector-owned `@Ignore` methods and no opt-in failing
qualification lane.

The four skips come from the upstream Oak test sources rather than this
connector:

| Upstream test | Why it cannot qualify this backend |
| --- | --- |
| `analyzerWithHyphenationCompoundWord` | The inherited fixture contains literal placeholder XML (`<...>`) and is already ignored upstream. |
| `ambiguousSubtreeIndexWithDescendantConstraint` | OAK-3992 hard-codes Lucene-specific plan behavior before backend execution. |
| `unionOnTwoDescendants` | OAK-3993 is rejected by Oak's shared SQL2 parser before an index plugin is invoked. |
| `descendantTestWithIndexTagExplainWithNoData` | The shared Oak planner chooses traversal for an empty index despite the tag; the inherited test is already ignored with that explanation. |

This document covers the connector and search interface. Client packaging,
multi-tenancy, cloud topology, HA, and backup remain outside the current
functional POC.

## What had to change

### Connector work

The Oak module now owns the translation and adaptation work expected of an
index plugin:

- Oak index definitions become typed MongoDB documents and Search index
  definitions;
- SQL2/XPath restrictions become `$search`, `$match`, sort, facet, highlight,
  autocomplete, spellcheck, more-like-this, or vector-search requests;
- MongoDB results become Oak cursors, scores, excerpts, facets, suggestions,
  spellcheck rows, and result-size estimates;
- updates, deletes, moves, aggregate refresh, and full reindex preserve Oak
  indexing semantics;
- the portable index-import provider is registered and the inherited
  `IndexImporterReindexTest` passes;
- `sync-mode=rt` blocks a synchronous commit until a Search-observed marker is
  visible for every affected index;
- reindex writes a separate collection generation and publishes its hidden
  `:collectionSeed` pointer only after the replacement Search index is
  queryable, leaving the currently served generation intact throughout;
- ordinary result rows stream through a bounded Mongo aggregation cursor using
  the Elastic-compatible `queryFetchSizes` and `queryTimeoutMs` index
  properties; result shapes that require whole-set processing remain eager;
- legacy Oak V1 definitions are accepted while the connector uses its own V2
  storage format;
- Lucene native query strings, native MLT, regex native queries, and public
  `rep:similar` SQL2/XPath are translated without Elasticsearch;
- composed analyzers, inline synonyms, escaped/unescaped full-text syntax, and
  hyphenated wildcard terms are handled at the Oak boundary;
- insecure, secure, and statistical facet modes execute their inherited Oak
  contracts;
- near-real-time assertions preserve the original Oak query/result contract
  while allowing the external index to become visible.

### Mongot work

The original 30 analyzer nonpasses were not all solvable in the connector.
Mongot's public Search analyzer schema did not expose several Lucene primitives
that Oak's common suite provisions. The custom Mongot build pinned to the
provisioned Atlas cluster adds:

- `patternReplace` character filtering;
- common-grams, dictionary-compound-word, fingerprint, Hunspell, keep-word,
  keyword-marker, min-hash, pattern-capture-group, type, and extended
  word-delimiter token filtering;
- protected-word and custom character-type configuration;
- the legacy `dutch_kp` Snowball stemmer removed from the newer Lucene
  dependency, restored as generated Lucene 9 source;
- multi-term normalization that applies only normalization-safe filters.

This is the concrete answer to the earlier ownership question: query mapping,
resource provisioning, and result adaptation belong in the connector; missing
Search/Lucene analyzer primitives require Mongot changes. No Elasticsearch
code, client, protocol, container, or dependency is used.

The custom Mongot change set used for the Atlas pin was qualified: all 42
focused analyzer behavioral targets and all 943 repository lint, formatter,
Gazelle, Checkstyle, and SpotBugs targets pass.

## Functional verdict

`SUPPORTED` means an inherited Oak common contract or a real SQL2/XPath query
selected the Mongot index and returned the required behavior. Translator-only
tests are supporting diagnostics, not the compatibility verdict.

| Capability | Verdict | Evidence |
| --- | --- | --- |
| SQL2 and XPath full text | SUPPORTED | Property, node, relative-property, aggregate, Boolean, phrase, wildcard, fuzzy, boost, escaped syntax, and hyphenated wildcard contracts pass. |
| Native query surfaces | SUPPORTED | Lucene native strings, native MLT, regex native query, `rep:similar`, and hybrid lexical/similarity tests pass without Elasticsearch. |
| Property queries | SUPPORTED | Equality, inequality, ranges, IN, LIKE, null/not-null, typed values, multi-value properties, and regex-defined properties pass. |
| Paths and node types | SUPPORTED | Exact, child, descendant, parent transformation, strict paths, primary types, mixins, query paths, and long paths pass. |
| Aggregates and relative properties | SUPPORTED | Both common aggregation suites pass, including V1 definitions, excerpts, binaries, updates, deletes, and stale aggregate removal. |
| Functions, order, limit, offset | SUPPORTED | All inherited function and ordering contracts pass. Oak applies limit/offset above the index cursor because the `QueryIndex` SPI does not provide those values to the plugin. |
| Facets | SUPPORTED | All 14 inherited secure, insecure, and statistical cases execute and pass. |
| Excerpts and highlighting | SUPPORTED | All 9 excerpt cases plus the inherited aggregate excerpt contract pass. |
| Suggestions and spellcheck | SUPPORTED | Base and descendant contracts pass; only the two upstream OAK-3992/OAK-3993 tests remain skipped. |
| Built-in and composed analyzers | SUPPORTED | 54 executable inherited analyzer contracts pass; the one upstream placeholder fixture remains skipped. |
| Dynamic boost | SUPPORTED | All 12 inherited cases pass, including analyzed terms, exclusions, confidence ordering, and similarity-tag reranking. |
| Similarity and vectors | SUPPORTED FOR POC | SQL2/XPath similarity, more-like-this, vector mapping/storage/ranking, lexical filtering, and explain output pass. Candidate and relevance tuning remain workload work, not missing plumbing. |
| Updates, deletes, moves, reindex | SUPPORTED | Incremental mutation, subtree deletion, move, aggregate refresh, long-path identity, stale-document exclusion, and atomic generation handoff tests pass. Superseded-generation deletion is deliberately deferred to a future cleaner. |
| Commit visibility | SUPPORTED | Synchronous `sync-mode=rt` commits return only after every affected index observes a unique marker through Mongot. Async lanes remain eventually consistent. |
| Result retrieval | SUPPORTED FOR POC | Normal rows use a lazy Mongo aggregation cursor with bounded batches and a server-side timeout. Exact facets, suggestions, spellcheck, and explicit exact-size requests still consume the complete candidate set by design. |
| Result size and whiteboard toggle | SUPPORTED | Both current Oak result-size contracts pass. |
| Planner selection | SUPPORTED | All 69 inherited planner tests pass, plus exclusion, strict-path, improper-use, traversal parity, and SQL2 optimization suites. |
| Index import | SUPPORTED | The remote no-op importer is registered with `type=mongot`, and the portable reindex/import-state cleanup contract passes. |
| OSGi registration | SUPPORTED FOR POC | The importer, editor, tracker, and query provider are registered with `type=mongot`. |

## Oak common-suite accounting

| Common suite | Result |
| --- | --- |
| `FullTextIndexCommonTest` | 14/14 pass |
| `PropertyIndexCommonTest` | 20/20 pass |
| `IndexPathRestrictionCommonTest` | 7/7 pass |
| `OrderByCommonTest` | 14/14 pass |
| `FunctionIndexCommonTest` | 21/21 pass |
| `IndexAggregationCommonTest` | 12/12 pass |
| `IndexAggregation2CommonTest` | 5/5 pass |
| `IndexPlannerCommonTest` | 69/69 pass |
| `IndexQueryCommonTest` | 37 pass, 1 upstream skip |
| `FacetCommonTest` | 14/14 pass |
| `ExcerptTest` | 9/9 pass |
| Suggestion suites | 19 pass, 2 upstream skips |
| Spellcheck suites | 10/10 pass |
| `FullTextAnalyzerCommonTest` | 54 pass, 1 upstream skip |
| `DynamicBoostCommonTest` | 12/12 pass |
| Result-size suites | 2/2 pass |
| `IndexImporterReindexTest` | 1/1 pass |
| Exclusion, strict-path, improper-use, traversal-parity, SQL2-optimization | 12/12 pass |

### Connector-specific test intent

| Test | Regression contract protected |
| --- | --- |
| `MongotIndexImporterTest` | The remote importer owns only `type=mongot` and does not mutate the definition. |
| `MongotIndexImporterReindexTest` | Oak's portable import marker is removed while the remote index is rebuilt. |
| `MongotIndexWriterFactoryTest` | `sync-mode=rt` uses Elastic-compatible precedence and never forces async indexes into synchronous waits. |
| `MongotCommitSemanticsTest` | One real-time Oak commit waits for every affected Mongot index, not merely one writer. |
| `MongotIndexWriterTest` real-time case | Writer close returns only after the exact mutation is visible through Search. |
| `MongotIndexWriterTest` generation cases | Reindex builds away from the live collection, publishes only on successful close, and handles empty replacement/initial generations. |
| `MongotIndexDefinitionTest` generation case | Readers resolve the collection generation published in the Oak definition. |
| `MongotIndexDefinitionTest` query-property cases | Elastic-compatible fetch-size/timeout defaults and validation remain stable. |
| `MongotIndexNamesTest` generation case | Nonzero generation seeds create stable, safe, distinct collection names. |
| `MongotResultIteratorTest` lazy case | The cursor consumes only demanded rows, skips rejected rows, and closes on exhaustion. |
| `MongotResultIteratorTest` exact-size case | Draining for an exact count does not lose rows still owed to the consumer. |
| `MongotResultIteratorTest` failure case | Row-adaptation failures close the underlying Mongo cursor. |
| `MongotCoreQueryCompatibilityTest` large-result case | A result set larger than the first driver batch remains complete and ordered, including limit/offset above the cursor. |
| `MongotReindexCompatibilityTest` | A real Oak repository switches to a complete replacement generation and excludes stale documents, including an empty reindex. |

## Near-drop-in comparison with Oak Lucene and Elasticsearch

“Near drop-in” does not mean a byte-for-byte replacement bundle. An Oak client
must configure this provider and change an index definition's `type` from
`lucene` or `elasticsearch` to `mongot`. The useful bar is that content models,
Oak index rules, SQL2/XPath queries, mutation behavior, and operational
commit/reindex contracts continue without material client changes.

| Integration contract | Lucene / Elasticsearch source behavior | Mongot connector result |
| --- | --- | --- |
| Oak plugin shape | Both provide query and editor implementations over Oak's shared search SPI. | Same shared `FulltextIndex`, planner, editor, writer, tracker, and OSGi service shape; only backend type/configuration changes. |
| Index-definition surface | Lucene defines the portable vocabulary; Elastic consumes it and adds backend settings. | Portable rules, analyzers, aggregates, functions, facets, similarity, suggestions, spellcheck, and path settings are consumed directly. Elastic-only shard/replica/mapping knobs are not copied. |
| JCR query surface | Both are reached through the same Oak SQL2/XPath engine. Oak internally names special suggestion/spellcheck restrictions `native*lucene`. | Same JCR queries pass. The internal function name remains `lucene` because that is Oak's backend-neutral AST convention; it does not introduce a Lucene or Elasticsearch dependency. |
| Import/reindex marker | Lucene imports local files; Elastic's remote importer is a no-op while Oak rebuilds remote data. | Uses the Elastic remote pattern: no file import, with the portable import-state/reindex contract qualified. |
| Incremental writes | Lucene writers and Elastic bulk writers upsert/delete by repository path. | Upsert, exact delete, subtree delete, move, aggregate refresh, and long-path identity are qualified against a real Search deployment. |
| Near-real-time mode | Lucene has local NRT/hybrid machinery. Elastic recognizes synchronous `sync-mode=rt`. | Recognizes the same `sync-mode=rt` contract and waits on a backend-native Search marker; async indexing is unchanged. |
| Reindex availability | Elastic builds a seeded physical index and switches its alias only after success. | Builds a seeded collection and publishes `:collectionSeed` in the Oak commit only after Mongot is queryable. The Oak definition is the atomic pointer because Mongot has no Elastic alias equivalent. |
| Large result retrieval | Lucene uses `searchAfter`; Elastic uses lazy async `search_after` requests with fetch-size and timeout properties. | Uses a lazy Mongo aggregation cursor, driver batch size, and `maxTime`. It preserves ordering/filter semantics for both `$search` and property-only pipelines without loading all normal rows into connector memory. |
| Backend health and administration | Lucene and Elastic register index-info providers, statistics, MBeans, and cleanup services. | Basic document statistics exist, but `IndexInfoProvider`, management metrics/MBeans, and a retired-generation cleaner are not yet implemented. These are real operational gaps, not query compatibility gaps. |
| Backend-specific machinery | Lucene owns directories, copy-on-read/write, local caches, and hybrid queues; Elastic owns aliases, shards, replicas, and HTTP-client settings. | Those mechanisms are not copied. Mongo collections, Search indexes, driver cursors, and Search-observed markers provide the corresponding backend responsibilities. |
| Embedding generation | Vector query support does not determine how embeddings are produced. | Vector storage, similarity, lexical/vector filtering, and hybrid query plumbing are qualified. Embedding generation is still a separate product/architecture decision. |

The implementation includes portable index importing, preserves the live
collection during reindex, and avoids eagerly materializing ordinary query
results. The remaining work is listed explicitly below.

## Remaining ownership

| Gap | Owner | Why |
| --- | --- | --- |
| Retired collection-generation cleanup with a grace period | Connector | MongoDB lifecycle policy around the generation pointer. It does not require a new Search primitive. |
| `IndexInfoProvider`, metrics, MBeans, richer OSGi controls | Connector integration | Administration and observability around already working indexing/query paths. |
| Progressive `searchAfter` requests rather than a driver cursor | Connector optimization | The current cursor is bounded and lazy; matching Elastic's successive fetch sizes exactly would require a token-paged iterator and careful handling of post-search sorts. |
| Lucene analyzer primitives already absent from public Search schema | Mongot | The Atlas-pinned custom Mongot build contains the extensions required for the all-green analyzer lane. |
| Client package/deployment validation, scale, HA, tenant isolation | Product/deployment architecture | Outside the connector/search-function scope of this POC. |
| Embedding generation and model lifecycle | Product decision, then connector if selected | Query-time vector/hybrid support does not decide where embeddings are produced. |

## POC decision

The POC now demonstrates the intended division of responsibility: an Oak
Mongot plugin plus its MongoDB aggregation/result adapter can replace the
Elasticsearch integration for the executable portable Oak search surface,
while Mongot remains the
Lucene-backed execution engine and must expose the analyzer primitives Oak
requires. The connector/common-test compatibility gate is green. The remaining
connector work is operational hardening and client integration—not a known gap
in the qualified JCR query, indexing, commit-visibility, or reindex handoff
surface.
