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

<!--
TOC created with some help:
grep "^#.*$" src/site/markdown/query/query-engine.md | sed 's/#/    /g' | sed 's/\(#*\) \(.*\)/\1 * [\2](#\2)/g'
-->

## The Query Engine

- [The Query Engine](#the-query-engine)
- [Overview](#overview)
  - [Query Processing](#query-processing)
    - [Cost Calculation](#cost-calculation)
    - [Identifying Nodes](#identifying-nodes)
    - [Ordering](#ordering)
    - [Iterating the Result Set](#iterating-the-result-set)
  - [Query Options](#query-options)
    - [Query Option Traversal](#query-option-traversal)
    - [Query Option Offset / Limit](#query-option-offset--limit)
    - [Query Option Index Tag](#query-option-index-tag)
    - [Index Selection Policy](#index-selection-policy)
  - [Compatibility](#compatibility)
    - [Result Size](#result-size)
    - [Quoting](#quoting)
    - [Equality for Path Constraints](#equality-for-path-constraints)
  - [Slow Queries and Read Limits](#slow-queries-and-read-limits)
  - [Keyset Pagination](#keyset-pagination)
  - [Full-Text Queries](#full-text-queries)
  - [Excerpts and Highlighting](#excerpts-and-highlighting)
    - [SimpleExcerptProvider](#simpleexcerptprovider)
  - [Native Queries](#native-queries)
  - [Similarity Queries](#similarity-queries)
  - [Spellchecking](#spellchecking)
  - [Suggestions](#suggestions)
  - [Facets](#facets)
  - [XPath to SQL-2 Transformation](#xpath-to-sql-2-transformation)
  - [The Node Type Index](#the-node-type-index)
  - [Temporarily Disabling an Index](#temporarily-disabling-an-index)
  - [Deprecating Indexes](#deprecating-indexes)
  - [The Deprecated Ordered Index](#the-deprecated-ordered-index)
  - [Index Storage and Manual Inspection](#index-storage-and-manual-inspection)
  - [SQL-2 Optimisation](#sql-2-optimisation)
  - [Additional XPath and SQL-2 Features](#additional-xpath-and-sql-2-features)
  - [Temporarily Blocking Queries](#temporarily-blocking-queries)


## Overview

Oak does not index as much content by default as does Jackrabbit 2. You need to create custom
indexes when necessary, much like in traditional RDBMSs. If there is no index for a
specific query, then the repository will be traversed. That is, the query will still
work but probably be very slow.

Query Indices are defined under the `oak:index` node.

### Query Processing

Internally, the query engine uses a cost based query optimizer that asks all the available
query indexes for the estimated cost to process the query. It then uses the index with the
lowest cost.

By default, the following indexes are available:

* A property index for each indexed property.
* A full-text index which is based on Apache Lucene / Solr.
* A node type index (which is based on an property index for the properties
  jcr:primaryType and jcr:mixins).
* A traversal index that iterates over a subtree.

If no index can efficiently process the filter condition, the nodes in the repository are
traversed at the given subtree.

Usually, data is read from the index and repository while traversing over the query
result. There are exceptions however, where all data is read in memory when the query
is executed. The most common case is when using an `order by` clause and
the index can not provide a sorted result.
There are other cases where paths of the results read so far are kept in memory,
in order to not return duplicate results.
This is the case when using `or` conditions such that two indexes are used
(internally a `union` query is executed).

If you enable debug logging for the module `org.apache.jackrabbit.oak.query`, you may see this:

    cost for nodeType is 1638354.0
    cost for property is 2.0
    cost for traverse is 3451100.0

This means the cost for the nodetype index _would_ be about 1638354.0,
the cost for the property index _would_ be about 2,
and the cost for traversal _would_ be about 3451100.0.
An index that can't deal with a certain condition will return the cost "Infinity".
It doesn't say traversal is actually used, it just lists the expected costs.
The query engine will then pick the index with the lowest expected cost,
which is (in the case above) "property".

The expected cost for traversal is, with Oak 1.0.x, really just a guess looking at
the length of the path. With Oak 1.2 and newer, the "counter" index is used
(see mainly OAK-1907). There is an known issue with this, if you add and remove a lot
of nodes in a loop, you could end up with a too-low cost, see OAK-4065.

#### Cost Calculation

Each query index is expected to estimate the worst-case cost to query with the given filter.
The returned value is between 1 (very fast; lookup of a unique node) and
the estimated number of entries to traverse, if the cursor would be fully read,
and if there could in theory be one network round-trip or disk read operation per node
(this method may return a lower number if the data is known to be fully in memory).

The returned value is supposed to be an estimate and doesn't have to be very accurate.
Please note this method is called on each index whenever a query is run,
so the method should be reasonably fast (not read any data itself, or at least not read too much data).

If an index implementation can not query the data, it has to return `Infinity` (`Double.POSITIVE_INFINITY`).

#### Identifying Nodes

If an index is selected, the query is executed against the index. The translation from the JCR Query syntax into the query language supported by the index includes as many constraints as possible which are supported by the index. Depending on the index definition this can mean that not all constraints can be resolved by the index itself. 
In this case, the Query Engine tries to let the index handle as much constraints as possible and later executes all remaining constraints while [Iterating the Result Set](#iterating-the-result-set) by retrieving the nodes from the node store and evaluating the nodes against the constraints. Each retrieval, including non-matching nodes is counted as a traversal. This means that despite the use of an index an additional traversal can be required if not all constraints in a query are executed against the index.

If no matching index is determined in the previous step, the Query Engine executes this query solely based on a traversing the Node Store and evaluating the nodes against the constraints.

#### Ordering
If a query requests an ordered result set, the Query Engine tries to get an already ordered result from the index; in case the index definition does not support the requested ordering or in case of a traversal, the Query Engine must execute the ordering itself. To achieve this the entire result set is read into memory and then sorted. This consumes memory, takes time and requires the Query Engine to read the full result set even in the case where a limit setting would otherwise limit the number of results traversed.

#### Iterating the Result Set
Query results are implemented as lazy iterators, and the result set is only read if needed. When the next result is requested, the result iterator seeks the potential nodes to find the next node matching the query. 
During this seek process the Query Engine reads and filters the potential nodes until it finds a match. Even if the query is handled completely by an index, the Query Engine needs to check if the requesting session is allowed to read the nodes.

That means that during this final step every potential node must be loaded from the node store, thus counting towards the read limit (see [Slow Queries and Read Limits](#slow-queries-and-read-limits)).


### Query Options

With query options, you can enforce the usage of indexes (failing the query if there is no index), 
limit which indexes should be considered and set the limit and offset for a query.

#### Query Option Traversal

By default, queries without index will log an info level message as follows
(see OAK-4888, since Oak 1.6):

    Traversal query (query without index): {statement}; consider creating an index

This message is only logged if no index is available, and if the query
potentially traverses many nodes.
No message is logged if an index is available, but traversing is cheap.

By setting the JMX configuration `QueryEngineSettings.failTraversal` to true,
queries without index throw an exception instead of just logging a message.

In the query itself, the syntax `option(traversal {ok|fail|warn})` is supported
(at the very end of the statement, after `order by`).
This is to override the default setting,
for queries that traverse a well known number of nodes (for example 10 or 20 nodes).
This is supported for both XPath and SQL-2, as follows:

    /jcr:root/oak:index/*[@type='lucene'] option(traversal ok)

    select * from [nt:base]
    where ischildnode('/oak:index')
    order by name()
    option(traversal ok)


#### Query Option Offset / Limit

`@since Oak 1.44.0 (OAK-9740)`

By setting the offset / limit of a query you can set the limits and offsets set on the query object.
Note, this setting will be overridden by any settings made via the `Query#setOffset` or `Query#setLimit` methods.

The syntax is `option(limit {num}, offset {num})` at the very end of the statement, after `order by` 
and can be used in conjunction with any other option.

This is supported for both XPath and SQL-2, as follows:

    /jcr:root/oak:index/*[@type='lucene'] option(limit 100)

    select * from [nt:base]
    where ischildnode('/oak:index')
    order by name()
    option(offset 19, limit 20)

#### Query Option Index Tag

`@since Oak 1.7.4 (OAK-937)`

By default, queries will use the index with the lowest expected cost (as in relational databases).
But some cases, it is needed to specify which index(es) should be considered for a query:

* If you want to ensure a specific type of index is used for specific type of queries,
  for example custom indexes are used for custom queries.
* If there are multiple Lucene fulltext indexes with different aggregation rules,
  and the index data overlaps.
  In the query, you want to specify which aggregation rule to use.
* To temporarily work around limitations of the index implementation,
  for example incorrect cost estimation.

To use index tags,
add `tags` (a multi-valued String property) to the index(es) of choice, for example:

    /oak:index/lucene/tags = [x, y]

Note each index can have multiple tags, and the same tag can be used in multiple indexes.
The syntax to limit a query to a certain tag is: `<query> option(index tag <tagName>)`:

    /jcr:root/content//element(*, nt:file)[jcr:contains(., 'test')] option(index tag x)

    select * from [nt:file]
    where ischildnode('/content')
    and contains(*, 'test')
    option(index tag [x])

The query will only consider the indexes that contain the specified tag (that is, possibly multiple indexes).
Each query supports one tag only.
If a query doesn't explicitly uses this option, then all indexes are considered
(including indexes with tags and indexes without tags).
The tag name may only contain the characters `a-z, A-Z, 0-9, _`.

Limitations:

* This is currently supported in indexes of type `lucene` compatVersion 2, and type `property`.
* For indexes of type `lucene`, when adding adding or changing the property `tags`,
  you need to also set the property `refresh` to `true` (Boolean),
  so that the change is applied. No indexing is required.
* Solr indexes, and the reference index, don't support tags yet.
  That means they still might return a low cost, even if the tag does not match.
* The nodetype index only partially supports this feature: if a tag is specified in the query, then the nodetype index
  is not used. However, tags in the nodetype index itself are ignored currently.
* There is currently no way to disable traversal that way.
  So if the expected cost of traversal is very low (lower than the cost of any index),
  the query will traverse.
  To avoid traversal, note that indexes support cost overrides,
  and traversal is never used for fulltext queries.

#### Index Selection Policy

`@since Oak 1.42.0 (OAK-9587)`

To ensure an index is only used if the `option(index tag <tagName>)` is specified,
certain index types support the `selectionPolicy`.
If set to the value `tag`, an index is only used for queries
that specify `option(index tag x)`, where `x` is one of the tags of this index.

This feature allows to safely add an index, without risking that existing queries
that don't specify the index tag will switch to this new index.

Limitations:

* This is currently supported in indexes of type `lucene` compatVersion 2, and type `property`.
* For indexes of type `lucene`, when adding or changing the property `selectionPolicy`,
  you need to also set the property `refresh` to `true` (Boolean),
  so that the change is applied. No indexing is required.

### Compatibility

#### Result Size

For NodeIterator.getSize(), some versions of Jackrabbit 2.x returned the estimated (raw)
Lucene result set size, including nodes that are not accessible.

By default, Oak does not do this; it either returns the correct result size, or -1.
Oak 1.2.x and newer supports a compatibility flag so that it works in a similar way
as Jackrabbit 2.x, by returning an estimate (see OAK-2926).
Specially, only query restrictions that are part of the used index are considered when calculating the size.
Additionally, ACLs are not applied to the results,
so nodes which are not visible to the current session will still be included in the count returned.
As such,  the count returned can be higher than the actual number of results
and the accurate count can only be determined by iterating through the results.

This only works with the Lucene `compatVersion=2` right now,
so even if enabled, getSize may still return -1 if the index used does not support the feature.
Example code to show how this work (where `test` is a common word in the index):

    String query = "//element(*, cq:Page)[jcr:contains(., 'test')]";
    Query query = queryManager.createQuery(qs, "xpath");
    QueryResult result = query.execute();
    long size = result.getRows().getSize();

This is best configured via OSGi configuration `fastQuerySize` of `org.apache.jackrabbit.oak.query.QueryEngineSettingsService` (since Oak 1.6.x),
or as described in OAK-2977, since Oak 1.3.x:
When using Apache Sling, add the following line to the file `conf/sling.properties`,
and then restart the application:

    oak.query.fastResultSize=true

#### Quoting

[Special characters in queries need to be escaped.](https://jackrabbit.apache.org/archive/wiki/JCR/EncodingAndEscaping_115513396.html)

However, compared to Jackrabbit 2.x,
the query parser is now generally more strict about invalid syntax.
The following query used to work in Jackrabbit 2.x, but not in Oak,
because multiple way to quote the path are used at the same time:

    SELECT * FROM [nt:base] AS s
    WHERE ISDESCENDANTNODE(s, ["/libs/sling/config"])

Instead, the query now needs to be:

    SELECT * FROM [nt:base] AS s
    WHERE ISDESCENDANTNODE(s, [/libs/sling/config])

#### Equality for Path Constraints

In Jackrabbit 2.x, the following condition was interpreted as a LIKE condition:

    SELECT * FROM nt:base WHERE jcr:path = '/abc/%'

Therefore, the query behaves exactly the same as if LIKE was used.
In Oak, this is no longer the case, and such queries search for an exact path match.

### Slow Queries and Read Limits

Slow queries are logged as follows:

    *WARN* Traversed 10000 nodes with filter Filter(query=select ...)
    consider creating an index or changing the query

If this is the case, an index might need to be created, or the condition
of the query might need to be changed to take advantage of an existing index.

Queries that traverse many nodes, or that read many nodes in memory, can be cancelled.
The limits can be set at runtime (also while a slow query is running) using JMX,
domain "org.apache.jackrabbit.oak", type "QueryEngineSettings", attribute names
"LimitInMemory" and "LimitReads".
These setting are not persisted, so in the next restart, the default values (unlimited) are used.
As a workaround, these limits can be changed using the system properties
"oak.queryLimitInMemory" and "oak.queryLimitReads".
Queries that exceed one of the limits are cancelled with an UnsupportedOperationException saying that
"The query read more than x nodes... To avoid running out of memory, processing was stopped."

"LimitReads" applies to the number of nodes read by a query.
It applies whether or not an index is used.
The exact number of nodes read for a query depends on many factors,
mainly the query, the query plan used, the index configuration, access rights, and nodes in the repository.
The result size of a query is often much smaller than the number of nodes read.
As an example, if a query has just two conditions, as in `a=1 and b=2`, and if there is an index on `a`,
then all nodes with `a=1` need to be read while traversing the result.
If more nodes are read than the set limit, then an exception is thrown.
If the query also has a path condition (for example descendants of `/home`),
and if the index supports path conditions (which is the case for all property indexes, and
also for Lucene indexes if `evaluatePathRestrictions` is set), then only nodes in the given subtree
are read.

"LimitInMemory" applies to nodes read in memory, in order to sort the result,
and in order to ensure the same node is only returned once.
It applies whether or not an index is used.
As an example, if a query uses `order by c`, and if the index used for this query does not
support ordering by this property, then all nodes that match the condition are read in memory first,
even before the first node is returned.
Property indexed can not order, and Lucene indexes can only order when enabling `ordered` for a property.
Ensuring the same node is only returned once: this is needed for queries that contain `union`
(it is not needed when using `union all`). It is also needed if a query uses `or` conditions
on different properties. For example, if a query uses `a=1 or b=2`,
then the conversion to `union` would be `select ... where a=1 union select ... where b=2`.
The query is converted to a `union` so that both indexes can be used,
in case there are separate indexes for `a` and `b`.
For XPath queries, such conversion to `union` is always made,
and for SQL-2 queries such a conversion is only made if the `union` query has a lower expected cost.
When using `or` in combination with the same property, as in `a=1 or a=2`, then no conversion to `union` is made.

### Keyset Pagination

It is best to limit the result size to at most a few hundred entries.
To read a large result, keyset pagination should be used.
Note that "offset" with large values (more than a few hundred) should be avoided, as it can lead to performance and memory issues.
Keyset pagination refers to ordering the result set by a key column, and then paginate using this column.
It requires an ordered index on the key column. Example:

    /jcr:root/content//element(*, nt:file)
    [@jcr:lastModified >= $lastEntry]
    order by @jcr:lastModified, @jcr:path

For the first query, set `$lastEntry` to 0, and for subsequent queries,
use the last modified time of the last result.

An order index is needed for these queries to work efficiently, e.g.:

    /oak:index/fileIndex
      - type = lucene
      - compatVersion = 2
      - async = async
      - includedPaths = [ "/content" ]
      - queryPaths = [ "/content" ]
      + indexRules
        + nt:file
          + properties
            + jcrLastModified
              - name = "jcr:lastModified"
              - propertyIndex = true
              - ordered = true

Notice that multiple entries with the same modified date might exist.
If your application requires that the same node is only processed once,
then additional logic is required to skip over the entries already seen (for the same modified date).

If there is no good property to use keyset pagination on, then the lowercase of the node name can be used.
It is best to start with `$lastEntry` as an empty string, and then in each subsequent run use the lowercase version of the node name of the last entry.
Notice that some nodes may appear in two query results, if there are multiple nodes with the same name.
In this case, SQL-2 needs to be used, because with XPath, escaping is applied to names.

    select [jcr:path], * from [nt:file] as a
    where lower(name(a)) >= $lastEntry
    and isdescendantnode(a, '/content')
    order by lower(name(a)), [jcr:path]

    /oak:index/fileIndex
      - type = lucene
      - compatVersion = 2
      - async = async
      - includedPaths = [ "/content" ]
      - queryPaths = [ "/content" ]
      + indexRules
        + nt:file
          + properties
            + lowercaseName
              - function = "lower(name())"
              - propertyIndex = true
              - ordered = true


### Full-Text Queries

The full-text syntax supported by Jackrabbit Oak is a superset of the JCR specification.

By default (that is, using a Lucene index with `compatVersion` 2), Jackrabbit Oak uses the
[Apache Lucene grammar for fulltext search](https://lucene.apache.org/core/4_7_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html).
[See also how to escape queries.](https://jackrabbit.apache.org/archive/wiki/JCR/EncodingAndEscaping_115513396.html).
However, the words `AND` and `NOT` are treated as search terms,  and only `OR` is supported as a keyword. Instead of `NOT`, use `-`.
Instead of `AND`, just write the terms next to each other (instead of `hello AND world`, just write `hello world`).

For older Lucene indexes (`compatVersion` 1),
the following syntax is supported within `contains` queries.
This is a subset of the Apache Lucene syntax:

    FullTextSearch ::= Or
    Or ::= And { ' OR ' And }*
    And ::= Term { ' ' Term }*
    Term ::= ['-'] { SimpleTerm | PhraseTerm } [ '^' Boost ]
    SimpleTerm ::= Word
    PhraseTerm ::= '"' Word { ' ' Word }* '"'
    Boost ::= <number>

Please note that `OR` needs to be written in uppercase.
Characters within words can be escaped using a backslash.

Examples:

    jcr:contains(., 'jelly sandwich^4')
    jcr:contains(@jcr:title, 'find this')

In the first example, the word "sandwich" has weight four times more than the word "jelly."
For details of boosting, see the Apache Lucene documentation about Score Boosting.

For compatibility with Jackrabbit 2.x, single quoted phrase queries are currently supported.
That means the query `contains(., "word ''hello world'' word")` is supported.
New applications should not rely on this feature.

### Excerpts and Highlighting

The Lucene index can be configured to provide excerpts and highlighting.
See <a href="lucene.html#Property_Definitions">useInExcerpt</a> for details
on how to configure excerpt generation.

For queries to use those excerpts, the query needs to use the Lucene index where
this is configured. The queries also needs to contain the "excerpt" property, as follows:

    /jcr:root/content//*[jcr:contains(., 'test')]/(rep:excerpt(.))

The excerpt is then read using the JCR API call `row.getValue("rep:excerpt(.)")`.

Since Oak version 1.10 (OAK-7151), optionally a property name can be specified in the query:

    /jcr:root/content//*[jcr:contains(., 'test')]/(rep:excerpt(@jcr:title) | rep:excerpt(.))

The excerpt for the title is then read using `row.getValue("rep:excerpt(@title)")`,
and the excerpt for the node using (as before) `row.getValue("rep:excerpt(.)")`.

#### SimpleExcerptProvider

The SimpleExcerptProvider is a fallback mechanism for excerpts and highlighting.
This mechanism has limitations, and should only be used if really needed.
The SimpleExcerptProvider is independent of the index configuration.
Highlighting is limited, for example stopwords are ignored.
Highlighting is case insensitive since Oak versions 1.2.30, 1.4.22, 1.6.12, 1.8.3, and 1.10 (OAK-7437).

The SimpleExcerptProvider is used when reading an excerpt
if the query doesn't contain an excerpt property, as in:

    /jcr:root/content//*[jcr:contains(., 'test')]

The SimpleExcerptProvider is also used if an excerpt is requested
for a property that is not specified in the query. For example,
when using `row.getValue("rep:excerpt(@title)")`, but the query does not contain
this property as an excerpt property, as in:

    /jcr:root/content//*[jcr:contains(., 'test')]/(rep:excerpt(.))

The SimpleExcerptProvider is also used for queries that don't use
a Lucene index, or if the query uses a Lucene index, but excerpts are not configured there.

### Native Queries
`@deprecated Oak 1.46`
To take advantage of features that are available in full-text index implementations
such as Apache Lucene and Apache Lucene Solr, so called `native` constraints are supported.
Such constraints are passed directly to the full-text index. This is supported
for both XPath and SQL-2. For XPath queries, the name of the function is `rep:native`,
and for SQL-2, it is `native`. The first parameter is the index type (currently supported
are `solr` and `lucene`). The second parameter is the native search query expression.
For SQL-2, the selector name (if needed) is the first parameter, just before the language.
Examples:

    //*[rep:native('solr', 'name:(Hello OR World)')]

    select [jcr:path] from [nt:base]
    where native('solr', 'name:(Hello OR World)')

    select [jcr:path] from [nt:base] as a
    where native(a, 'solr', 'name:(Hello OR World)')

This also allows to use the Solr [MoreLikeThis](http://wiki.apache.org/solr/MoreLikeThis)
feature. An example query is:

    select [jcr:path] from [nt:base]
    where native('solr', 'mlt?q=id:UTF8TEST&mlt.fl=manu,cat&mlt.mindf=1&mlt.mintf=1')

If no full-text implementation is available, those queries will fail.

### Similarity Queries

Oak supports similarity queries when using the Lucene or Solr indexes.
For example, the following query will return nodes that have similar content than
the node /test/a:

    //element(*, nt:base)[rep:similar(., '/test/a')]

Compared to Jackrabbit 2.x, support for rep:similar has the following limitations:
Full-text aggregation is not currently supported.

### Spellchecking

`@since Oak 1.1.17, 1.0.13`

Oak supports spellcheck queries when using the Lucene or Solr indexes.
Unlike most queries, spellcheck queries won't return a JCR `Node` as
the outcome of such queries will be text terms
that come from content as written into JCR `properties`.
For example, the following query will return spellchecks for the (wrongly spelled) term `helo`:

    /jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())

The result of such a query will be a JCR `Row` which will contain the corrected terms,
as spellchecked by the used underlying
index, in a special property named `rep:spellcheck()`.

Clients wanting to obtain spellchecks could use the following JCR code:

`@until Oak 1.3.10, 1.2.13` spellchecks are returned flat.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    String spellchecks = "";
    if (it.hasNext()) {
        spellchecks = it.getValue("rep:spellcheck()").getString()
    }

The `spellchecks` String would be have the following pattern `\[[\w|\W]+(\,\s[\w|\W]+)*\]`, e.g.:

    [hello, hold]

`@since Oak 1.3.11, 1.2.14` each spellcheck would be returned per row.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    List<String> spellchecks = new LinkedList<String>();
    while (it.hasNext()) {
        spellchecks.add(it.getValue("rep:spellcheck()").getString());
    }

If either Lucene or Solr were configured to provide the spellcheck feature, see
[Enable spellchecking in Lucene](lucene.html#Spellchecking) and [Enable
spellchecking in Solr](solr.html#Spellchecking)

Note that spellcheck terms come already filtered according to calling user privileges,
so that users could see spellcheck
corrections only coming from indexed content they are allowed to read.

### Suggestions

`@since Oak 1.1.17, 1.0.15`

Oak supports search suggestions when using the Lucene or Solr indexes.
Unlike most queries, suggest queries won't return a JCR `Node` as the outcome of such queries
will be text terms that come from content as written into JCR `properties`.
For example, the following query will return search suggestions for the (e.g. user entered) term `in `:

    /jcr:root[rep:suggest('in ')]/(rep:suggest())

The result of such a query will be a JCR `Row` which will contain the suggested terms,
together with their score, as
suggested and scored by the used underlying index, in a special property named `rep:suggest()`.

Clients wanting to obtain suggestions could use the following JCR code:

`@until Oak 1.3.10, 1.2.13` suggestions are returned flat.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:suggest('in ')]/(rep:suggest())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    String suggestions = "";
    if (it.hasNext()) {
        suggestions = it.getValue("rep:suggest()").getString()
    }

The `suggestions` String would be have the following pattern
`\[\{(term\=)[\w|\W]+(\,weight\=)\d+\}(\,\{(term\=)[\w|\W]+(\,weight\=)\d+\})*\]`, e.g.:

    [{term=in 2015 a red fox is still a fox,weight=1.5}, {term=in 2015 my fox is red,
    like mike's fox and john's fox,weight=0.7}]


`@since Oak 1.3.11, 1.2.14` each suggestion would be returned per row.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:suggest('in ')]/(rep:suggest())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    List<String> suggestions = new LinkedList<String>();
    while (it.hasNext()) {
        suggestions.add(it.getValue("rep:suggest()").getString());
    }

If either Lucene or Solr were configured to provide the suggestions feature,
see [Enable suggestions in Lucene](lucene.html#Suggestions) and [Enable
suggestions in Solr](solr.html#Suggestions).
Note that suggested terms come already filtered according to calling user privileges,
so that users could see suggested
terms only coming from indexed content they are allowed to read.

### Facets

`@since Oak 1.3.14` Oak has support for [facets](https://en.wikipedia.org/wiki/Faceted_search).
Once enabled (see details for [Lucene](lucene.html#Facets) and/or [Solr](solr.html#Suggestions) indexes)
facets can be retrieved on properties (backed by a proper
field in Lucene / Solr) using the following snippet:

    String sql2 = "select [jcr:path], [rep:facet(tags)] from [nt:base] " +
                    "where contains([jcr:title], 'oak')");
    Query q = qm.createQuery(sql2, Query.JCR_SQL2);
    QueryResult result = q.execute();
    FacetResult facetResult = new FacetResult(result);
    Set<String> dimensions = facetResult.getDimensions(); // { "tags" }
    List<FacetResult.Facet> facets = facetResult.getFacets("tags");
    for (FacetResult.Facet facet : facets) {
        String label = facet.getLabel();
        int count = facet.getCount();
        ...
    }

Nodes/Rows can still be retrieved from within the QueryResult object the usual way.

Do note that retrieving facets don't affect the result set or query constraints. So, only those
indexes which can resolve the query constraints would be considered for resolving the query.
For lucene indexes the index must also index relevant properties for faceting to be considered
for evaluating the query. So, a query like:

    SELECT [rep:facet(jcr:title)] FROM [nt:unstructured]

can only be resolved by an index which is indexing all `nt:unstructured` nodes. Following query is
is what should be used to get facets from nodes which have existing faceted proeprty:

    SELECT [rep:facet(jcr:title)] FROM [nt:unstructured] WHERE [jcr:title] IS NOT NULL

### XPath to SQL-2 Transformation

To support the XPath query language, such queries are internally converted to SQL-2.

Every conversion is logged in `debug` level under the
`org.apache.jackrabbit.oak.query.QueryEngineImpl` logger:

    org.apache.jackrabbit.oak.query.QueryEngineImpl Parsing xpath statement:
        //element(*)[@sling:resourceType = 'slingevent:Lock')]
    org.apache.jackrabbit.oak.query.QueryEngineImpl XPath > SQL2:
        select [jcr:path], [jcr:score], * from [nt:base] as a
        where [sling:resourceType] = 'slingevent:Lock'
        /* xpath: //element(*)[@sling:resourceType = 'slingevent:Lock'
        and @lock.created < xs:dateTime('2013-09-02T15:44:05.920+02:00')] */

_Each transformed SQL-2 query contains the original XPath query as a comment._

When converting from XPath to SQL-2, `or` conditions are automatically converted to
`union` queries, so that indexes can be used for conditions of the form
`a = 'x' or b = 'y'`.

### The Node Type Index

The `NodeTypeIndex` implements a `QueryIndex` using `PropertyIndexLookup`s on
`jcr:primaryType` `jcr:mixinTypes` to evaluate a node type restriction on the filter.
The cost for this index is the sum of the costs of the `PropertyIndexLookup`
for queries on `jcr:primaryType` and `jcr:mixinTypes`.

### Temporarily Disabling an Index

To temporarily disable an index (for example for testing), set the index type to `disabled`.
Please note that while the index type is not set, the index is not updated, so if you enable it again,
it might not be correct. This is specially important for synchronous indexes.

### Deprecating Indexes

If an index is still needed for backward compatibility, but should not longer be used
for queries, then set the property `deprecated` in the index definition.
This will log a warning "This index is deprecated" if the index is used for any queries.
This will allow to detect any usage of the index in the application.

### The Deprecated Ordered Index

This index has been deprecated in favour of Lucene Property
index. Please refer to [Lucene Index documentation](lucene.html) for
details.

For help on migrating to a Lucece index please refer to:
[Migrate ordered index](ordered-index-migrate.html)

For historical information around the index please refer to:
[Ordered Index](ordered-index.html).

### Index Storage and Manual Inspection

Sometimes there is a need to inspect the index content for debugging (or pure curiosity).
The index content is generally stored as content under the index definition as hidden nodes
(this doesn't apply to the solr index).
In order to be able to browse down into an index content you need a low level repository tool
that allows NodeStore level access.
There are currently 2 options: the oak-console (command line tool,
works will all existing NodeStore implementations) and the oak-explorer
(gui based on java swing, works only with the SegmentNodeStore),
both available as run modes of the
[oak-run](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-run/README.md) module

The structure of the index is specific to each implementation and is subject to change.
What is worth mentioning is that all the _*PropertyIndex_
flavors store the content as unstructured nodes (clear readable text),
the _Lucene_ index is stored as binaries, so one would need to export the
entire Lucene directory to the local file system and browse it using a dedicated tool.

### SQL-2 Optimisation

    @since 1.3.9 with -Doak.query.sql2optimisation

Enabled by default in 1.3.11 it will perform a round of optimisation
on the `Query` object obtained after parsing a SQL-2 statement. It will
for example attempt a conversion of OR conditions into UNION
[OAK-1617](https://issues.apache.org/jira/browse/OAK-1617).

To disable it provide `-Doak.query.sql2optimisation=false` at the start-up.

### Additional XPath and SQL-2 Features

The Oak implementation supports some features that are not part of the JCR specification:

    @since 1.5.12

Union for XPath and SQL-2 queries. Examples:

    /jcr:root/(content|lib)/*
    /jcr:root/content//*[@a] | /jcr:root/lib//*[@b]) order by @c
    select * from [nt:base] as a where issamenode(a, '/content')
    union select * from [nt:base] as a where issamenode(a, '/lib')

XPath functions "fn:string-length" and "fn:local-name".

### Temporarily Blocking Queries

    @since 1.14.0

Application code can run bad queries that read a lot of data or consume a lot of memory.
The best solution is to fix the application, however this can take some time.
Queries can be blocked at runtime using validator patterns, without having to immediately change the application.
Validator patterns can be set and inspected using the JMX `QueryEngineSettingsMBean` as follows:

* `setQueryValidatorPattern`: Adds or removes a query pattern.
* `queryValidatorJson`: Gets the existing patterns, including how often and when last execution occurred.

When adding a new pattern, it is recommended to first set `failQuery` to `false` to verify
the pattern is correct (only a warning is logged when running matching queries).
Once the pattern is correct, set `failQuery` to `true`.
Validator patterns can be stored in the repository under `/oak:index/queryValidator/<patternKey>`
(nodetype e.g. `nt:unstructured`) as follows:

* `pattern`: The regular expression of the query. Alternatively, a multi-valued string that contains a list of exact parts of the query.
* `failQuery`: Whether to fail the query (true) or just log a warning (false).
* `comment`: The pattern comment.

If the pattern is set using the multi-valued string, then the regular expression pattern is constructed from this array of texts.
In this case, no escaping is needed. Example patterns are:

* `[ "SELECT * FROM", "ORDER BY [name]" ]`: All queries that start and end with the given texts.
* `"/jcr:root/var/acme/.*"`: All queries that match this regular expression.

Patterns are evaluated in alphabetical order.
They are only read once, at startup.

See also [OAK-8294](https://issues.apache.org/jira/browse/OAK-8294)
