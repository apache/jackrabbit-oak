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

* [Overview](#Overview)
    * [Query Processing](#Query_Processing)
        * [Cost Calculation](#Cost_Calculation)
    * [Query Options](#Query_Options)
        * [Query Option Traversal](#Query_Option_Traversal)
        * [Query Option Index Tag](#Query_Option_Index_Tag)
    * [Compatibility](#Compatibility)
        * [Result Size](#Result_Size)
        * [Quoting](#Quoting)
        * [Equality for Path Constraints](#Equality_for_Path_Constraints)
    * [Slow Queries and Read Limits](#Slow_Queries_and_Read_Limits)
    * [Full-Text Queries](#Full-Text_Queries)
    * [Native Queries](#Native_Queries)
    * [Similarity Queries](#Similarity_Queries)
    * [Spellchecking](#Spellchecking)
    * [Suggestions](#Suggestions)
    * [Facets](#Facets)
    * [XPath to SQL-2 Transformation](#XPath_to_SQL-2_Transformation)
    * [The Node Type Index](#The_Node_Type_Index)
    * [Temporarily Disabling an Index](#Temporarily_Disabling_an_Index)
    * [The Deprecated Ordered Index](#The_Deprecated_Ordered_Index)
    * [Index Storage and Manual Inspection](#Index_Storage_and_Manual_Inspection)
    * [SQL-2 Optimisation](#SQL-2_Optimisation)
    * [Additional XPath and SQL-2 Features](#Additional_XPath_and_SQL-2_Features)
  

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

### Query Options

With query options, you can enforce the usage of indexes (failing the query if there is no index), 
and you can limit which indexes should be considered.

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

#### Query Option Index Tag

`@since Oak 1.7.4 (OAK-937)`

By default, queries will use the index with the lowest expected cost (as in relational databases).
But in rare cases, it is needed to specify which index(es) should be considered for a query:

* If there are multiple Lucene fulltext indexes with different aggregation rules,
  and the index data overlaps.
  In the query, you want to specify which aggregation rule to use.
* To temporarily work around limitations of the index implementation, 
  for example incorrect cost estimation.

Using index tags should be the exception, and should only be used temporarily.
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
  So if the expected cost of traversal is very low, the query will traverse.
  Note that traversal is never used for fulltext queries.

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

This is best configured via OSGi configuration (since Oak 1.6.x),
or as described in OAK-2977, since Oak 1.3.x:
When using Apache Sling, add the following line to the file `conf/sling.properties`,
and then restart the application:

    oak.query.fastResultSize=true

#### Quoting

[Special characters in queries need to be escaped.](https://wiki.apache.org/jackrabbit/EncodingAndEscaping)

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

### Full-Text Queries

The full-text syntax supported by Jackrabbit Oak is a superset of the JCR specification.

By default (that is, using a Lucene index with `compatVersion` 2), Jackrabbit Oak uses the 
[Apache Lucene grammar for fulltext search](https://lucene.apache.org/core/4_7_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html).
[See also how to escape queries.](https://wiki.apache.org/jackrabbit/EncodingAndEscaping)

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


### Native Queries

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
        spellchecks = row.getValue("rep:spellcheck()").getString()        
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
        spellchecks.add(row.getValue("rep:spellcheck()").getString());        
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
        suggestions = row.getValue("rep:suggest()").getString()        
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
        suggestions.add(row.getValue("rep:suggest()").getString());        
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
                    "where contains([jcr:title], 'oak');
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

    

    