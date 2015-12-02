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

## The Query Engine

Oak does not index as much content by default as does Jackrabbit 2. You need to create custom 
indexes when necessary, much like in traditional RDBMSs. If there is no index for a 
specific query, then the repository will be traversed. That is, the query will still 
work but probably be very slow.

Query Indices are defined under the `oak:index` node.

### Compatibility

#### Result Size

For NodeIterator.getSize(), some versions of Jackrabbit 2.x returned the estimated (raw)
Lucene result set size, including nodes that are not accessible. 
Oak does not do this; it either returns the correct result size, or -1.

#### Quoting

The query parser is now generally more strict about invalid syntax.
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

### Full-Text Queries

The full-text syntax supported by Jackrabbit Oak is a superset of the JCR specification.
The following syntax is supported within `contains` queries:

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
Unlike most queries, spellcheck queries won't return a JCR `Node` as the outcome of such queries will be text terms 
that come from content as written into JCR `properties`.
For example, the following query will return spellchecks for the (wrongly spelled) term `helo`:

    /jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())
    
The result of such a query will be a JCR `Row` which will contain the corrected terms, as spellchecked by the used underlying 
index, in a special property named `rep:spellcheck()`.

Clients wanting to obtain spellchecks could use the following JCR code:

`@until Oak 1.3.10` spellchecks are returned flat.
       
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
    
`@since Oak 1.3.11` each spellcheck would be returned per row.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    List<String> spellchecks = new LinkedList<String>();
    while (it.hasNext()) {
        spellchecks.add(row.getValue("rep:spellcheck()").getString());        
    }
    
If either Lucene or Solr were configured to provide the spellcheck feature, see [Enable spellchecking in Lucene](lucene.html#Spellchecking) and [Enable
spellchecking in Solr](solr.html#Spellchecking)

Note that spellcheck terms come already filtered according to calling user privileges, so that users could see spellcheck 
corrections only coming from indexed content they are allowed to read.

### Suggestions

`@since Oak 1.1.17, 1.0.15`

Oak supports search suggestions when using the Lucene or Solr indexes.
Unlike most queries, suggest queries won't return a JCR `Node` as the outcome of such queries will be text terms 
that come from content as written into JCR `properties`.
For example, the following query will return search suggestions for the (e.g. user entered) term `in `:

    /jcr:root[rep:suggest('in ')]/(rep:suggest())
    
The result of such a query will be a JCR `Row` which will contain the suggested terms, together with their score, as 
suggested and scored by the used underlying index, in a special property named `rep:suggest()`.

Clients wanting to obtain suggestions could use the following JCR code:

`@until Oak 1.3.10` suggestions are returned flat.
       
    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:suggest('in ')]/(rep:suggest())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    String suggestions = "";
    if (it.hasNext()) {
        suggestions = row.getValue("rep:suggest()").getString()        
    }
    
`@since Oak 1.3.11` each suggestion would be returned per row.

    QueryManager qm = ...;
    String xpath = "/jcr:root[rep:suggest('in ')]/(rep:suggest())";
    QueryResult result = qm.createQuery(xpath, Query.XPATH).execute();
    RowIterator it = result.getRows();
    List<String> suggestions = new LinkedList<String>();
    while (it.hasNext()) {
        suggestions.add(row.getValue("rep:suggest()").getString());        
    }
    
If either Lucene or Solr were configured to provide the suggestions feature, see [Enable suggestions in Lucene](lucene.html#Suggestions) and [Enable
suggestions in Solr](solr.html#Suggestions), the `suggestions` String would be have the following pattern `\[\{(term\=)[\w|\W]+(\,weight\=)\d+\}(\,\{(term\=)[\w|\W]+(\,weight\=)\d+\})*\]`, e.g.:

    [{term=in 2015 a red fox is still a fox,weight=1.5}, {term=in 2015 my fox is red, like mike's fox and john's fox,weight=0.7}]

Note that suggested terms come already filtered according to calling user privileges, so that users could see suggested
terms only coming from indexed content they are allowed to read.

### XPath to SQL2 Transformation

To support the XPath query language, such queries are internally converted to SQL2. 

Every conversion is logged in `debug` level under the 
`org.apache.jackrabbit.oak.query.QueryEngineImpl` logger:

    org.apache.jackrabbit.oak.query.QueryEngineImpl Parsing xpath statement: 
        //element(*)[@sling:resourceType = 'slingevent:Lock')]
    org.apache.jackrabbit.oak.query.QueryEngineImpl XPath > SQL2: 
        select [jcr:path], [jcr:score], * from [nt:base] as a 
        where [sling:resourceType] = 'slingevent:Lock' 
        /* xpath: //element(*)[@sling:resourceType = 'slingevent:Lock' 
        and @lock.created < xs:dateTime('2013-09-02T15:44:05.920+02:00')] */

_Each transformed SQL2 query contains the original XPath query as a comment._

When converting from XPath to SQL-2, `or` conditions are automatically converted to
`union` queries, so that indexes can be used for conditions of the form 
`a = 'x' or b = 'y'`.

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
is executed: when using a full-text index, and when using an "order by" clause.

<a name="property-index"></a>
### The Property Index

Is useful whenever there is a query with a property constraint that is not full-text:

    SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id

To define a property index, you have to add an index definition node that:

* Must be a child node of `/oak:index`.
* Must be of type `oak:QueryIndexDefinition`.
* `type` (String) must have the  property set to "property".
* `propertyNames` (Name, multi-valued):
    the  property to be indexed.
    This is a multi-valued property, and must not be empty.
    It usually contains only _one_ property name.
    All nodes that have _any_ of those properties are stored in this index.
* `includedPaths` (String, multi-valued):
    the paths that are included ('/' if not set).
    The index is only used if the query has a path restriction that is not excluded,
    and part of the included paths.
* `excludedPaths` (String, multi-valued):
    the paths where this index is excluded (none if not set).
    The index is only used if the query has a path restriction that is not excluded,
    and part of the included paths.
    
It is recommended to index one property per index.
(If multiple properties are indexed within one index, 
then the index contains all nodes that has either one of the properties,
which can make the query less efficient, and can make the query pick the wrong index.)

Optionally you can specify:

* `declaringNodeTypes` (Name, multi-valued): the index only applies to a certain node type.
* `unique` (Boolean): if set to `true`, a uniqueness constraint on this
  property is added. Ensure you set declaringNodeTypes, 
  otherwise all nodes of the repository are affected (which is most likely not what you want),
  and you are not able to version the node.
* `entryCount` (Long): the estimated number of path entries in the index, 
  to override the cost estimation (a high entry count means a high cost).
* `keyCount` (Long), the estimated number of keys in the index,
  to override the cost estimation (a high key count means a lower cost and
  a low key count means a high cost
  when searching for specific keys; has no effect when searching for "is not null").
* `reindex` (Boolean): if set to `true`, the full content is re-indexed.
  This can take a long time, and is run synchronously with storing the index
  (except with an async index). See "Reindexing" below for details.

Example:

    {
      NodeBuilder index = root.child("oak:index");
      index.child("uuid")
        .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
        .setProperty("type", "property")
        .setProperty("propertyNames", "jcr:uuid")
        .setProperty("declaringNodeTypes", "mix:referenceable")
        .setProperty("unique", true)
        .setProperty("reindex", true);
    }

or to simplify you can use one of the existing `IndexUtils#createIndexDefinition` helper methods:

    {
      NodeBuilder index = IndexUtils.getOrCreateOakIndex(root);
      IndexUtils.createIndexDefinition(index, "myProp", true, false, ImmutableList.of("myProp"), null);
    }

#### Reindexing

Reindexing a property index happens synchronously by setting the __`reindex`__ flag to __`true`__. This means that the 
first #save call will generate a full repository traversal with the purpose of building the index content and it might
take a long time.

Asynchronous reindexing of a property index is available as of OAK-1456. The way this works is by pushing the property 
index updates to a background job and when the indexing process is done, the property definition will be switched back 
to a synchronous updates mode.
To enable this async reindex behaviour you need to first set the __`reindex-async`__ and __`reindex`__ flags to 
__`true`__ (call #save). You can verify the initial setup worked by refreshing the index definition node and looking
for the __`async`__ = __`async-reindex`__ property.
Next you need to start the dedicated background job via a jmx call to the 
__`PropertyIndexAsyncReindex#startPropertyIndexAsyncReindex`__ MBean.

Example:

    {
      NodeBuilder index = root.child("oak:index");
      index.child("property")
        .setProperty("reindex-async", true)
        .setProperty("reindex", true);
    }

### The Lucene Index

See [Lucene Index](lucene.html) for details.

### The Solr Index

See [Solr Index](solr.html) for details.

### The Node Type Index

The `NodeTypeIndex` implements a `QueryIndex` using `PropertyIndexLookup`s on `jcr:primaryType` `jcr:mixinTypes` to evaluate a node type restriction on the filter.
The cost for this index is the sum of the costs of the `PropertyIndexLookup` for queries on `jcr:primaryType` and `jcr:mixinTypes`.

### Temporarily Disabling an Index

To temporarily disable an index (for example for testing), set the index type to `disabled`.
Please note that while the index type is not set, the index is not updated, so if you enable it again,
it might not be correct. This is specially important for synchronous indexes.

### The Ordered Index (deprecated since 1.1.8)

This index has been deprecated in favour of Lucene Property
index. Please refer to [Lucene Index documentation](lucene.html) for
details.

For help on migrating to a Lucece index please refer to:
[Migrate ordered index](ordered-index-migrate.html)

For historical information around the index please refer to:
[Ordered Index](ordered-index.html).

### Cost Calculation

Each query index is expected to estimate the worst-case cost to query with the given filter. 
The returned value is between 1 (very fast; lookup of a unique node) and the estimated number of entries to traverse, if the cursor would be fully read, and if there could in theory be one network round-trip or disk read operation per node (this method may return a lower number if the data is known to be fully in memory).

The returned value is supposed to be an estimate and doesn't have to be very accurate. Please note this method is called on each index whenever a query is run, so the method should be reasonably fast (not read any data itself, or at least not read too much data).

If an index implementation can not query the data, it has to return `Double.POSITIVE_INFINITY`.

### Index storage and manual inspection

Sometimes there is a need to inspect the index content for debugging (or pure curiosity).
The index content is generally stored as content under the index definition as hidden nodes (this doesn't apply to the solr index).
In order to be able to browse down into an index content you need a low level repository tool that allows NodeStore level access.
There are currently 2 options: the oak-console (command line tool, works will all existing NodeStore implementations) and the oak-explorer
(gui based on java swing, works only on the TarMK), both available as run modes of the [oak-run](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-run/README.md) module

The structure of the index is specific to each implementation and is subject to change. What is worth mentioning is that all the _*PropertyIndex_
flavors store the content as unstructured nodes (clear readable text), the _Lucene_ index is stored as binaries, so one would need to export the
entire Lucene directory to the local file system and browse it using a dedicated tool.

### SQL2 Optimisation

    @since 1.3.9 with -Doak.query.sql2optimisation

if enabled by providing `-Doak.query.sql2optimisation` at start-up, it
will perform a round of optimisation on the `Query` object obtained
after parsing a SQL2 statement. It will for example attempt a
conversion of OR conditions into UNION (OAK-1617).