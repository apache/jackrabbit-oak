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

Oak does not index content by default as does Jackrabbit 2. You need to create custom 
indexes when necessary, much like in traditional RDBMSs. If there is no index for a 
specific query, then the repository will be traversed. That is, the query will still 
work but probably be very slow.

Query Indices are defined under the `oak:index` node.

### Compatibility

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

    *WARN* Traversed 1000 nodes with filter Filter(query=select ...)
    consider creating an index or changing the query

If this is the case, an index might need to be created, or the condition 
of the query might need to be changed to take advantage of an existing index.

If a query reads more than 10 thousand nodes in memory, then the query is cancelled
with an UnsupportedOperationException saying that 
"The query read more than 10000 nodes in memory. To avoid running out of memory, processing was stopped."
As a workaround, this limit can be changed using the system property "oak.queryLimitInMemory".

If a query traversed more than 100 thousand nodes (for example because there is no index
at all and the whole repository is traversed), then the query is cancelled
with an UnsupportedOperationException saying that 
"The query read or traversed more than 10000 nodes. To avoid affecting other tasks, processing was stopped.".
As a workaround, this limit can be changed using the system property "oak.queryLimitReads".

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

Oak supports similarity queries when using the Lucene full-text index. 
For example, the following query will return nodes that have similar content than
the node /test/a:

    //element(*, nt:base)[rep:similar(., '/test/a')]
    
Compared to Jackrabbit 2.x, support for rep:similar has the following limitations:
Full-text aggregation is not currently supported.

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

### The Property Index

Is useful whenever there is a query with a property constraint that is not full-text:

    SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id

To define a property index on a subtree you have to add an index definition node that:

* must be of type `oak:QueryIndexDefinition`
* must have the `type` property set to __`property`__
* contains the `propertyNames` property that indicates what properties will be stored in the index.
  `propertyNames` can be a list of properties, and it is optional.in case it is missing, the node name will be used as a property name reference value

_Optionally_ you can specify

* a uniqueness constraint on a property index by setting the `unique` flag to `true`
* that the property index only applies to a certain node type by setting the `declaringNodeTypes` property
* the `reindex` flag which when set to `true`, triggers a full content re-index.

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

### The Ordered Index

Extension of the Property index will keep the order of the indexed
property persisted in the repository.

Used to speed-up queries with `ORDER BY` clause, _equality_ and
_range_ ones.

    SELECT * FROM [nt:base] ORDER BY jcr:lastModified
    
    SELECT * FROM [nt:base] WHERE jcr:lastModified > $date
    
    SELECT * FROM [nt:base] WHERE jcr:lastModified < $date
    
    SELECT * FROM [nt:base]
    WHERE jcr:lastModified > $date1 AND jcr:lastModified < $date2

    SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id

To define a property index on a subtree you have to add an index
definition node that:

* must be of type `oak:QueryIndexDefinition`
* must have the `type` property set to __`ordered`__
* contains the `propertyNames` property that indicates what properties
  will be stored in the index.  `propertyNames` has to be a single
  value list of type `Name[]`

_Optionally_ you can specify

* the `reindex` flag which when set to `true`, triggers a full content
  re-index.
* The direction of the sorting by specifying a `direction` property of
  type `String` of value `ascending` or `descending`. If not provided
  `ascending` is the default.
* The index can be defined as asynchronous by providing the property
  `async=async`

_Caveats_

* In case deploying on the index on a clustered mongodb you have to
  define it as asynchronous by providing `async=async` in the index
  definition. This is to avoid cluster merges.

### The Lucene Full-Text Index

The full-text index handles the 'contains' type of queries:

    //*[jcr:contains(., 'text')]

If a full-text index is configured, then all queries that have a full-text condition
use the full-text index, no matter if there are other conditions that are indexed,
and no matter if there is a path restriction.

If no full-text index is configured, then queries with full-text conditions
may not work as expected. (The query engine has a basic verification in place 
for full-text conditions, but it does not support all features that Lucene does,
and it traverses all nodes if there are no indexed constraints).

The full-text index update is asynchronous via a background thread, 
see `Oak#withAsyncIndexing`.
This means that some full-text searches will not work for a small window of time: 
the background thread runs every 5 seconds, plus the time is takes to run the diff 
and to run the text-extraction process. 

The async update status is now reflected on the `oak:index` node with the help of 
a few properties, see [OAK-980](https://issues.apache.org/jira/browse/OAK-980)

TODO Node aggregation [OAK-828](https://issues.apache.org/jira/browse/OAK-828)

The index definition node for a lucene-based full-text index:

* must be of type `oak:QueryIndexDefinition`
* must have the `type` property set to __`lucene`__
* must contain the `async` property set to the value `async`, this is what sends the 
index update process to a background thread

_Optionally_ you can add

 * what subset of property types to be included in the index via the `includePropertyTypes` property
 * a blacklist of property names: what property to be excluded from the index via the `excludePropertyNames` property
 * the `reindex` flag which when set to `true`, triggers a full content re-index.

Example:

    {
      NodeBuilder index = root.child("oak:index");
      index.child("lucene")
        .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
        .setProperty("type", "lucene")
        .setProperty("async", "async")
        .setProperty(PropertyStates.createProperty("includePropertyTypes", ImmutableSet.of(
            PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_BINARY), Type.STRINGS))
        .setProperty(PropertyStates.createProperty("excludePropertyNames", ImmutableSet.of( 
            "jcr:createdBy", "jcr:lastModifiedBy"), Type.STRINGS))
        .setProperty("reindex", true);
    }


### The Solr Index

The Solr index is mainly meant for full-text search (the 'contains' type of queries):

    //*[jcr:contains(., 'text')]

but is also able to search by path, property restrictions and primary type restrictions.
This means the Solr index in Oak can be used for any type of JCR query.

Even if it's not just a full-text index, it's recommended to use it asynchronously (see `Oak#withAsyncIndexing`)
because, in most production scenarios, it'll be a 'remote' index, and therefore network eventual latency / errors would 
have less impact on the repository performance.
To set up the Solr index to be asynchronous that has to be defined inside the index definition, see [OAK-980](https://issues.apache.org/jira/browse/OAK-980)

TODO Node aggregation.

##### Index definition for Solr index

The index definition node for a Solr-based index:

 * must be of type `oak:QueryIndexDefinition`
 * must have the `type` property set to __`solr`__
 * must contain the `async` property set to the value `async`, this is what sends the 

index update process to a background thread.
_Optionally_ one can add

 * the `reindex` flag which when set to `true`, triggers a full content re-index.

Example:

    {
      NodeBuilder index = root.child("oak:index");
      index.child("solr")
        .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
        .setProperty("type", "solr")
        .setProperty("async", "async")
        .setProperty("reindex", true);
    }
    
#### Setting up the Solr server
For the Solr index to work Oak needs to be able to communicate with a Solr instance / cluster.
Apache Solr supports multiple deployment architectures: 

 * embedded Solr instance running in the same JVM the client runs into
 * single remote instance
 * master / slave architecture, eventually with multiple shards and replicas
 * SolrCloud cluster, with Zookeeper instance(s) to control a dynamic, resilient set of Solr instances for high 
 availability and fault tolerance

The Oak Solr index can be configured to use an 'embedded Solr server' or either a 'remote Solr server' (being able to 
connect to a single remote instance or to a SolrCloud cluster via Zookeeper).

##### OSGi environment
TODO

##### non OSGi environment
TODO

#### Differences with the Lucene index
As of Oak version 1.0.0:

* Solr index doesn't support search using relative properties, see [OAK-1835](https://issues.apache.org/jira/browse/OAK-1835).
* Solr configuration is mostly done on the Solr side via schema.xml / solrconfig.xml files.
* Lucene can only be used for full-text queries, Solr can be used for full-text search _and_ for JCR queries involving
path, property and primary type restrictions

### The Node Type Index

The `NodeTypeIndex` implements a `QueryIndex` using `PropertyIndexLookup`s on `jcr:primaryType` `jcr:mixinTypes` to evaluate a node type restriction on the filter.
The cost for this index is the sum of the costs of the `PropertyIndexLookup` for queries on `jcr:primaryType` and `jcr:mixinTypes`.

### Cost Calculation

Each query index is expected to estimate the worst-case cost to query with the given filter. 
The returned value is between 1 (very fast; lookup of a unique node) and the estimated number of entries to traverse, if the cursor would be fully read, and if there could in theory be one network round-trip or disk read operation per node (this method may return a lower number if the data is known to be fully in memory).

The returned value is supposed to be an estimate and doesn't have to be very accurate. Please note this method is called on each index whenever a query is run, so the method should be reasonably fast (not read any data itself, or at least not read too much data).

If an index implementation can not query the data, it has to return `Double.POSITIVE_INFINITY`.

