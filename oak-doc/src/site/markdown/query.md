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

The query parser is now generally more strict about invalid syntax.
The following query used to work in Jackrabbit 2.x, but not in Oak,
because multiple way to quote the path are used at the same time:

    SELECT * FROM [nt:base] AS s 
    WHERE ISDESCENDANTNODE(s, ["/libs/sling/config"])
    
Instead, the query now needs to be:

    SELECT * FROM [nt:base] AS s 
    WHERE ISDESCENDANTNODE(s, [/libs/sling/config])
    
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


### The Lucene Full-Text Index

The full-text index handles only the 'contains' type of queries.

    //*[jcr:contains(., 'text')]

Not having a full-text index means that the full-text queries will not be able to work properly. Currently the query engine has a basic verification in place for full-text conditions, but that is brittle and can miss hits.

The full-text index update is asynchronous via a background thread, see `Oak#withAsyncIndexing`.
This means that some full-text searches will not work for a small window of time: the background thread runs every 5 seconds, plus the time is takes to run the diff and to run the text-extraction process. 

The async update status is now reflected on the `oak:index` node with the help of a few properties, see [OAK-980](https://issues.apache.org/jira/browse/OAK-980)

TODO Node aggregation [OAK-828](https://issues.apache.org/jira/browse/OAK-828)

The index definition node for a lucene-based full-text index:

* must be of type `oak:QueryIndexDefinition`
* must have the `type` property set to __`lucene`__
* must contain the `async` property set to the value `async`, this is what sends the index update process to a background thread

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


### The Solr Full-Text Index

`TODO`

### The Node Type Index

The `NodeTypeIndex` implements a `QueryIndex` using `PropertyIndexLookup`s on `jcr:primaryType` `jcr:mixinTypes` to evaluate a node type restriction on the filter.
The cost for this index is the sum of the costs of the `PropertyIndexLookup` for queries on `jcr:primaryType` and `jcr:mixinTypes`.

### Cost Calculation

Each query index is expected to estimate the worst-case cost to query with the given filter. 
The returned value is between 1 (very fast; lookup of a unique node) and the estimated number of entries to traverse, if the cursor would be fully read, and if there could in theory be one network round-trip or disk read operation per node (this method may return a lower number if the data is known to be fully in memory).

The returned value is supposed to be an estimate and doesn't have to be very accurate. Please note this method is called on each index whenever a query is run, so the method should be reasonably fast (not read any data itself, or at least not read too much data).

If an index implementation can not query the data, it has to return `Double.POSITIVE_INFINITY`.

