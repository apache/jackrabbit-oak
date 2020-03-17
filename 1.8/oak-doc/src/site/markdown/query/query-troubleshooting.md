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

## Query Troubleshooting

### Slow Queries

The first step in query troubleshooting is often to detect a query is slow,
or traverses many nodes. Queries that traverse many nodes are logged
as follows:

    *WARN* org.apache.jackrabbit.oak.plugins.index.Cursors$TraversingCursor 
        Traversed 22000 nodes with filter Filter(query=
        select * from [nt:base] where isdescendantnode('/etc') and lower([jcr:title]) like '%coat%');
        consider creating an index or changing the query

To get good performance, queries should not traverse more than about 1000 nodes
(specially for queries that are run often).

#### Potentially Slow Queries

In addition to avoiding queries that traverse many nodes, 
it makes sense to avoid queries that don't use an index.
Such queries might be fast (and only traverse few nodes) with a small repository,
but with a large repository they are typically slow as well.
Therefore, it makes sense to detect such queries as soon as possible
(in a developer environment), 
even before the code that runs those queries is tested with a larger repository.
Oak will detect such queries and log them as follows 
(with log level INFO for Oak 1.6.x, and WARN for Oak 1.8.x):

    *INFO* org.apache.jackrabbit.oak.query.QueryImpl Traversal query (query without index): 
        select * from [nt:base] where isdescendantnode('/etc') and lower([jcr:title]) like '%coat%'; 
        consider creating an index

#### Query Plan

To understand why the query is slow, the first step is commonly to get the
query execution plan. To do this, the query can be executed using `explain select ...`.
For the above case, the plan is:

    [nt:base] as [nt:base] /* traverse "/etc//*" 
    where (isdescendantnode([nt:base], [/etc])) and (lower([nt:base].[jcr:title]) like '%coat%') */
    
That means, all nodes below `/etc` are traversed.

#### Making the Query More Specific

In order to make the query faster, try to add more constraints, or make constraints tighter.
This will usually require some knowledge about the expected results.
For example, if the path restriction is more specific, then less nodes need to be read.
This is also true if an index is used. Also, if possible use a more specific node type.
To understand if a nodetype or mixin is indexed, consult the nodetype index
at `/oak:index/nodetype`, property `	declaringNodeTypes`.
But even if this is not the case, the nodetype should be as specific as possible.
Assuming the query is changed to this:

    select * from [acme:Product] 
    where isdescendantnode('/etc/commerce') 
    and lower([jcr:title]) like '%coat%')
    and [commerceType] = 'product'

The only _relevant_ change was to improve the path restriction.
But in this case, it already was enough to make the traversal warning go away.

#### Queries Without Index

After changing the query, 
there is still a message in the log file that complains the query doesn't use an index,
as described above:

    *INFO* org.apache.jackrabbit.oak.query.QueryImpl 
        Traversal query (query without index): 
        select * from [acme:Product] where isdescendantnode('/etc/commerce') 
        and lower([jcr:title]) like '%coat%'
        and [commerceType] = 'product'; consider creating an index

The query plan of the index didn't change, so still nodes are traversed.
In this case, there are relatively few nodes because it's 
an almost empty development repository, so no traversal warning is logged.
But for production, there might be a lot more nodes under `/etc/commerce`, 
so it makes sense to continue optimization.

#### Where Traversal is OK

If it is known from the data model that a query will never traverse many nodes,
then no index is needed. This is a corner case, and only applies to queries that 
traverse a fixed number of (for example) configuration nodes, or
if the number of descendant nodes is guaranteed to be very low by using 
a certain nodetype that only allows for a fixed number of child nodes.
If this is the case, then the query can be changed to say traversal is fine.
To mark such queries, append `option(traversal ok)` to the query.
This feature should only be used for those rare corner cases.

    select * from [nt:base] 
    where isdescendantnode('/etc/commerce') 
    and lower([jcr:title]) like '%coat%'
    and [commerceType] = 'product'
    option(traversal ok)

#### Estimating Node Counts

To find out how many nodes are in a certain path, you can use the JMX bean `NodeCounter`,
which can estimate the node count. Example: run
`getEstimatedChildNodeCounts` with `p1=/` and `p2=2` might give you:

    /: 2522208,
    ...
    /etc: 1521504,
    /etc/commerce: 29216,
    /etc/images: 1231232,
    ...

So in this case, there are still many nodes below `/etc/commerce` in the production repository. 
Also note that the number of nodes can grow over time.

#### Prevent Running Traversal Queries

To avoid running queries that don't use an index altogether,
you can change the configuration in the JMX bean `QueryEngineSettings`:
if you set `FailTraversal` to `true`, then queries without index will throw an exception
when trying to execute them, no matter how many nodes are in the repository.
This doesn't mean queries will never traverse over nodes, it just means
that queries that _must_ traverse over nodes will fail.

#### Using a Different or New Index

There are multiple options:

* Consider creating an index for `jcr:title`. But for `like '%..%'` conditions,
  this is not of much help, because all nodes with that property will need to be read.
  Also, using `lower` will make the index less effective.
  So, this only makes sense if there are very few nodes with this property
  expected to be in the system.
* If there are very few nodes with that nodetype,
  consider adding `acme:Product` to the nodetype index. This requires reindexing.
  The query could then use the nodetype index, and within this nodetype,
  just traverse below `/etc/commerce`.
  The `NodeCounter` can also help understand how many `acme:Product`
  nodes are in the repository, if this nodetype is indexed.
  To find out, run `getEstimatedChildNodeCounts` with
  `p1=/oak:index/nodetype` and `p2=2`.
* If the query needs to return added nodes immediately (synchronously; that is without delay),
  consider creating a [property index](./property-index.html).
  Note that Lucene indexes are asynchronous, and new nodes may not
  appear in the result for a few seconds.
* To ensure there is only one node matching the result in the repository,
  consider creating a unique [property index](./property-index.html).
* Consider using a fulltext index, that is: change the query from using 
  `lower([jcr:title]) like '%...%'` to using `contains([jcr:title], '...')`.
  Possibly combine this with adding the property
  `commerceType` to the fulltext index.

The last plan is possibly the best solution for this case.

#### Index Definition Generator

In case you need to modify or create a Lucene property index,
you can use the [Oak Index Definition Generator](http://oakutils.appspot.com/generate/index) tool.

As the tool doesn't know your index configuration, it will always suggest
to create a new index; it might be better to extend an existing index.
However, note that:

* Changing an existing index requires reindexing that index.
* If an out-of-the-box index is modified, you will need to merge those modifications 
  when migrating to newer software.
  It is best to add documentation to the index definition to simplify merging,
  for example in the form of "info" properties.

#### Verification

After changing the query, and possibly the index, run the `explain select` again,
and verify the right plan is used, in this case that might be, for the query:

    select * from [acme:Product] 
    where isdescendantnode('/etc/commerce') 
    and contains([jcr:title], 'Coat')
    and [commerceType] = 'product'

    [nt:unstructured] as [acme:Product] /* lucene:lucene(/oak:index/lucene) 
    full:jcr:title:coat ft:(jcr:title:"Coat")

So in this case, only the fulltext restriction of the query was used by the index,
but this might already be sufficient. If it is not, then the fulltext index might
be changed to also index `commerceType`, or possibly 
to use `evaluatePathRestrictions`.
