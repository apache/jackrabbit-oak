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

## The Property Index

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
  * `includedPaths` (String, multi-valued):
    the paths that are included ('/' if not set).
    Since Oak version 1.4 (OAK-3263).
    The index is only used if the query has a path restriction that is not excluded,
    and part of the included paths.
* `excludedPaths` (String, multi-valued):
    the paths where this index is excluded (none if not set).
    Since Oak version 1.4 (OAK-3263).
    The index is only used if the query has a path restriction that is not excluded,
    and part of the included paths.
* `valuePattern` (String)
    A regular expression of all indexed values.
    The index is used for equality conditions where the value matches the pattern,
    and for "in(...)" queries where all values match the pattern.
    The index is not used for "like" conditions.
    Since Oak version 1.7.2 (OAK-4637).
* `valueExcludedPrefixes`
    The index is used for equality conditions where the value does not start with the given prefix,
    and the prefix does not start with the value, 
    similarly for "in(...)" conditions,
    and similarly for "like" conditions.
    and for "in(...)" queries where all values match the pattern.
    Since Oak version 1.7.2 (OAK-4637).
* `valueIncludedPrefixes`
    The index is used for equality conditions where the value starts with the given prefix,
    similarly for "in(...)" conditions,
    and similarly for "like" conditions.
    Since Oak version 1.7.2 (OAK-4637).
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

#### <a name="reindexing"></a> Reindexing

Usually, reindexing is only needed if the configuration of an index is changed, 
such that the index should contain more or different data.
For example, reindexing is needed if the property to be indexed is changed, 
if a nodetype is added to __`declaringNodeTypes`__, or if __`includedPaths`__ is changed.
It is not strictly needed if less data is to be indexed, for example if a nodetype is removed.
However, to save space, it might make sense to reindex even in that case.
Typically, if a query does not return the expected result, reindexing does not help;
more likely, the reason in somewhere else to be found, and disabling the index should be tried first.

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

When recovering a failed async reindex special care needs to be taken wrt. the created checkpoint and the __`async`__ property.
The checkpoint should be released via the __`CheckpointManager`__ mbean, and the __`async`__ property needs to be manually deleted 
while also setting the __`reindex`__ flags to __`true`__ to make sure the index returns to a consistent state, in sync with the head revision.

#### Cost Estimation

When running a query, the property index reports its estimated cost to the query engine,
and then the query engine picks the index with the lowest cost (cost-based query optimization).
The algorithm to calculate the estimated cost is roughly as follows (a bit simplified):

* The cost is infinity (so the index is never used) 
  if the condition contains a fulltext constraint, 
  no applicable restriction,
  the wrong nodetype, or
  if the path filtering (`includedPaths` / `excludedPaths`) does not match the query.
* For the nodetype index, the cost is the sum of the cost for the `jcr:primaryType` lookup
  (if the primary type is known),
  plus the cost for the `jcr:mixinTypes` lookup (if that is known).
* Otherwise, the cost is based on the overhead (which is 2), 
  plus the estimated number of entries.
* For an "x is not null" condition, 
  the estimated number of entries is
  either the configured `entryCount` or, if not set, the 
  approximate number of entries in the index.
  The approximation is an "order of magnitude" estimation (Morris' algorithm).
* For a unique index and "x = 1" condition, 
  the estimated number of entries is either 0 or 1 
  (depending on whether the key is found).
* For a non-unique index and a "x = 1" condition,
  if the `entryCount` and `keyCount` are set, those setting are used to estimate
  the number of entries. If not, the 
  approximate number of entries for the key is read (maintained using Morris’ algorithm).
  In addition to that, the path condition is used to scale down
  the estimated count depending on the approximate number of nodes
  in that subtree versus the approximate number of entries
  in the repository, using approximation available via the `counter` index.

For example, for a query with path restriction "/content/products/t-shirts" and property restriction
"color = 'red'", if there is an index for the property "color", then
the entry count approximation is read from the index. Let's say it is 10'000 for this value.
Then the approximate number of nodes in the subtree "/content/products/t-shirts" is read 
(let's say it is 20'000), and the approximate number of nodes in the repository 
(let's say it is 1 million).
Therefore, the estimated number of entries is scaled down (divided by 50) from 10'000 to 200.
The estimated cost is therefore 202, due to the overhead of 2.