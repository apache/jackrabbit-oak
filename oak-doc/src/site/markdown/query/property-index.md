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

