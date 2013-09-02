## Query

Oak does not index content by default as does Jackrabbit 2. You need to create custom indexes when
necessary, much like in traditional RDBMSs. If there is no index for a specific query then the
repository will be traversed. That is, the query will still work but probably be very slow.

Query Indices are defined under the `oak:index` node.

### Cost calculation

Each query index is expected to estimate the worst-case cost to query with the given filter. 
The returned value is between 1 (very fast; lookup of a unique node) and the estimated number of entries to traverse, if the cursor would be fully read, and if there could in theory be one network round-trip or disk read operation per node (this method may return a lower number if the data is known to be fully in memory).

The returned value is supposed to be an estimate and doesn't have to be very accurate. Please note this method is called on each index whenever a query is run, so the method should be reasonably fast (not read any data itself, or at least not read too much data).

If an index implementation can not query the data, it has to return `Double.POSITIVE_INFINITY`.

### Property index

To define a property index on a subtree you have to add an index definition node that:

* must be of type `oak:queryIndexDefinition`
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
        .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
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


### Node type index

The `NodeTypeIndex` implements a `QueryIndex` using `PropertyIndexLookup`s on `jcr:primaryType` `jcr:mixinTypes` to evaluate a node type restriction on the filter.
The cost for this index is the sum of the costs of the `PropertyIndexLookup` for queries on `jcr:primaryType` and `jcr:mixinTypes`.


### Lucene full-text index

The full-text index update is asynchronous via a background thread, see `Oak#withAsyncIndexing`.

This means that some full-text searches will not work for a small window of time: the background thread runs every 5 seconds, plus the time is takes to run the diff and to run the text-extraction process. The async update status is now reflected on the `oak:index` node with the help of a few properties, see [OAK-980](https://issues.apache.org/jira/browse/OAK-980)

TODO Node aggregation [OAK-828](https://issues.apache.org/jira/browse/OAK-828)

The index definition node for a lucene-based full-text index:

* must be of type `oak:queryIndexDefinition`
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
        .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
        .setProperty("type", "lucene")
        .setProperty("async", "async")
        .setProperty(PropertyStates.createProperty("includePropertyTypes", ImmutableSet.of(
            PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_BINARY), Type.STRINGS))
        .setProperty(PropertyStates.createProperty("excludePropertyNames", ImmutableSet.of( 
            "jcr:createdBy", "jcr:lastModifiedBy"), Type.STRINGS))
        .setProperty("reindex", true);
    }


### Solr full-text index

`TODO`
