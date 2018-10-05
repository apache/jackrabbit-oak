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
  
## Solr Index

The Solr index is mainly meant for full-text search (the 'contains' type of queries):

    //*[jcr:contains(., 'text')]

but is also able to search by path and property restrictions.
Primary type restriction support is also provided by it's not recommended as it's usually much better to use the [node type 
index](query.html#The_Node_Type_Index) for such kind of queries.

Even if it's not just a full-text index, it's recommended to use it asynchronously (see `Oak#withAsyncIndexing`)
because, in most production scenarios, it'll be a 'remote' index and therefore network latency / errors would 
have less impact on the repository performance.

The index definition node for a Solr-based index:

 * must be of type `oak:QueryIndexDefinition`
 * must have the `type` property set to __`solr`__
 * must contain the `async` property set to the value `async`, this is what sends the index update process to a background thread.

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
        
The Oak Solr index creates one document in the Solr index for each node in the repository, each of such documents has 
usually at least a field for each property associated with the related node.
Indexing of properties can be done by name: e.g. property 'jcr:title' of a node is written into a field 'jcr:title' of 
the corresponding Solr document in the index, or by type: e.g. properties 'jcr:data' and 'binary_content' of type 
_binary_ are written into a field 'binary_data' that's responsible for the indexing of all fields having that type and 
thus properly configured for hosting such type of data.
    
### Configuring the Solr index

Besides the index definition parameters mentioned above, a number of additional parameters can be defined in 
 Oak Solr index configuration.
Such a configuration is composed by:

 - the search / indexing configuration (see [OakSolrConfiguration](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/configuration/OakSolrConfiguration.html))
 - the Solr server configuration (see [SolrServerConfiguration](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/configuration/SolrServerConfiguration.html))
 
### Search / indexing configuration options

Such options define how Oak handles search and indexing requests in order to properly delegate such operations to Solr.

#### Use for property restrictions

If set to _true_ the Solr index will be used also for filtering nodes by property restrictions.

Default is 'false'.

#### Use for path restrictions

If set to _true_ the Solr index will be used also for filtering nodes by path restrictions.

Default is 'false'.

#### Use for primary types

If set to _true_ the Solr index will be used also for filtering nodes by primary type.

Default is 'false'.

#### Path field

The name of the field to be used for searching an exact match of a certain path.

Default is 'path_exact'.

#### Catch all field

The name of the field to be used for searching when no specific field is defined in the search query (e.g. user entered 
queries like 'foo bar').

Default is 'catch_all'.

Default Solr schema.xml provided with Oak Solr index contains a copyField from everything to 'catch_all', that causing
all the properties of a certain node to be indexed into that field (as separate values) therefore a query run against 
that field would match if any of the properties of the original node would have matched such a query.

#### Descendant path field

The name of the field to be used for searching for nodes descendants of a certain node.

Default is 'path_des'.

E.g. The Solr query to find all the descendant nodes of /a/b would be 'path_des:\/a\/b'.

#### Children path field

The name of the field to be used for searching for child nodes of a certain node.

Default is 'path_child'.

E.g. The Solr query to find all the child nodes of /a/b would be 'path_child:\/a\/b'.

#### Parent path field

The name of the field to be used for searching for parent node of a certain node.

Default is 'path_anc'.

E.g. The Solr query to find the parent node of /a/b would be 'path_anc:\/a\/b'.

#### Property restriction fields

The (optional) mapping of property names into Solr fields, so that a mapping jcr:title=foo is defined each node having 
 the property jcr:title will have its correspondant Solr document having a property foo indexed with the value of the 
 jcr:title property.

Default is no mapping, therefore the default mechanism of mapping property names to field names is performed.

#### Used properties

A whitelist of properties to be used for indexing / searching by Solr index.
Such a whitelist, if not empty, would dominate whatever configuration defined for the [Ignored_properties](#Ignored_properties).

Default is an empty list.

E.g. If such a whitelist contains properties _jcr:title_ and _text_ the Solr index will only index such properties for each
node and will be possible to use it for searching only on those two properties.

#### Ignored properties
A blacklist of properties to be ignored while indexing and searching by the Solr index.

Such a blacklist makes sense (it will be taken into account by the Solr index) only if the [Used properties](#Used_properties)
 option doesn't have any value.

Default is the following array: _("rep:members", "rep:authorizableId", "jcr:uuid", "rep:principalName", "rep:password"}_.

#### Commit policy

The Solr commit policy to be used when indexing nodes as documents in Solr.

Possible values are 'SOFT', 'HARD', 'AUTO'.

SOFT: perform a Solr soft-commit for each indexed document.

HARD: perform a Solr (hard) commit for each indexed document.

AUTO: doesn't perform any commit and relies on auto commit being configured on plain Solr's configuration (solrconfig.xml).

Default is _SOFT_.

#### Rows

The number of documents per 'page' to be fetched for each query.

Default is _Integer.MAX_VALUE_ (was _50_ in Oak 1.0).

#### Collapse _jcr:content_ nodes

`@since 1.3.4, 1.2.4, 1.0.18`

Whether the [Collapsing query parser](https://cwiki.apache.org/confluence/display/solr/Collapse+and+Expand+Results) should
be used when searching in order to collapse nodes that are descendants of 'jcr:content' nodes into the 'jcr:content' node only.
E.g. if normal query results would include '/a/jcr:content' and '/a/jcr:content/b/', with this option enabled only 
'/a/jcr:content' would be returned by Solr using the Collapsing query parser.
This feature requires an additional field to be indexed, therefore if this is turned on, reindexing should be triggered
in order to make it work properly.  

#### Collapsed path field 

`@since 1.3.4, 1.2.4, 1.0.18`

The name of the field used for collapsing descendants of jcr:content nodes, see 'Collapse jcr:content nodes' option.

##### Solr server configuration options

TBD
    
#### Setting up the Solr server

For the Solr index to work Oak needs to be able to communicate with a Solr instance / cluster.
Apache Solr supports multiple deployment architectures: 

 * embedded Solr instance running in the same JVM the client runs into
 * single remote instance
 * master / slave architecture, eventually with multiple shards and replicas
 * SolrCloud cluster, with Zookeeper instance(s) to control a dynamic, resilient set of Solr servers for high 
 availability and fault tolerance

The Oak Solr index can be configured to either use an 'embedded Solr server' or a 'remote Solr server' (being able to 
connect to a single remote instance or to a SolrCloud cluster via Zookeeper).

##### Supported Solr deployments
Depending on the use case, different Solr server deployments are recommended.

###### Embedded Solr server
The embedded Solr server is recommended for developing and testing the Solr index for an Oak repository. With that an 
in-memory Solr instance is started in the same JVM of the Oak repository, without HTTP bindings (for security purposes 
as it'd allow HTTP access to repository data independently of ACLs). 
Configuring an embedded Solr server mainly consists of providing the path to a standard [Solr home dir](https://wiki.apache.org/solr/SolrTerminology) 
(_solr.home.path_ Oak property) to be used to start Solr; this path can be either relative or absolute, if such a path 
would not exist then the default configuration provided with _oak-solr-core_ artifact would be put in the given path.
To start an embedded Solr server with a custom configuration (e.g. different schema.xml / solrconfig.xml than the default
 ones) the (modified) Solr home files would have to be put in a dedicated directory, according to Solr home structure, so 
 that the solr.home.path property can be pointed to that directory.

###### Single remote Solr server
A single (remote) Solr instance is the simplest possible setup for using the Oak Solr index in a production environment. 
Oak will communicate to such a Solr server through Solr's HTTP APIs (via [SolrJ](http://wiki.apache.org/solr/Solrj) client).
Configuring a single remote Solr instance consists of providing the URL to connect to in order to reach the [Solr core]
(https://wiki.apache.org/solr/SolrTerminology) that will host the Solr index for the Oak repository via the _solr.http.url_
 property which will have to contain such a URL (e.g. _http://10.10.1.101:8983/solr/oak_). 
All the configuration and tuning of Solr, other than what's described on this page, will have to be performed on the 
Solr side; [sample Solr configuration](http://svn.apache.org/viewvc/jackrabbit/oak/trunk/oak-solr-core/src/main/resources/solr/) 
files (schema.xml, solrconfig.xml, etc.) to start with can be found in _oak-solr-core_ artifact.

###### SolrCloud cluster
A [SolrCloud](https://cwiki.apache.org/confluence/display/solr/SolrCloud) cluster is the recommended setup for an Oak 
Solr index in production as it provides a scalable and fault tolerant architecture.
In order to configure a SolrCloud cluster the host of the Zookeeper instance / ensemble managing the Solr servers has 
to be provided in the _solr.zk.host_ property (e.g. _10.1.1.108:9983_) since the SolrJ client for SolrCloud communicates 
directly with Zookeeper.
The [Solr collection](https://wiki.apache.org/solr/SolrTerminology) to be used within Oak is named _oak_, having a replication
 factor of 2 and using 2 shards; this means in the default setup the SolrCloud cluster would have to be composed by at 
 least 4 Solr servers as the index will be split into 2 shards and each shard will have 2 replicas.
SolrCloud also allows the hot deploy of configuration files to be used for a certain collection so while setting up the 
 collection to be used for Oak with the needed files before starting the cluster, configuration files can also be uploaded 
 from a local directory, this is controlled by the _solr.conf.dir_ property of the 'Oak Solr remote server configuration'.
For a detailed description of how SolrCloud works see the [Solr reference guide](https://cwiki.apache.org/confluence/display/solr/SolrCloud).

##### OSGi environment
Create an index definition for the Solr index, as described [above](#Solr_index).
Once the query index definition node has been created, access OSGi ConfigurationAdmin via e.g. Apache Felix WebConsole:

 1. find the 'Oak Solr indexing / search configuration' item and eventually change configuration properties as needed
 2. find either the 'Oak Solr embedded server configuration' or 'Oak Solr remote server configuration' items depending 
 on the chosen Solr architecture and eventually change configuration properties as needed
 3. find the 'Oak Solr server provider' item and select the chosen provider ('remote' or 'embedded') 

#### Advanced search features

##### Aggregation

`@since Oak 1.1.4, 1.0.13`

Solr index supports query time aggregation, that can be enabled in OSGi by setting `SolrQueryIndexProviderService` service 
property `query.aggregation` to true.       
       
##### Suggestions

`@since Oak 1.1.17, 1.0.15`

Default Solr configuration ([solrconfig.xml](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/resources/solr/oak/conf/solrconfig.xml#L1102) 
and [schema.xml](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/resources/solr/oak/conf/schema.xml#L119)) 
comes with a preconfigured suggest component, which uses Lucene's [FuzzySuggester](https://lucene.apache.org/core/4_7_0/suggest/org/apache/lucene/search/suggest/analyzing/FuzzySuggester.html)
under the hood. Updating the suggester in [default configuration](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/resources/solr/oak/conf/solrconfig.xml#L1110) 
is done every time a `commit` request is sent to Solr however it's recommended not to do that in production systems if possible, 
as it's much better to send explicit request to Solr to rebuild the suggester dictionary, e.g. once a day, week, etc.

More / different suggesters can be configured in Solr, as per [reference documentation](https://cwiki.apache.org/confluence/display/solr/Suggester).

##### Spellchecking

`@since Oak 1.1.17, 1.0.13`

Default Solr configuration ([solrconfig.xml](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/resources/solr/oak/conf/solrconfig.xml#L1177)) 
comes with a preconfigured spellchecking component, which uses Lucene's [DirectSpellChecker](http://lucene.apache.org/core/4_7_0/suggest/org/apache/lucene/search/spell/DirectSpellChecker.html)
under the hood as it doesn't require any additional data structure neither in RAM nor on disk.

More / different spellcheckers can be configured in Solr, as per [reference documentation](https://cwiki.apache.org/confluence/display/solr/Spell+Checking).

#### Facets

`@since Oak 1.3.14`

In order to enable proper usage of facets in Solr index the following dynamic field needs to be added to the _schema.xml_

        <dynamicField name="*_facet" type="string" indexed="false" stored="false" docValues="true" multiValued="true"/>

with dedicated _copyFields_ for specific properties.

        <copyField source="jcr:primaryType" dest="jcr:primaryType_facet"/> <!-- facet on jcr:primaryType field/property -->

#### Persisted configuration

`@since Oak 1.4.0`

It's possible to create (multiple) Solr indexes via persisted configuration.
A persisted Oak Solr index is created whenever an index definition with _type = solr_ has a child node named _server_ 
and such a child node has the _solrServerType_ property set (to either _embedded_ or _remote_).
If no such child node exists, an Oak Solr index will be only created upon explicit registration of a [SolrServerProvider]
 e.g. via OSGi.
All the [OakSolrConfiguration](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/configuration/OakSolrConfiguration.html)
 and [SolrServerConfiguration](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/configuration/SolrServerConfiguration.html)
 properties are exposed and configurable, see also [OakSolrNodeStateConfiguration#Properties](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/configuration/nodestate/OakSolrNodeStateConfiguration.java#L245)
  and [NodeStateSolrServerConfigurationProvider#Properties](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-solr-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/solr/configuration/nodestate/NodeStateSolrServerConfigurationProvider.java#L94)

```
/oak:index/solrRemote
  - jcr:primaryType = "oak:QueryIndexDefinition"
  - type = "solr"
  - async = "async"
  + server
    - jcr:primaryType = "nt:unstructured"
    - solrServerType = "remote"
    - httpUrl = "http://localhost:8983/solr/oak"
```

If such configurations exists in the repository the [NodeStateSolrServersObserver](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/configuration/nodestate/NodeStateSolrServersObserver.html) 
should be registered too (e.g. via [NodeStateSolrServersObserverService](http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/index/solr/osgi/NodeStateSolrServersObserverService.html) 
OSGi service).

#### Notes
As of Oak version 1.0.0:

 * Solr index doesn't support search using relative properties, see [OAK-1835](https://issues.apache.org/jira/browse/OAK-1835).
 * Lucene can only be used for full-text queries, Solr can be used for full-text search _and_ for JCR queries involving
path, property and primary type restrictions.

As of Oak version 1.2.0:

 * Solr index doesn't support index time aggregation, but only query time aggregation
 * Lucene and Solr can be both used for full text, property and path restrictions