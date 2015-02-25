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
<a name="solr-index-definition"></a>
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
 * SolrCloud cluster, with Zookeeper instance(s) to control a dynamic, resilient set of Solr servers for high 
 availability and fault tolerance

The Oak Solr index can be configured to either use an 'embedded Solr server' or a 'remote Solr server' (being able to 
connect to a single remote instance or to a SolrCloud cluster via Zookeeper).

##### OSGi environment
All the Solr configuration parameters are described in the 'Solr Server Configuration' section on the 
[OSGi configuration](osgi_config.html) page.

Create an index definition for the Solr index, as described [above](#solr-index-definition).
Once the query index definition node has been created, access OSGi ConfigurationAdmin via e.g. Apache Felix WebConsole:

 1. find the 'Oak Solr indexing / search configuration' item and eventually change configuration properties as needed
 2. find either the 'Oak Solr embedded server configuration' or 'Oak Solr remote server configuration' items depending 
 on the chosen Solr architecture and eventually change configuration properties as needed
 3. find the 'Oak Solr server provider' item and select the chosen provider ('remote' or 'embedded') 

##### Solr server configurations
Depending on the use case, different Solr server configurations are recommended.

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
All the configuration and tuning of Solr, other than what's described in 'Solr Server Configuration' section of the [OSGi 
configuration](osgi_config.html) page, will have to be performed on the Solr side; [sample Solr configuration]
 (http://svn.apache.org/viewvc/jackrabbit/oak/trunk/oak-solr-core/src/main/resources/solr/) files (schema.xml, 
 solrconfig.xml, etc.) to start with can be found in _oak-solr-core_ artifact.

###### SolrCloud cluster
A [SolrCloud](https://cwiki.apache.org/confluence/display/solr/SolrCloud) cluster is the recommended setup for an Oak 
Solr index in production as it provides a scalable and fault tolerant architecture.
In order to configure a SolrCloud cluster the host of the Zookeeper instance / ensemble managing the Solr servers has 
to be provided in the _solr.zk.host_ property (e.g. _10.1.1.108:9983_) since the SolrJ client for SolrCloud communicates 
directly with Zookeeper.
The [Solr collection](https://wiki.apache.org/solr/SolrTerminology) to be used within Oak is named _oak_, having a replication
 factor of 2 and using 2 shards; this means in the default setup the SolrCloud cluster would have to be composed by at 
 least 4 Solr servers as the index will be split into 2 shards and each shard will have 2 replicas. Such parameters can 
 be changed, look for the 'Oak Solr remote server configuration' item on the [OSGi configuration](osgi_config.html) page.
SolrCloud also allows the hot deploy of configuration files to be used for a certain collection so while setting up the 
 collection to be used for Oak with the needed files before starting the cluster, configuration files can also be uploaded 
 from a local directory, this is controlled by the _solr.conf.dir_ property of the 'Oak Solr remote server configuration'.
For a detailed description of how SolrCloud works see the [Solr reference guide](https://cwiki.apache.org/confluence/display/solr/SolrCloud).

#### Differences with the Lucene index
As of Oak version 1.0.0:

* Solr index doesn't support search using relative properties, see [OAK-1835](https://issues.apache.org/jira/browse/OAK-1835).
* Solr configuration is mostly done on the Solr side via schema.xml / solrconfig.xml files.
* Lucene can only be used for full-text queries, Solr can be used for full-text search _and_ for JCR queries involving
path, property and primary type restrictions.

