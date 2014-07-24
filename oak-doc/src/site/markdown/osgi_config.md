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

# Repository OSGi Configuration

Oak comes with a simple mechanism for constructing content repositories
for use in embedded deployments and test cases. Details regarding that are provided
as part of [Repository Construction](construct.html). When used in OSGi environment
then various Oak components can be configured using OSGi Configuration Support.

Depending on component the configuration can be modified at runtime or needs to be
specified before the initial system setup.

Static Configuration
: Such configuration settings cannot be changed once a repository
  is initialized. For example choosing a `DataStore` or specifying the path of User Home.
  Such properties should not be changed once a system is initialized.

Dynamic Configuration
: Some of the configuration settings like thread pool size, cache size etc can be changed
  at runtime or after initial system setup

Each OSGi configuration is referred via a PID i.e. persistent identifier. Sections below
provide details around various PID used in Oak

### NodeStore

#### SegmentNodeStore

_PID `org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStoreService`_

repository.home
: Path to repository home under which various repository related data is stored. Segment files would be
stored under _${repository.home}/segmentstore_ directory

tarmk.size
: Default - 256 (in MB)
: Maximum file size (in MB)

<a name="document-node-store"></a>
#### DocumentNodeStore

_PID `org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService`_

mongouri
: Default - mongodb://localhost:27017
: Specifies the [MongoURI][1] required to connect to Mongo Database

db
: Default - oak
: Name of the database in Mongo

cache
: Default - 256
: Cache size in MB. This is distributed among various caches used in DocumentNodeStore

changesSize
: Default - 256
: Size in MB of capped collection used in Mongo for caching the diff output.

customBlobStore
: Default false
: Boolean value indicating that custom `BlobStore` to use. By default it uses `MongoBlobStore`.

maxReplicationLagInSecs
: Default 21600 (6 hours)
: Determines the duration beyond which it can be safely assumed that state on secondary would be consistent 
  with primary and its safe to read from them. (See [OAK-1645][OAK-1645])
  
blobGcMaxAgeInSecs
: Default 86400 (24 hrs)
: Blob Garbage Collector (GC) logic would only consider those blobs for GC which are not accessed recently 
  (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). For example as per default only those blobs which have
  been created 24 hrs ago would be considered for GC
  
versionGcMaxAgeInSecs
: Default 86400 (24 hrs)
: Oak uses MVCC model to store the data. So each update to a node results in new version getting created. This 
  duration controls how much old revision data should be kept. For example if a node is deleted at time T1 then its
  content would only be marked deleted at revision for T1 but its content would not be removed. Only when a Revision
  GC is run then its content would removed and that too only after (currentTime -T1 > versionGcMaxAgeInSecs)

Example config file

    mongouri=mongodb://localhost:27017
    db=oak
    
##### Mongo Configuration

All the configuration related to Mongo can be specified via [Mongo URI][1]

* **Authentication** - Username and password should be specified as part of uri e.g. the following 
  connects and logs in to the admin database as user sysop with the password moon:
  
        mongodb://sysop:moon@localhost
    
* **Read Preferences and Write Concern** - These also can be spcified as part of Mongo URI. Refer to 
  [Read Preference and Write Concern](documentmk.html#rw-preference) section for more details. For
  e.g. following would set _readPreference_ to _secondary_ and prefer replica with tag _dc:ny,rack:1_.
  It would also specify the write timeout to 10 sec
  
        mongodb://db1.example.net,db2.example.com?readPreference=secondary&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=&w=1&wtimeoutMS=10000    
  
One can also specify the connection pool size, socket timeout etc. For complete details about various 
possible option refer to [Mongo URI][1]  

<a name="config-blobstore"></a>  
### Configuring DataStore/BlobStore
BlobStores are used to store the binary content. Support for Jackrabbit 2 `DataStore` is also provided via
a `DataStoreBlobStore` wrapper. To use a specific BlobStore implementation following two steps need to be
performed

1. Configure NodeStore - NodeStore config need to be modified to enable use of custom BlobStore via setting
`customBlobStore` to true
2. Configure BlobStore - Create config for the required BlobStore by using the PID for that BlobStore.

Refer to [Config steps in Apache Sling](#config-sling) for an example on how to configure a `FileDataStore`
with `DocumentNodeStore`

#### Jackrabbit 2 - FileDataStore
Jackrabbit 2 [FileDataStore][2] can be configured via following _pid_

_PID `org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore`_

path
: Default - Not specified
: Path to the directory under which the files would be stored. 

minRecordLength
: Default - 100
: Size in bytes. Binary content less than `minRecordLength` would be inlined i.e. the data store id is
the data itself).

maxCachedBinarySize
: Default - 17408 (17 KB)
: Size in bytes. Binaries with size less than or equal to this size would be stored in in memory cache

cacheSizeInMB
: Default - 16
: Size in MB. In memory cache for storing small files whose size is less than `maxCachedBinarySize`. This
  helps in better performance when lots of small binaries are accessed frequently.

#### Jackrabbit 2 - S3DataStore

_PID `org.apache.jackrabbit.oak.plugins.blob.datastore.S3DataStore`_

maxCachedBinarySize
: Default - 17408 (17 KB)
: Size in bytes. Binaries with size less than or equal to this size would be stored in in memory cache

cacheSizeInMB
: Default - 16
: Size in MB. In memory cache for storing small files whose size is less than `maxCachedBinarySize`. This
  helps in better performance when lots of small binaries are accessed frequently.

### System properties and Framework properties

Following properties are supported by Oak. They are grouped in two parts _Stable_ and
_Experimental_. The stable properties would be supported in future version but the experimental properties
would _might_ not be supported in future versions

#### Stable 

oak.mongo.uri
: Type - System property and Framework Property
: Specifies the [MongoURI][1] required to connect to Mongo Database

oak.mongo.db
: Type - System property and Framework Property
: Name of the database in Mongo

#### Experimental

### Configuration Steps for Apache Sling

The OSGi Configuration Admin service defines a mechanism for passing configuration settings
to an OSGi bundle. How a configuration is registered with the OSGi system varies depending
on the application.

<a name="config-sling"></a>
For example to configure `DocumentNodeStore` to use `FileDataStore` in Apache Sling

1. Create a config file with name _org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.cfg_ under
_${sling.home}/install_ folder with content

        #Mongo server details
        mongouri=mongodb://localhost:27017

        #Name of Mongo database to use
        db=aem-author

        #Store binaries in custom BlobStore e.g. FileDataStore
        customBlobStore=true
2. Create a config file with name _org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore.cfg_ under
   _${sling.home}/install_ folder with content

        #The minimum size of an object that should be stored in this data store.
        minRecordLength=4096

        #path to the DataStore
        path=./sling/repository/datastore

        #cache for storing small binaries in-memory
        cacheSizeInMB=128

#### Framework Properties vs OSGi Configuration
OSGi components can read config data from two sources.

1. ConfigurationAdmin - These are configured via placing the *.cfg files under _${sling.home}/install_ folder.
   These can also be modified at runtime via Felix WebConsole typically available at http://localhost:8080/system/console
2. Framework Properties - An OSGi framework can be configured to start with some framework properties. These
   properties cannot be changed at runtime. In Apache Sling these can be specified in _${sling.home}/sling.properties_
   or _${sling.home}/conf/sling.properties_

In Oak some of the config properties are also read from _framework properties_. If a value is specified
in both config file and framework properties then framework property takes precedence.

For example by default Sling sets **repository.home** to _${sling.home}/repository_. So this value
need not be specified in config files

[1]: http://docs.mongodb.org/manual/reference/connection-string/
[2]: http://jackrabbit.apache.org/api/2.4/org/apache/jackrabbit/core/data/FileDataStore.html
[OAK-1645]: https://issues.apache.org/jira/browse/OAK-1645

### Solr Server Configuration
Solr index requires some configuration to be properly used, in OSGi environments such configurations can be performed 
via OSGi Configuration Admin.

The following configuration items can be defined (e.g. through Apache Felix WebConsole).

1. Oak Solr indexing / search configuration: Configuration for _OakSolrConfigurationProvider_ service with the following parameters:

        #field for searching for nodes having a certain exact path
        path.exact.field = path_exact
        
        #field for searching for nodes descendants of a node with a certain path
        path.desc.field = path_des
        
        #field for searching for nodes children of a node with a certain path
        path.child.field = path_child
        
        #field for searching for nodes parents of a node with a certain path
        path.parent.field = path_anc
        
        #field to be used for searching when no field is defined in the search query (e.g. user entered queries like 'foo bar')
        catch.all.field = catch_all
        
        #number of documents per 'page' to be fetched for each query 
        rows = 100000
        
        #Solr commit policy to be used when indexing nodes as documents in Solr 
        commit.policy = SOFT
        
        #wether the Solr index should be used also for filtering nodes by path restrictions 
        path.restrictions = false
        
        #wether the Solr index should be used also for filtering nodes by property restrictions 
        property.restrictions = false
        
        #wether the Solr index should be used also for filtering nodes by primary type 
        primarytypes.restrictions = false

2. Oak Solr remote server configuration: Configuration for _RemoteSolrServerProvider_ service with the following parameters:
       
        #URL to connect to a single remote Solr instance, including the core name (e.g. http://10.10.1.107:8983/solr/oak)
        solr.http.url = 
        
        #Zookeeper host to connect to when using SolrCloud clusters (e.g. 10.10.1.102:9983)
        solr.zk.host =
        
        #name ot the Solr collection to use when connecting to a SolrCloud cluster
        solr.collection = oak 
        
        #number of shards to be used for the collection to be used with SolrCloud
        solr.shards.no = 2
        
        #Solr replication factor, no. of replicas to be created for each shard (for each collection) with SolrCloud 
        solr.replication.factor = 2
        
        #directory eventually containing the configuration files to be uploaded for creating the SolrCloud collection 
        solr.conf.dir =  
        
3. Oak Solr embedded server configuration: Configuration for _EmbeddedSolrServerProvider_ service with the following parameters:

        #path to the Solr home directory to be used for starting the EmbeddedSolrServer (can be absolute or relative)
        solr.home.path = solr 
        
        #name of the Solr core to be created within the EmbeddedSolrServer
        solr.core.name = oak
        
        #path to the cores config file to be used for starting Solr
        solr.config.path = solr.xml 

4. Oak Solr server provider: Configuration for _SolrServerProvider_ service with the following parameters:

        #type of Solr server provider to be used, supported types are none, remote (RemoteSolrServerProvider) and embedded (EmbeddedSolrServerProvider)
        server.type = none

