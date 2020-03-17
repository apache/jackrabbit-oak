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
Jackrabbit Oak - Standalone Application Example
===============================================

This example demonstrates how to embed Oak in an standalone application

Getting Started
---------------

To get started build the build the latest sources with Maven 3 and Java 6 
(or higher). 

    $ cd oak-examples/standalone
    $ mvn clean install 
    
Once done you can run the application by executing

    $ java -jar target/oak-standalone-*.jar
    
This would start an Oak based repository which uses filesystem storage. All 
the content would be by default stored under `oak` folder. The server
would listen at port 8080 and support remote access via DavEx (at `/server`) 
and WebDAV (at `/repository/default`). 

Now lets write something in the repository

    $ curl --request MKCOL --data @- --user admin:admin \
           http://localhost:8080/server/default/jcr:root/hello/ <<END
    <sv:node sv:name="hello" xmlns:sv="http://www.jcp.org/jcr/sv/1.0">
      <sv:property sv:name="message" sv:type="String">
        <sv:value>Hello, World!</sv:value>
      </sv:property>
      <sv:property sv:name="date" sv:type="Date">
        <sv:value>2009-11-17T12:00:00.000Z</sv:value>
      </sv:property>
    </sv:node>
    END

This would create a node `hello` at root. 

    $ curl --user admin:admin http://localhost:8080/server/default/jcr:root/hello.json
    
This should return a json rendition of the node. Application also has some 
other web interfaces which are linked at http://localhost:8080/

### Scripting Repository

The application also has a [Script Console][1] at http://localhost:8080/osgi/system/console/sc
which can be used to execute scripts like below

```java
import javax.jcr.Repository
import javax.jcr.Session
import javax.jcr.SimpleCredentials
import javax.jcr.query.QueryResult
import javax.jcr.query.Row

def queryStr = '''select [jcr:path], [jcr:score], *
    from [oak:QueryIndexDefinition]
'''

Repository repo = osgi.getService(Repository.class)
Session s = null
try {
    s = repo.login(new SimpleCredentials("admin", "admin".toCharArray()))
    def qm = s.getWorkspace().getQueryManager()
    def query = qm.createQuery(queryStr,'sql')
    QueryResult result = query.execute()

    result.rows.each {Row r -> println r.path}
} finally {
    s?.logout()
}
```

Above script would dump path for all index definition nodes.

Using Mongo
-----------

By default the application uses SegmentMk for which stores the data on 
filesystem. Instead of that it can be configured to use Mongo

    $ java -jar target/oak-standalone-*.jar --mongo

It would try to connect to a Mongo server at localhost and 27017 port. One can
specify the server detail also

    $ java -jar target/oak-standalone-*.jar --mongo=mongodb://server:27017
    

Application Structure
---------------------

Oak uses a repository home (defaults to `oak`) folder in current 
directory.

    oak/
    ├── bundles
    ├── dav
    │   └── tmp
    ├── repository
    │   ├── datastore
    │   ├── index
    │   └── segmentstore
    │       ├── data00000a.tar
    │       ├── journal.log
    │       └── repo.lock
    ├── repository-config.json
    └── segmentmk-config.json

In above structure

1. `bundles` - Storage used by OSGi bundles
2. `repository` - All content is stored here. Binary content is stored in 
   `datastore` directory and node content is stored in `segmentstore` directory
3. json file - These are config files which are used to configure the 
  repository components like JAAS setup, SegmentStore config etc. These are OSGi
  config. To see what all config options are supported go to 
  http://localhost:8080/osgi/system/console/configMgr
  
The default setup is configured to use a FileDataStore to store binary 
content for both SegmentMk and MongoMk

Setup
-----

Repository is configured and setup in `RepositoryInitializer`. On startup
it would check if repository home exist or not. If not it would copy a set of
default config files depending on options provided (like --mongo ) and then
construct the Oak Repository instance via `OakOSGiRepositoryFactory`

    private Repository createRepository(List<String> repoConfigs, File repoHomeDir) throws RepositoryException {
        Map<String,Object> config = Maps.newHashMap();
        config.put(OakOSGiRepositoryFactory.REPOSITORY_HOME, repoHomeDir.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE, commaSepFilePaths(repoConfigs));
        config.put(OakOSGiRepositoryFactory.REPOSITORY_SHUTDOWN_ON_TIMEOUT, false);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_ENV_SPRING_BOOT, true);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS, 10);

        //Set of properties used to perform property substitution in
        //OSGi configs
        config.put("repo.home", repoHomeDir.getAbsolutePath());
        config.put("oak.mongo.db", mongoDbName);
        config.put("oak.mongo.uri", getMongoURI());

        //Configures BundleActivator to get notified of
        //OSGi startup and shutdown
        configureActivator(config);

        return new OakOSGiRepositoryFactory().getRepository(config);
    }
    
In above setup

1. `REPOSITORY_HOME` - Path to the directory which would be used to store 
   repository content
2. `REPOSITORY_CONFIG_FILE` - Comma separated json config file paths 

Standalone Application is based on [Spring Boot](http://projects.spring.io/spring-boot/)
and thus supports all features provided by it. 

[1]: http://felix.apache.org/documentation/subprojects/apache-felix-script-console-plugin.html