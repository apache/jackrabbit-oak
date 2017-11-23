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

## Index Management

In Oak, indexes are managed using the JCR API.

## Index Management using Oak-Run
`@since 1.6.0` 

Oak-Run support managing indexes using the "branch-less" mode,
which avoids branch commits, which can be relatively slow on MongoDB.
In this case, the repository must be accessed in standalone mode,
meaning no other cluster nodes must run concurrently.
The tool supports running scripts that contain index definition changes,
as well an interative mode, for development.
While developing scripts, it is typically a good idea to verify the [Json is valid](http://jsonlint.com/). 
To start the interactive mode,
with the branch-less commit option, and MongoDB, use:

    java -jar oak-run-*.jar json-index --disableBranches --script - \
      mongodb://localhost:27017/oak

This will open the repository and allow to write and read from the repository.
An example script creates an index "test", and then re-indexes it:

    {"addNode": "/oak:index/test", "node": {
        "jcr:primaryType": "oak:QueryIndexDefinition", 
        "type": "property", "{Name}propertyNames": ["test"]
    }}
    {"session": "save"}
    {"xpath": "/jcr:root/oak:index/test", "depth": 2}
    {"setProperty": "/oak:index/test/reindex", "value": true}
    {"session": "save"}
    {"xpath": "/jcr:root/oak:index/test", "depth": 2}

This index is re-indexed as follows (note that reindexing is typically only needed 
if the index definition changes):

    {"setProperty": "/oak:index/test/reindex", "value": true}
    {"session": "save"}
    {"xpath": "/jcr:root/oak:index/test", "depth": 2}
    
### Command Reference
    
The following commands are available:

* Add a node: `{"addNode": "<path>", "node": { ... }}`. 
  Adding child nodes is supported.
  Property types "String" and "Boolean" are automatically detected,
  for other property types use the property name prefix `{<PropertTypeName>}`.
  Ignored if the node already exists.
* Add a node: `{"removeNode": "<path>"}`. 
  Ignored if the node does not exist.
* Set a property: `{"setProperty": "<path>/<propertyName>", "value": <value>}`. 
  Ignored if the node does not exist. 
  Use the value `null` to remove a property.
* Save the changes: `{"session": "save"}`. 
* Query using XPath: `{"xpath": "<query>"}`.
  Optionally, specify `"depth"` of the returned nodes.
  Use `"quiet": true` to disable output.
  The result is stored in the variable `$result`, and
  the number of rows is stored in the variable `$resultSize`.
* Query using SQL-2: `{"sql": "<query>"}`.
  Otherwise same as for `xpath`.
* Print: `{"session": "<message>"}`.
* Set a variable: `{"$<variableName>": <value>}`.
  All variable are global.
  Variable names can be used instead of values
  and in queries.
  Indirect addressing is available using `$$`.
* Loop: `{"for": "$<variableName>", "do": [ <commands> ]}"`.
* Endless loop: `{"loop": [ <commands> ]}"`.
  Exit by setting the variable `{"$break": true}`.
* Conditional commands: `{<command>, "if": <a>, "=" <b>}`.
  This can be used for any commands.
  
### Examples

#### Reindex Counter Index

The following script reindexes the counter index (in synchronous mode),
and then switches it back to async mode.

    // reindex-counter.txt
    // reindex the counter index
    {"print": "reindex count for counter index:"}
    {"sql": "select [reindexCount] from [nt:base] where [jcr:path] = '/oak:index/counter'"}
    {"setProperty": "/oak:index/counter/async", "value": null}
    {"setProperty": "/oak:index/counter/reindex", "value": true}
    {"print": "reindexing counter index..."}
    {"session": "save"}
    {"print": "switch to async"}
    {"setProperty": "/oak:index/counter/async", "value": "async"}
    {"session": "save"}
    {"print": "reindex count for counter index is now:"}
    {"sql": "select [reindexCount] from [nt:base] where [jcr:path] = '/oak:index/counter'"}
    {"print": "done"}
    exit
    
Such scripts are typically stored as a text file (here `reindex-counter.txt`),
and executed as follows:

    java -jar oak-run-1.6-SNAPSHOT.jar json-index --disableBranches \
        --script reindex-counter.txt  
        mongodb://localhost:27017/oak

#### Create an Index

The following script created the index externalId:

    // create-externalId.txt
    // create a unique index on externalId
    {"print": "check if externalId already exists"}
    {"xpath": "/jcr:root/oak:index/externalId"}
    {"if": "$resultSize", "=": 1, "print": "index externalId already exists"}
    {"$create": []}
    {"if": "$resultSize", "=": 0, "$create": [1]}
    {"for": "$create", "do": [
        {"print": "does not exist; creating..."},
        {"addNode": "/oak:index/externalId", "node": {
            "jcr:primaryType": "oak:QueryIndexDefinition",
            "{Name}propertyNames": ["rep:externalId"],
            "type": "property",
            "unique": true
        }},
        {"session": "save"},
        {"print": "done; index is now:"},
        {"xpath": "/jcr:root/oak:index/externalId", "depth": 2}
    ]}
    exit
    
#### Create Nodex for Testing

The tool can also be used for testing queries, and creating nodes.
The following creates 1000 nodes `/test/n<x>`, saving every 100 nodes:

    // create-nodex.txt
    // create 1000 nodes under /test/n<x>
    {"removeNode": "/test"}
    {"session": "save"}
    {"addNode": "/test", "node":{"jcr:primaryType": "oak:Unstructured"}}
    {"$commit": 100}
    {"$y": 0}
    {"$x": 0}
    {"loop": [
        {"$x": "$x", "+": 1},
        {"if": "$x", "=": 1000, "$break": true},
        {"$y": "$y", "+": 1},
        {"if": "$y", "=": "$commit", "session": "save"},
        {"if": "$y", "=": "$commit", "print": "$x"},
        {"if": "$y", "=": "$commit", "$y": 0},
        {"$p": "/test/n", "+": "$x"},
        {"addNode": "$p", "node":{"jcr:primaryType": "oak:Unstructured"}}
    ]}
    {"session": "save"}
    {"print": "done"}
    exit

