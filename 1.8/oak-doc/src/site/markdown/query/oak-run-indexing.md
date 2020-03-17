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
# <a name="oak-run-indexing"></a> Oak Run Indexing

* [Oak Run Indexing](#oak-run-indexing)
    * [Common Options](#common-options)
    * [Generate Index Info](#index-info)
    * [Dump Index Definitions](#dump-index-defn)
    * [Dump Index Data](#async-index-data)
    * [Index Consistency Check](#check-index)
    * [Reindex](#reindex)
        * [A - out-of-band indexing](#out-of-band-indexing)
            * [Step 1 - Text PreExtraction](#out-of-band-pre-extraction)
            * [Step 2 - Create Checkpoint](#out-of-band-create-checkpoint)
            * [Step 3 - Perform Reindex](#out-of-band-perform-reindex)
            * [Step 4 - Import the index](#out-of-band-import-reindex)
                * [4.1 - Via oak-run](#import-index-oak-run)
                * [4.2 - Via IndexerMBean](#import-index-mbean)
                * [4.3 - Via script](#import-index-script)
        * [B - Online indexing](#online-indexing)
            * [Step 1 - Text PreExtraction](#online-indexing-pre-extract)
            * [Step 2 - Perform reindexing](#online-indexing-perform-reindex)
        * [Updating or Adding New Index Definitions](#index-definition-updates)
        * [JSON File Format](#json-file-format)
        * [Tika Setup](#tika-setup)

`@since Oak 1.7.0`

**Work in progress. Not to be used on production setups**

With Oak 1.7 we have added some tooling as part of oak-run `index` command. Below are details around various
operations supported by this command.

The `index` command supports connecting to different NodeStores via various options which are documented 
[here](../features/oak-run-nodestore-connection-options.html). Example below assume a setup consisting of 
SegmentNodeStore and FileDataStore. Depending on setup use the appropriate connection options.

By default the tool would generate output file in directory `indexing-result` which is referred to as output directory.
 
Unless specified all operations connect to the repository in read only mode

## <a name="common-options"></a> Common Options

All the commands support following common options

1. `--index-paths` - Comma separated list of index paths for which the selected operations need to be performed. If
   not specified then the operation would be performed against all the indexes.
   
Also refer to help output via `-h` command for some other options

## <a name="index-info"></a> Generate Index Info

    java -jar oak-run*.jar index --fds-path=/path/to/datastore  /path/to/segmentstore/ --index-info 

Generates a report consisting of various stats related to indexes present in the given repository. The generated
report is stored by default in `<output dir>/index-info.txt`

Supported for all index types

## <a name="dump-index-defn"></a> Dump Index Definitions

    java -jar oak-run*.jar index --fds-path=/path/to/datastore  /path/to/segmentstore/ --index-definitions
     
`--index-definitions` operation dumps the index definition in json format to a file `<output dir>/index-definitions.json`. The json
file contains index definitions keyed against the index paths

Supported for all index types

## <a name="async-index-data"></a> Dump Index Data

    java -jar oak-run*.jar index --fds-path=/path/to/datastore  /path/to/segmentstore/ --index-dump
     
`--index-dump` operation dumps the index content in output directory. The output directory would contain one folder for 
each index. Each folder would have a property file `index-details.txt` which contains `indexPath`

Supported for only Lucene indexes.

## <a name="check-index"></a> Index Consistency Check

    java -jar oak-run*.jar index --fds-path=/path/to/datastore  /path/to/segmentstore/ --index-consistency-check
    
`--index-consistency-check` operation performs index consistency check against various indexes. It supports 2 level

* Level 1 - Specified as `--index-consistency-check=1`. Performs a basic check to determine if all blobs referred in index
  are valid
* Level 2 - Specified as `--index-consistency-check=2`. Performs a more through check to determine if all index files
  are valid and no corruption has happened. This check is slower
  
It would generate a report in `<output dir>/index-consistency-check-report.txt`

Supported for only Lucene indexes.

## <a name="reindex"></a> Reindex

The reindex operation supports 2 modes of index

* Out-of-band indexing - Here oak-run would connect to repository in read only mode. It would require certain manual steps
* Online Indexing - Here oak-run would connect to repository in `--read-write` mode

Supported for only Lucene indexes.

If the indexes being reindex have fulltext indexing enabled then refer to [Tika Setup](#tika-setup) for steps
on how to adapt the command to include Tika support for text extraction

### <a name="out-of-band-indexing"></a> A - out-of-band indexing

Out of band indexing has following phases

1. Get checkpoint issued 
2. Perform indexing with read only connection to NodeStore upto checkpoint state
3. Import the generated indexes 
4. Complete the increment indexing from checkpoint state to current head


#### <a name="out-of-band-pre-extraction"></a> Step 1 - Text PreExtraction

If the index being reindexed involves fulltext index and the repository has binary content then its recommended
that first  [text pre-extraction](pre-extract-text.html) is performed. This ensures that costly operation around text
extraction is done prior to actual indexing so that actual indexing does not do text extraction in critical path

#### <a name="out-of-band-create-checkpoint"></a>Step 2 - Create Checkpoint

Go to `CheckpointMBean` and create a checkpoint with a long enough lifetime like 10 days. For this invoke
 `CheckpointMBean#createCheckpoint` with 864000000 as argument for lifetime

#### <a name="out-of-band-perform-reindex"></a> Step 3 - Perform Reindex

In this step we perform the actual indexing via oak-run where it connects to repository in read only mode. 
    
     java -jar oak-run*.jar index --reindex \
     --index-paths=/oak:index/indexName \
     --checkpoint=0fd2a388-de87-47d3-8f30-e86b1cf0a081 \	
     --fds-path=/path/to/datastore  /path/to/segmentstore/ 
     
Here following options can be used

* `--pre-extracted-text-dir` - Directory path containing pre extracted text generated via step #1 (optional)
* `--index-paths` - This command requires an explicit set of index paths which need to be indexed (required)
* `--checkpoint` - The checkpoint up to which the index is updated, when indexing in read only mode. For
  testing purpose, it can be set to 'head' to indicate that the head state should be used. (required)
* `--index-definitions-file` - json file file path which contains updated index definitions
  
If the index does not support fulltext indexing then you can omit providing BlobStore details
  
#### <a name="out-of-band-import-reindex"></a>Step 4 - Import the index

As a last step we need to import the index back in the repository. This can be done in one of the 
following ways

##### <a name="import-index-oak-run"></a>4.1 - Via oak-run

In this mode we import the index using oak-run

    java -jar oak-run*.jar index --index-import --read-write \
        --index-import-dir=<index dir>  \
        --fds-path=/path/to/datastore /path/to/segmentstore
    
Here "index dir" is the directory which contains the index files created in step #3. Check the logs from previous
command for the directory path.

This mode should only be used when repository is from Oak version 1.7+ as oak-run connects to the repository in 
read-write mode.

##### <a name="import-index-mbean"></a>4.2 - Via IndexerMBean

In this mode we import the index using JMX. Looks for `IndexerMBean` and then import the index directory using the 
`importIndex` operation

##### <a name="import-index-script"></a>4.3 - Via script

TODO - Provide a way to import the data on older setup using some script


### <a name="online-indexing"></a>B - Online indexing

Online indexing automates some of the manual steps which are required for out-of-band indexing. 

This mode should only be used when repository is from Oak version 1.7+ as oak-run connects to the repository in 
read-write mode.
     
#### <a name="online-indexing-pre-extract"></a>Step 1 - Text PreExtraction

This is same as in out-of-band indexing

#### <a name="online-indexing-perform-reindex"></a>Step 2 - Perform reindexing

In this step we configure oak-run to connect to repository in read-write mode and let it perform all other steps i.e
checkpoint creation, indexing and import

    java -jar oak-run*.jar index --reindex --index-paths=/oak:index/lucene --read-write --fds-path=/path/to/datastore /path/to/segmentstore

### <a name="index-definition-updates"></a> Updating or Adding New Index Definitions

`@since Oak 1.7.5`

Index tooling support updating and adding new index definitions to existing setups. This can be done by passing 
in path of a json file which contains index definitions

    java -jar oak-run*.jar index --reindex --index-paths=/oak:index/newAssetIndex \
    --index-definitions-file=index-definitions.json \
    --fds-path=/path/to/datastore /path/to/segmentstore  
   
Where index-definitions.json has following structure

    {
      "/oak:index/newAssetIndex": {
        "evaluatePathRestrictions": true,
        "compatVersion": 2,
        "type": "lucene",
        "async": "async",
        "jcr:primaryType": "oak:QueryIndexDefinition",
        "indexRules": {
          "jcr:primaryType": "nt:unstructured",
          "dam:Asset": {
            "jcr:primaryType": "nt:unstructured",
            "properties": {
              "jcr:primaryType": "nt:unstructured",
              "valid": {
                "name": "valid",
                "propertyIndex": true,
                "jcr:primaryType": "nt:unstructured",
                "notNullCheckEnabled": true
              },
              "mimetype": {
                "name": "mimetype",
                "analyzed": true,
                "jcr:primaryType": "nt:unstructured"
              }
            }
          }
        }
      }
    }
    
Some points to note about this json file
* Each key of top level object refers to the index path
* The value of each such key refers to complete index definition 
* If the index path is not present in existing repository then it would result in a new index being created
* In case of new index it must be ensured that parent path structure must already exist in repository. 
  So if a new index is being created at `/content/en/oak:index/contentIndex` then path upto  `/content/en/oak:index`
  should already exist in repository
* If this option is used with online indexing then do ensure that oak-run version matches with the Oak version 
  used by target repository

You can also use the json file generated from [Oakutils](http://oakutils.appspot.com/generate/index). It needs to be 
modified to confirm to above structure i.e. enclose the whole definition under the intended index path key.

In general the index definitions does not need any special encoding of values as Index definitions in Oak use
only String, Long and Double types mostly. However if the index refers to binary config like Tika config then
the binary data would need to encoded. Refer to next section for more details.
    
This option is supported in both online and out-of-band indexing. 

For more details refer to [OAK-6471][OAK-6471]
    
### <a name="json-file-format"></a> JSON File Format

Some of the standard types used in Oak are not supported directly by JSON like names, blobs etc. Those would need to be 
encoded in a specific format.

Below are the encoding rules

LONG
: No encoding required
: _"compatVersion": 2_

BOOLEAN
: No encoding required
: _"propertyIndex": true,_

DOUBLE
: No encoding required
: _"weight": 1.5_ 

STRING
: Prefix the value with `str:`
: Generally the value need not be encoded. Encoding is only required if the string starts with 3 letters and then colon
: _"pathPropertyName": "str:jcr:path"_  

DATE
: Prefix the value with `dat:`. The value is ISO8601 formatted date string
: _"created": "dat:2017-07-20T13:23:21.196+05:30"_  

NAME
: Prefix the value with `nam:`.
: For `jcr:primaryType` and `jcr:mixins` no encoding is required. Any property with these names would be converted to
  NAME type
: _"nodetype": "nam:nt:base"_ 

PATH
: Prefix the value with `pat:`
: _"imagePath": "pat:/content/assets/book.jpg"_  

URI
: Prefix the value with `uri:`
: _"serverURI": "uri:http\://foo.example.com"_  

BINARY
: By default the binary values are encoded as Base64 string if the binary is less than 1 MB size. The encoded value is 
  prefixed with `:blobId:`
: _"jcr:data": ":blobId:axygz"_  


### <a name="tika-setup"></a> Tika Setup

If the indexes being reindex have fulltext indexing enabled then you need to include Tika library in classpath.
This is required even if pre extraction is used so as to ensure that any new binary added after pre-extraction
is done can be indexed.

First download the [tika-app](https://tika.apache.org/download.html) jar from Tika downloads. You should be able 
to use 1.15 version with Oak 1.7.4 jar.

Then modify the index command like below. The rest of arguments remain same as documented before.

    java -cp oak-run.jar:tika-app-1.15.jar org.apache.jackrabbit.oak.run.Main index
    

[OAK-6471]: https://issues.apache.org/jira/browse/OAK-6471