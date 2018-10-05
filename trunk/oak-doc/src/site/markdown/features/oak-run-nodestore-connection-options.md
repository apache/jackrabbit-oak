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
# Oak Run NodeStore Connection

`@since Oak 1.7.6`

This page provide details around various options supported by some of the oak-run commands to connect
to NodeStore repository. By default most of these commands (unless documented) would connect in read only
mode.

These options are supported by following command (See [OAK-6210][OAK-6210])

* console
* index
* tika

Depending on your setup you would need to configure the NodeStore and BlobStore in use for commands to work.
Some commands may not require the BlobStore details. Check the specific oak-run command help to see if
access to BlobStore is required or not. 

## Read Write Mode

By default most commands would connect to NodeStore in read only mode. This ensure that oak-run commands 
can be safely used with productions setup and does not cause any side effect.

For some operations read-write access would be required. This can be done by passing `--read-write` option.
In read-write mode it should be ensured that Oak version from oak-run is matching with Oak version used by
application to create the repository. 

A newer version of oak-run can read repository created by older version of Oak (as Oak is backward compatible)
However if writes are done by newer version of oak-run (which is more recent than Oak version used by repository application)
then it may cause issues due to change in storage format.


## NodeStore

### SegmentNodeStore

To connect to SegmentNodeStore just specify the path to folder used by SegmentNodeStore for storing the
repository content

    java -jar oak-run <command> /path/to/segmentstore
    
If `--read-write` option is enabled then it must be ensured that target repository is not in use. Otherwise
oak-run would not be able access the NodeStore.
    
### DocumentNodeStore - Mongo

To connect to Mongo specify the MongoURI

    java -jar oak-run <command> mongodb://server:port
    
It support some other options like cache size, cache distribution etc. Refer to help output via `-h` to
see supported options

### DocumentNodeStore - RDB

<<TBD>>

## BlobStore

### FileDataStore

Specify the path to directory used by `FileDataStore` via `--fds-path` option

    java -jar oak-run <command> /path/to/segmentstore --fds-path=/path/to/fds
    
### S3DataStore

Specify the path to config file which contains connection details related to S3 bucket to be used via `-s3ds` option

    java -jar oak-run <command> /path/to/segmentstore --s3ds=/path/to/S3DataStore.config
    
The file should be a valid config file as configured S3DataStore in OSGi setup for pid 
`org.apache.jackrabbit.oak.plugins.blob.datastore.S3DataStore.config`. 

Do change the `path` property to location based on system from where command is being used. If you are running
the command on the setup where the Oak application is running then ensure that `path` is set to a different 
location.
    
[OAK-6210]: https://issues.apache.org/jira/browse/OAK-6210