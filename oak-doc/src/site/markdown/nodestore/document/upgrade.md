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
# DocumentNodeStore upgrade instructions

In general Oak aims to provide a smooth upgrade path that allows a new version
of Oak to simply run on data written by an older version of Oak. In some cases
additional manual steps are needed or recommended to ensure performance and
reduce downtime of a system to a minimum.

## <a name="1.8"></a>Upgrade to 1.8

Oak 1.8 introduced some changes in the DocumentNodeStore that require an
one-time sweep of the DocumentStore per cluster node. This sweep is triggered
automatically on startup when Oak detects an upgrade from an older version.
Depending on the size of the repository, the sweep operation may take some time
and prolong the downtime of the system during an upgrade. Alternatively, the
one-time sweep operation can also be triggered for an inactive cluster node with
the oak-run tool while the remaining cluster nodes are active and in use by the
application. This is the recommended approach because it minimizes downtime.

### Prerequisites

* Create a backup of the system
* The new Oak version and/or application that bundles or uses Oak
* The oak-run tool in the same version as used by the updated application
* A successful test run of below steps on a clone of the production system
before they are applied to production.

### Instructions

The following instructions assume a cluster with two nodes C1 and C2 running on
Oak 1.6 or older.

* Remove documents potentially created by [OAK-4345][0]. The issue only affected
deployments based on MongoDB. Connect to the database with a MongoDB shell and
then execute:

        > db.nodes.remove({_id:{$type:7}})
* Stop cluster node C1. If possible, the cluster node should be shut down
gracefully because the next step can only be executed when C1 is considered
inactive. A recovery of C1 is otherwise necessary if it is forcefully killed.
This happens automatically when there are other active nodes in the cluster, but
is only initiated after the lease of C1 timed out. The DocumentNodeStore MBean
of an active cluster node can be inspected to find out whether some other
cluster node is considered inactive (see InactiveClusterNodes attribute).
* Run the revisions sweep command using the oak-run tool for C1. A sweep can
only run on an inactive cluster node, otherwise the command will refuse to run.
Assuming C1 used clusterId 1, the command line would look like this:

        > java -Xmx2g -jar oak-run-1.8.0.jar revisions mongodb://localhost:27017/oak sweep --clusterId 1
    
    For larger repositories it is recommended to be more generous with the cache
    size, which will speed up the sweep operation: `--cacheSize 1024`
    More detailed progress is available when `--verbose` is added.
    
    Once finished the tool will print a summary:
    
        Updated sweep revision to r15d12cb1836-0-1. Branch commit markers added to 8907 documents. Reverted uncommitted changes on 19 documents. (7.94 min)

* C1 is now ready for an upgrade to Oak 1.8. 
* Stop cluster node C2. This is when downtime of the system starts.
* Unlock the repository for an upgrade to Oak 1.8. This step is only possible
when *all* nodes of a cluster are inactive.
See also [unlock upgrade](../documentmk.html) section. At this point the
previous Oak version cannot use the DocumentStore anymore. A restore from the
backup will be necessary should any of the following steps fail for some reason
and the upgrade needs to be rolled back.
* Start cluster node C1 with the new version of Oak and the application.
* Run the revisions sweep command using the oak-run tool for C2 (assuming it
used clusterId 2):
         
         > java -Xmx2g -jar oak-run-1.8.0.jar revisions mongodb://localhost:27017/oak sweep --clusterId 2
         
* Start cluster node C2 with the new version of Oak and the application.
* Create recommended indexes in MongoDB and remove old ones. For a more
efficient Revision GC, the existing indexes on `_deletedOnce` and `_sdType`
should be replaced. Please note, the partial index on `_deletedOnce` and
`_modified` requires at least MongoDB 3.2.

        > db.nodes.createIndex({_sdType:1, _sdMaxRevTime:1}, {sparse:true})
        {
            "createdCollectionAutomatically" : false,
            "numIndexesBefore" : 5,
            "numIndexesAfter" : 6,
            "ok" : 1
        }
        > db.nodes.dropIndex("_sdType_1")
        { "nIndexesWas" : 6, "ok" : 1 }
        > db.nodes.createIndex({_deletedOnce:1, _modified:1}, {partialFilterExpression:{_deletedOnce:true}})
        {
            "createdCollectionAutomatically" : false,
            "numIndexesBefore" : 5,
            "numIndexesAfter" : 6,
            "ok" : 1
        }
        > db.nodes.dropIndex("_deletedOnce_1")
        { "nIndexesWas" : 6, "ok" : 1 }
        
    See also instructions how to [build indexes on a replica set][1] to minimize
    impact on the system.

[0]: https://issues.apache.org/jira/browse/OAK-4345
[1]: https://docs.mongodb.com/manual/tutorial/build-indexes-on-replica-sets/#index-building-replica-sets