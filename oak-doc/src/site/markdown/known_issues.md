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

Known issues
============
All known issues are listed in the Apache [JIRA](https://issues.apache.org/jira/browse/OAK).
Changes with respect to Jackrabbit-core are collected in [OAK-14]
(https://issues.apache.org/jira/browse/OAK-14) and its sub-tasks.

* Locking:
    * Locking and unlocking of nodes is not implemented yet. You will not see an exception as long as
      the [TODO](https://issues.apache.org/jira/browse/OAK-193)-flag prevents the implementation from
      throwing UnsupportedOperationException, but the node *will not* be locked.
      See [OAK-150](https://issues.apache.org/jira/browse/OAK-150)


* Versioning [OAK-168](https://issues.apache.org/jira/browse/OAK-168):
    * `VersionHistory#removeVersion()` is not implemented yet
    * `VersionManager#merge()` is not implemented yet
    * `VersionManager#restore()` with version-array is not implemented yet
    * Activities are not implemented
    * Configurations are not implemented


* Query:
    * Known issue with OR statements in full text queries
      See [OAK-902](https://issues.apache.org/jira/browse/OAK-902)


* Workspace Operations:
    * Cross workspace operations are not implemented yet
      See [OAK-916](https://issues.apache.org/jira/browse/OAK-916)
    * Workspace Management (creating/deleting workspaces) is not implemented yet
      See [OAK-916](https://issues.apache.org/jira/browse/OAK-916)
    * `Workspace#copy()` is not properly implemented
      See [OAK-917](https://issues.apache.org/jira/browse/OAK-917) and sub tasks
        * copy of versionable nodes does not create new version history
          See [OAK-918](https://issues.apache.org/jira/browse/OAK-918)
        * copy of locked nodes does not remove the lock
          See [OAK-919](https://issues.apache.org/jira/browse/OAK-919)
        * copy of trees with limited read access
          See [OAK-920](https://issues.apache.org/jira/browse/OAK-920)


In some cases Oak throws Runtime exceptions instead of a properly typed exception. We are working
on correcting this. Please do not work around this by adapting catch clauses in your application.
