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

* Workspace Operations:
    * Cross workspace operations are not implemented yet
      See [OAK-118](https://issues.apache.org/jira/browse/OAK-118)
    * Workspace Management (creating/deleting workspaces) is not implemented yet
      See [OAK-118](https://issues.apache.org/jira/browse/OAK-118)

* Observation queues handling
    * With an excessively large commit rate, the observation queues can fill up
      and if not prevented and the queue hits a certain threshold and/or the limit
      then it would result in Local events reported as external event
      [OAK-2683][OAK-2683]​

* Changing an observation listener's filter:
    * Prefiltering of observation queues means that if an observation listener's
      filter is changed, that new filter will only fully be applied to newly
      added changes to the observation queues. Those changes that potentially
      were still in the queue will have gone through both the old prefilter
      and the new (post-)filter. [OAK-5208]
    
[OAK-300]: https://issues.apache.org/jira/browse/OAK-300
[OAK-2683]: https://issues.apache.org/jira/browse/OAK-2683
[OAK-5208]: https://issues.apache.org/jira/browse/OAK-5208