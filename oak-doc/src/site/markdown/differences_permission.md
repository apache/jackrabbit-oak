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
### Permission Evaluation : Differences wrt Jackrabbit 2.x

*NOTE: Work in Progress*

Refer to [OAK-942](https://issues.apache.org/jira/browse/OAK-942) for a general overview of changes
with respect to Jackrabbit 2.

* As of Oak `Node#remove()` only requires sufficient permissions to remove the target node. In
  contrast to jackrabbit the validation will not traverse the tree and verify remove permission on
  all child nodes/properties. There exists a configuration flag that aims to produce best effort
  backwards compatibility but this flag is currently not enabled by default. Please let us know if
  you suspect this causes wrong behavior in your application.

* 'JackrabbitNode#rename': Due to the nature of the diff mechanism in oak it is
  not possible to distinguish between rename and a move-call with subsequent
  reordering. Consequently the permission evaluation will no longer apply the
  special handling for the renaming as it was present in Jackrabbit 2.x (renaming
  just required the ability to modify the child collection of the parent node).

* By default user management operations require the specific user mgt related
  permission that has been introduced with OAK-1.0. This behavior can be
  turned off by setting the corresponding configuration flag.

* As of OAK reading and writing items in the version store does not follow the
  regular permission evaluation but depends on access rights present on the
  corresponding versionable node [OAK-444](https://issues.apache.org/jira/browse/OAK-444).

