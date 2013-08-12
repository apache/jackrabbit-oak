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

Backward compatibility
======================

Oak implements the JCR API and we expect most applications to work out of the box. However, the Oak
code base is very young and not yet on par with Jackrabbit 2. Some of the more obscure parts of JCR
are not (yet) implemented. If you encounter a problem running your application on Oak, please cross
check against Jackrabbit 2 before reporting an issue against Oak.

Reporting issues
================

If you encounter a problem where functionality is missing or Oak does not behave as expected please
check whether this is a [known change in behaviour](https://issues.apache.org/jira/browse/OAK-14) or
a [known issue](https://issues.apache.org/jira/browse/OAK). If in doubt ask on the [Oak dev list]
(http://oak.markmail.org/). Otherwise create a [new issue](https://issues.apache.org/jira/browse/OAK).

Notable changes
===============

This section gives a brief overview of the most notable changes in Oak with respect to Jackrabbit 2.
These changes are generally caused by overall design decisions carefully considering the benefits
versus the potential backward compatibility issues.

Session state and refresh behaviour
-----------------------------------

In Jackrabbit 2 sessions always reflects the latest state of the repository. With Oak a session
reflects a stable view of the repository from the time the session was acquired ([MVCC model]
(http://en.wikipedia.org/wiki/MVCC)). This is a fundamental design aspect for achieving the
distributed nature of an Oak repository.

This change can cause subtle differences in behavior when two sessions perform modifications
relying on one session seeing the other session's changes. Oak requires explicit calls to
`Session.refresh()`in this case.

----------------------------------------------------------------------------------------------------

*Note*: To ease migration to Oak, sessions will currently automatically refresh after being idle
        for more than one second. Sessions being idle from more than one minute will log a warning
        to the log file.
        This is a transient feature and will most probably be removed in future versions of Oak.
        See [OAK-803](https://issues.apache.org/jira/browse/OAK-803) for further details and for
        how this behaviour can be changed.

----------------------------------------------------------------------------------------------------

On Oak `Item.refresh()` is deprecated and will always cause an `Session.refresh()`. The former call
will result in a warning written to the log in order to facilitate locating trouble spots.

Query
-----

Oak does not index content by default as does Jackrabbit 2. You need to create custom indexes when
necessary, much like in traditional RDBMSs. If there is no index for a specific query then the
repository will be traversed. That is, the query will still work but probably be very slow.

See TODO for how to create a custom index.

Observation
-----------

Regarding observation listeners:

* `Event.getUserId()`, `Event.getUserData()`and `Event.getDate()` will only be available for locally
  generated events (i.e. on the same cluster node). To help identifying potential trouble spots,
  calling any of these methods without a previous call to `JackrabbitEvent#isExternal()` will write
  a warning to the log file.

* Push notification mechanisms like JCR observation weight heavy on distributed systems. Therefore,
  if an application requirement is not actually an "eventing problem" consider using different means
  like query and custom indexes.
  [Apache Sling](http://sling.apache.org) identified and classified common [usage patterns]
  (https://cwiki.apache.org/confluence/display/SLING/Observation+usage+patterns) of observation and
  recommendations on alternative solutions where applicable.

* Touched properties: Jackrabbit 2 used to generate a `PROPERTY_CHANGED` event when touching a
  property (i.e setting a property to its current value). Oak keeps closer to the specification and
  [omits such events](https://issues.apache.org/jira/browse/OAK-948).

Same name siblings
------------------

Same name siblings (SNS) are deprecated in Oak. We figured that the actual benefit supporting same
name siblings as mandated by JCR is dwarfed by the additional implementation complexity. Instead
there are ideas to implement a feature for automatic [disambiguation of node names]
(https://issues.apache.org/jira/browse/OAK-129).

In the meanwhile we have [basic support](https://issues.apache.org/jira/browse/OAK-203) for same
name siblings but that might not cover all cases.

Authentication
--------------

Please refer to [OAK-793](https://issues.apache.org/jira/browse/OAK-793) for a general overview of
changes with respect to Jackrabbit 2.

Access Control Management
-------------------------

Refer to [OAK-792](https://issues.apache.org/jira/browse/OAK-792) for a general overview of changes
with respect to Jackrabbit 2.

The following modification are most likely to have an effect on existing applications:

* `AccessControlManager#hasPrivilege()` and `AccessControlManager#getPrivileges()` will throw a
  `PathNotFoundException` if the node for the specified path is not accessible. The Jackrabbit 2
  implementation is wrong and we fixed that in OAK ([OAK-886]
  (https://issues.apache.org/jira/browse/OAK-886)). If the new behaviour turns out to be a problem
  with existing applications we might consider adding backward compatible behaviour.

Permissions
-----------

Refer to [OAK-942](https://issues.apache.org/jira/browse/OAK-942) for a general overview of changes
with respect to Jackrabbit 2.

* As of Oak `Node#remove()` only requires sufficient permissions to remove the target node. In
  contrast to jackrabbit the validation will not traverse the tree and verify remove permission on
  all child nodes/properties. There exists a configuration flag that aims to produce best effort
  backwards compatibility but this flag is currently not enabled by default. Please let us know if
  you suspect this causes wrong behavior in your application.

* By default user management operations require the specific user mgt related
  permission that has been introduced with OAK-1.0. This behavior can be
  turned off by setting the corresponding configuration flag.

* As of OAK reading and writing items in the version store does not follow the
  regular permission evaluation but depends on access rights present on the
  corresponding versionable node [OAK-444](https://issues.apache.org/jira/browse/OAK-444).

Privilege Management
--------------------

Refer to [OAK-910](https://issues.apache.org/jira/browse/OAK-910) for a general overview of changes
with respect to Jackrabbit 2.

User Management
---------------

Refer to [OAK-791](https://issues.apache.org/jira/browse/OAK-791) for a general overview of changes
with respect to Jackrabbit 2.

Principal Management
--------------------

Refer to [OAK-909](https://issues.apache.org/jira/browse/OAK-909) for a general overview of changes
with respect to Jackrabbit 2.

Known issues
============
All known issues are listed in the Apache [JIRA](https://issues.apache.org/jira/browse/OAK).
Changes with respect to Jackrabbit-core are collected in [OAK-14]
((https://issues.apache.org/jira/browse/OAK-14)) and its sub-tasks.

* Locking:
  * Locking and unlocking of nodes is not implemented yet. You will not see an exception as long as
    the [TODO](https://issues.apache.org/jira/browse/OAK-193)-flag prevents the implementation from
    throwing UnsupportedOperationException, but the node *will not* be locked.
    See [OAK-150](https://issues.apache.org/jira/browse/OAK-150)

* Nodetype Management:
  * Removing mixins is not implemented yet
    See [OAK-767](https://issues.apache.org/jira/browse/OAK-767)

* Versioning:
  * JCR version labels are not implemented yet
  * `VersionHistory#removeVersion()` is not implemented yet
  * `VersionHistory#getAllLinearVersions()` is not implemented yet
  * `VersionManager#merge()` is not implemented yet
  * `VersionManager#restore()` with version-array is not implemented yet
  * Activities are not implemented yet
  * Configurations are not implemented yet
  * See [OAK-168](https://issues.apache.org/jira/browse/OAK-168)

* Query:
  * Known issue with OR statements in full text queries
   See [OAK-902](https://issues.apache.org/jira/browse/OAK-902)

* Workspace Operations:
  * Cross workspace operations are not implemented yet
    See [OAK-916](https://issues.apache.org/jira/browse/OAK-916)
  * `Workspace#importXml()` not implemented yet
    See [OAK-773](https://issues.apache.org/jira/browse/OAK-773)
  * Workspace Management (creating/deleting workspaces) is not implemented yet
    See [OAK-916](https://issues.apache.org/jira/browse/OAK-916)
  * `Workspace#copy()` is not properly implemented
    See [OAK-917](https://issues.apache.org/jira/browse/OAK-917) and sub tasks
    * copy of referenceable nodes does not work
      See [OAK-915](https://issues.apache.org/jira/browse/OAK-915)
    * copy of versionable nodes does not create new version history
      See [OAK-918](https://issues.apache.org/jira/browse/OAK-918)
    * copy of locked nodes does not remove the lock
      See [OAK-919](https://issues.apache.org/jira/browse/OAK-919)
    * copy of trees with limited read access
      See [OAK-920](https://issues.apache.org/jira/browse/OAK-920)

* Access Control Management and Permissions:
  * Move operations are not properly handled wrt. permissions
    See [OAK-710](https://issues.apache.org/jira/browse/OAK-710)

* User Management:
  * Group membership stored in tree structure is not yet implemented
    See [OAK-482](https://issues.apache.org/jira/browse/OAK-482)

In some cases Oak throws Runtime exceptions instead of a properly typed exception. We are working
on correcting this. Please do not work around this by adapting catch clauses in your application.
