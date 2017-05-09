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
distributed nature of an Oak repository. A rarely encountered side effect of this is that sessions
expose [write skew](architecture/transactional-model.html).

This change can cause subtle differences in behavior when two sessions perform modifications
relying on one session seeing the other session's changes. Oak requires explicit calls to
`Session.refresh()`in this case.

> *Note*: To ease migration to Oak, sessions being idle for more than one minute will log a warning
> to the log file. Furthermore sessions are automatically synchronised to reflect the same state
> across accesses within a single thread. That is, an older session will see the changes done
> through a newer session given both sessions are accessed from within the same thread.
>
> Automatic session synchronisation is a transient feature and will most probably be removed in
> future versions of Oak. See [OAK-803](https://issues.apache.org/jira/browse/OAK-803) for further
> details regarding session backwards compatibility and
> [OAK-960](https://issues.apache.org/jira/browse/OAK-960) regarding in thread session
> synchronisation.
>
> The `SessionMBean` provides further information on when a session is refreshed and wheter
> a refresh will happen on the next access. 

On Oak `Item.refresh()` is deprecated and will always cause an `Session.refresh()`. The former call
will result in a warning written to the log in order to facilitate locating trouble spots.

On Oak `Item.save()` is deprecated and will per default log a warning and fall back to
`Session.save()`. This behaviour can be tweaked with `-Ditem-save-does-session-save=false` in which
case no fall back to `Session#save()` will happen but an `UnsupportedRepositoryException` is thrown
if the sub-tree rooted at the respective item does not contain all transient changes. See
[OAK-993](https://issues.apache.org/jira/browse/OAK-993) for details.

Query
-----

Oak does not index as much content by default as does Jackrabbit 2. You need to create custom indexes when
necessary, much like in traditional RDBMSs. If there is no index for a specific query then the
repository will be traversed. That is, the query will still work but probably be very slow.
See the [query overview page](query/query.html) for how to create a custom index.

There were some smaller bugfixes in the query parser which might lead to incompatibility.
See the [query overview page](query/query.html) for details.

In Oak, the method `QueryManager.createQuery` does not 
return an object of type `QueryObjectModel`.

Observation
-----------
* `Event.getInfo()` contains the primary and mixin node types of the associated parent node of the 
  event. The key `jcr:primaryType` maps to the primary type and the key `jcr:mixinTypes` maps to an 
  array containing the mixin types.

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

* Event generation is done by looking at the difference between two revisions of the persisted
  content trees. Items not present in a previous revision but present in the current revision are
  reported as `Event.NODE_ADDED` and `Event.PROPERTY_ADDED`, respectively. Items present in a
  previous revision but not present in the current revision are reported as `Event.NODE_REMOVED` and
  `Event.PROPERTY_REMOVED`, respectively. Properties that changed in between the previous revision
  and the current revision are reported as `PROPERTY_CHANGED`. As a consequence operations that
  cancelled each others in between the previous revision and the current revision are not reported.
  Furthermore the order of the events depends on the underlying implementation and is not specified.
  In particular there are some interesting consequences:

    * Touched properties: Jackrabbit 2 used to generate a `PROPERTY_CHANGED` event when touching a
      property (i.e. setting a property to its current value). Oak keeps closer to the specification
      and [omits such events](https://issues.apache.org/jira/browse/OAK-948). More generally removing
      a subtree and replacing it with the same subtree will not generate any event.

    * Removing a referenceable node and adding it again will result in a `PROPERTY_CHANGED` event for
      `jcr:uuid`; the same applies for other built-in protected and mandatory properties
      such as e.g. jcr:versionHistory if the corresponding versionable node
      was removed and a versionable node with the same name is being created.

    * Limited support for `Event.NODE_MOVED`:

      + A node that is added and subsequently moved will not generate a `Event.NODE_MOVED`
        but a `Event.NODE_ADDED` for its final location.

      + A node that is moved and subsequently removed will not generate a `Event.NODE_MOVED`
        but a `Event.NODE_REMOVED` for its initial location.

      + A node that is moved and subsequently moved again will only generate a single
        `Event.NODE_MOVED` reporting its initial location as `srcAbsPath` and its
         final location as `destAbsPath`.

      + A node whose parent was moved and that moved itself subsequently reports its initial
        location as `srcAbsPath` instead of the location it had under the moved parent.

      + A node that was moved and subsequently its parent is moved will report its final
        location as `destAbsPath` instead of the location it had before its parent moved.

      + Removing a node and adding a node with the same name at the same parent will be
        reported as `NODE_MOVED` event as if it where caused by `Node.orderBefore()` if
        the parent node is orderable and the sequence of operations caused a change in
        the order of the child nodes.

      + The exact sequence of `Node.orderBefore()` will not be reflected through `NODE_MOVED`
        events: given two child nodes `a` and `b`, ordering `a` after `b` may be reported as
        ordering `b` before `a`.

* The sequence of differences Oak generates observation events from is guaranteed to contain the
  before and after states of all cluster local changes. This guarantee does not hold for cluster
  external changes. That is, cancelling operations from cluster external events might not be
  reported event though they stem from separate commits (`Session.save()`).

* Unregistering an observation listener blocks for no more than one second. If a pending
  `onEvent()` call does not complete by then a warning is logged and the listener will be
  unregistered without further waiting for the pending `onEvent()` call to complete.
  See [OAK-1290](https://issues.apache.org/jira/browse/OAK-1290) and
  [JSR_333-74](https://java.net/jira/browse/JSR_333-74) for further information.

* See [OAK-1459](https://issues.apache.org/jira/browse/OAK-1459) introduced some differences
  in what events are dispatch for bulk operations (moving and deleting sub-trees):

<table>
<tr>
<th>Operation</th>
<th>Jackrabbit 2</th>
<th>Oak</th>
</tr>
<tr>
<td>add sub-tree</td>
<td>NODE_ADDED event for every node in the sub-tree</td>
<td>NODE_ADDED event for every node in the sub-tree</td>
</tr>
<tr>
<td>remove sub-tree</td>
<td>NODE_REMOVED event for every node in the sub-tree</td>
<td>NODE_REMOVED event for the root of the sub-tree only</td>
</tr>
<tr>
<td>move sub-tree</td>
<td>NODE_MOVED event, NODE_ADDED event for the root of the sub-tree only,
    NODE_REMOVED event for every node in the sub-tree</td>
<td>NODE_MOVED event, NODE_ADDED event for the root of the sub-tree only,
    NODE_REMOVED event for the root of the sub-tree only</td>
</tr>
</table>

Binary streams
--------------

In Jackrabbit 2 binary values were often (though not always) stored in
or spooled into a file in the local file system, and methods like
`Value.getStream()` would thus be backed by `FileInputStream` instances.
As a result the `available()` method of the stream would typically return
the full count of remaining bytes, regardless of whether the next `read()`
call would block to wait for disk IO.

In Oak binaries are typically stored in an external database or (in case of
the SegmentNodeStore) using a custom data structure in the local file system. The streams
returned by Oak are therefore custom `InputStream` subclasses that implement
the `available()` method based on whether the next `read()` call will return
immediately or if it needs to block to wait for the underlying IO operations.

This difference may affect some clients that make the incorrect assumption
that the `available()` method will always return the number of remaining
bytes in the stream, or that the return value is zero only at the end of the
stream. Neither assumption is correctly based on the `InputStream` API
contract, so such client code needs to be fixed to avoid problems with Oak.

Locking
-------

Oak does not support the strict locking semantics of Jackrabbit 2.x. Instead
a "fuzzy locking" approach is used with lock information stored as normal
content changes. If a `mix:lockable` node is marked as holding a lock, then
the code treats it as locked, regardless of what other concurrent sessions
that might see different versions of the node see or do. Similarly a lock token
is simply the path of the locked node.

This fuzzy locking should not be used or relied as a tool for synchronizing
the actions of two clients that are expected to access the repository within
a few seconds of each other. Instead this feature is mostly useful as a higher
level tool, for example a human author could use a lock to mark a document as
locked for a few hours or days during which other users will not be able to
modify the document.

Same name siblings
------------------

Same name siblings (SNS) are deprecated in Oak. We figured that the actual benefit supporting same
name siblings as mandated by JCR is dwarfed by the additional implementation complexity. Instead
there are ideas to implement a feature for automatic [disambiguation of node names](https://issues.apache.org/jira/browse/OAK-129).

In the meanwhile we have [basic support](https://issues.apache.org/jira/browse/OAK-203) for same
name siblings but that might not cover all cases.

XML Import
----------

The import behavior for
[`IMPORT_UUID_CREATE_NEW`](http://www.day.com/maven/jsr170/javadocs/jcr-2.0/javax/jcr/ImportUUIDBehavior.html#IMPORT_UUID_CREATE_NEW)
in Oak is implemented slightly different compared to Jackrabbit. Jackrabbit 2.x only creates a new
UUID when it detects an existing conflicting node with the same UUID. Oak always creates a new UUID,
even if there is no conflicting node. The are mainly two reasons why this is done in Oak:

* The implementation in Oak is closer to what the JCR specification says: *Incoming nodes are
assigned newly created identifiers upon addition to the workspace. As a result, identifier
collisions never occur.*
* Oak uses a MVCC model where a session operates on a snapshot of the repository. It is therefore
very difficult to ensure new UUIDs only in case of a conflict. Based on the snapshot view of a
session, an existing node with a conflicting UUID may not be visible until commit.

Identifiers
-----------

In contrast to Jackrabbit 2.x, only referenceable nodes in Oak have a UUID assigned. With Jackrabbit
2.x the UUID is only visible in content when the node is referenceable and exposes the UUID as a
`jcr:uuid` property. But using `Node.getIdentifier()`, it is possible to get the UUID of any node.
With Oak this method will only return a UUID when the node is referenceable, otherwise the
identifier is the UUID of the nearest referenceable ancestor with the relative path to the node.

Manually adding a property with the name `jcr:uuid` to a non referenceable node might have
unexpected effects as Oak maintains an unique index on `jcr:uuid` properties. As the namespace
`jcr` is reserved, doing so is strongly discouraged.

Versioning
----------

* Because of the different identifier implementation in Oak, the value of a `jcr:frozenUuid` property
on a frozen node will not always be a UUID (see also section about Identifiers). The property
reflects the value returned by `Node.getIdentifier()` when a node is copied into the version storage
as a frozen node. This also means a node restored from a frozen node will only have a `jcr:uuid`
when it is actually referenceable.

* Oak does currently not implement activities (`OPTION_ACTIVITIES_SUPPORTED`), configurations and
baselines (`OPTION_BASELINES_SUPPORTED`).

* Oak does currently not implement the various variants of `VersionManager.merge` but throws an
`UnsupportedRepositoryOperationException` if such a method is called.

Security
--------

* [Authentication](security/authentication/differences.html)
* [Access Control Management](security/accesscontrol/differences.html)
* [Permission Evaluation](security/permission/differences.html)
* [Privilege Management](security/privilege/differences.html)
* [Principal Management](security/principal/differences.html)
* [User Management](security/user/differences.html)

Workspaces
----------

An Oak repository only has one default workspace.

Node Name Length Limit
----------------------

With the document storage backend (MongoDB, RDBMS), there is currently 
a limit of 150 UTF-8 bytes on the length of the node names.
See also OAK-2644.