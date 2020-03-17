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
### Access Control Management : Differences wrt Jackrabbit 2.x

#### Differences by Interface

##### AccessControlManager
###### hasPrivilege and getPrivileges
As of OAK those methods throw `PathNotFoundException` if the corresponding node
is not accessible by the editing session. This is in accordance with the behavior
mandated by JSR 283 and a bug in Jackrabbit 2.x.

###### getEffectivePolicies
In contrast to Jackrabbit 2.x the editing session is used to retrieve the effective
policies and the policies returned by these methods are guarantueed to only return
information that is otherwise accessible by the session. The corresponding methods
in Jackrabbit 2.x use to throw an  exception in this situation.

##### AccessControlPolicy
OAK introduces a new type of policy that enforces regular read-access for everyone
on the trees that hold this new `ReadPolicy` (see [OAK-951]). The main usage of this new policy
is to ensure backwards compatible behavior of repository level information (node
types, namespace, privileges) that are now kept within the content repository.
In Jackrabbit 2.x this information was stored in the file system without the
ability to apply or enforce regular access control such as present with items in
the repository.

Currently these special read policies are defined as part of the overall security
configuration and cannot be managed/edited using regular access control management
API (see ([OAK-951](https://issues.apache.org/jira/browse/OAK-951))).

##### AccessControlEntry
Validation: as of OAK the implementation of the `AccessControlEntry` interface is
no longer in charge of validating the specified privileges. While some validation
is still performed in the corresponding `AccessControlList` methods, the complete
validation is delegated to the commit phase and executed by a specific `Validator`
implementation.

The default behavior with respect to principal validation is compliant with the
specification and the same as in Jackrabbit 2.x.: Adding an ACE for an principal
unknown to the repository will fail. However, in order to be consistent
with the behavior upon XML import that validation is relaxed if the import 
behavior is changed to allow for unknown principals (see ([OAK-1350](https://issues.apache.org/jira/browse/OAK-1350))) 
and section XML Import below.

Restrictions: as of OAK the optional restrictions present with a given
`JackrabbitAccessControlEntry` can be multivalued (see below).

#### Additional Functionality

##### Principal-based Access Control
The principal-based access control management as present in Jackrabbit-core is no
longer present with OAK. The main benefit of the principal-based approach has been
incorporated with the changes in the default [permission evaluation](../permission/differences.html)).
In addition the default access control manager implementation supports all methods
defined by `JackrabbitAccessControlManager`; i.e. editing access control information
by principal is possible as long as the editing session has sufficient permission
on the target node(s). Similarly, the per principal policies exposed to a given
session will always respect that access rights of that session.

##### Restrictions
The implementation of additional restrictions associated with an ACE has been 
slighly modified/extended.

See section [Restriction Management](../authorization/restriction.html) for details. 

##### XML Import

* respects `ImportBehavior` for handling of principals instead of just performing best effort import
* supports both `Workspace` and `Session` import

#### Configuration

- The "omit-default-permission" configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
- As of OAK no extra access control content is installed by default which renders that flag superfluous.

#### Important Note

The following modification is most likely to have an effect on existing applications:

- `AccessControlManager#hasPrivilege()` and `AccessControlManager#getPrivileges()` will throw a
  `PathNotFoundException` if the node for the specified path is not accessible. The Jackrabbit 2
  implementation is wrong and we fixed that in OAK ([OAK-886](https://issues.apache.org/jira/browse/OAK-886)).
  If the new behaviour turns out to be a problem with existing applications we might consider
  adding backward compatible behaviour.

<!-- hidden references -->
[OAK-951]: https://issues.apache.org/jira/browse/OAK-951
[ImportBehavior]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/xml/ImportBehavior.html
