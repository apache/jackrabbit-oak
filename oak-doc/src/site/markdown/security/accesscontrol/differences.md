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
### AccessControl Management : Differences wrt Jackrabbit 2.x

#### 1. Characteristics of the Default Implementation

##### General
In general the authorization related code in OAK clearly separates between access
control management (such as defined by the JCR and Jackrabbit API) and the internal
permission evaluation (see also [Permission Evaluation](differences_permissions.html)).

The default implementation of the access control management corresponds to the
resource-based implementation present with Jackrabbit 2.x. The former principal-base
access control management is no longer available but it's functionality has been
incorporated both in the default ac management implementation and the permission evaluation.

##### JCR API
###### AccessControlManager#hasPrivilege and #getPrivileges
As of OAK those methods throw `PathNotFoundException` if the corresponding node
is not accessible by the editing session. This is in accordance with the behavior
mandated by JSR 283 and a bug in Jackrabbit 2.x.

###### AccessControlManager#getEffectivePolicies
In contrast to Jackrabbit 2.x the editing session is used to retrieve the effective
policies and the policies returned by these methods are guarantueed to only return
information that is otherwise accessible by the session. The corresponding methods
in Jackrabbit 2.x use to throw an  exception in this situation.

###### AccessControlPolicy
OAK introduces a new type of policy that enforces regular read-access for everyone
on the trees that hold this new `ReadPolicy` [0]. The main usage of this new policy
is to ensure backwards compatible behavior of repository level information (node
types, namespace, privileges) that are now kept within the content repository.
In Jackrabbit 2.x this information was stored in the file system without the
ability to apply or enforce regular access control such as present with items in
the repository.

Currently these special read policies are defined as part of the overall security
configuration and cannot be managed/edited using regular access control management
API (see ([OAK-951](https://issues.apache.org/jira/browse/OAK-951))).

###### AccessControlEntry
Validation: as of OAK the implementation of the `AccessControlEntry` interface is
no longer in charge of validating the specified privileges. While some validation
is still performed in the corresponding `AccessControlList` methods, the complete
validation is delegated to the commit phase and executed by a specific `Validator`
implementation.

The default behavior with respect to principal validation is compliant with the
specification and the same as in Jackrabbit 2.x.: Adding an ACE for an principal
unknown to the repository will fail. However in order to be consistent
with the ability have a more relaxed behavior upon XML import that validation
will be relaxed if the import behavior is being changed to allow for unknown
principals (see ([OAK-1350](https://issues.apache.org/jira/browse/OAK-1350))) and
the section Import below.

Restrictions: as of OAK the optional restrictions present with a given
`JackrabbitAccessControlEntry` can be multivalued (see below).

##### Jackrabbit API
###### Principal-based Access Control
The principal-based access control management as present in Jackrabbit-core is no
longer present with OAK. The main benefit of the principal-based approach has been
incorporated with the changes in the default [permission evaluation](differences_permissions.html)).
In addition the default access control manager implementation supports all methods
defined by `JackrabbitAccessControlManager`; i.e. editing access control information
by principal is possible as long as the editing session has sufficient permission
on the target node(s). Similarly, the per principal policies exposed to a given
session will always respect that access rights of that session.

###### Restrictions
The implementation of the additional restrictions associated with an ACE has been modified/extended as follows:

- Separate restriction management API (see below) on the OAK level that allows to ease plugging custom restrictions.
- Changed node type definition for storing restrictions in the default implementation.
    - as of OAK restrictions are collected underneath a separate child node "rep:restrictions"
    - restrictions can be multi-valued (see [JCR-3637](https://issues.apache.org/jira/browse/JCR-3637), [JCR-3641](https://issues.apache.org/jira/browse/JCR-3641))
    - backwards compatible behavior for restrictions stored underneath the ACE node directly
- New restrictions:
    - "rep:ntNames", which allows to limit the affected ACE to nodes of the specified node type(s)
    - "rep:prefixes", which allows to limit the effect to item names that have a specific namespace prefix.

##### Import

The import of access control content via JCR XML import has been extended to
respect the `o.a.j.oak.spi.xml.ImportBehavior` flags instead of just performing
a best effort import.

Currently the `ImportBehavior` is only used to switch between different ways of
handling principals unknown to the repository. For consistency and in order to
match the validation requirements as specified by `AccessControlList#addAccessControlEntry`
the default behavior is ABORT (while in Jackrabbit 2.x the behavior always was BESTEFFORT).

The different `ImportBehavior` flags are implemented as follows:
- `ABORT`: throws an `AccessControlException` if the principal is unknown
- `IGNORE`: ignore the entry defining the unknown principal
- `BESTEFFORT`: import the access control entry with an unknown principal.

In order to get the same best effort behavior as present with Jackrabbit 2.x
the configuration parameters of the `AuthorizationConfiguration` must contain
the following entry:

    importBehavior = "besteffort"

See also ([OAK-1350](https://issues.apache.org/jira/browse/OAK-1350)))

#### 2. Node Types

As mentioned above the node type definitions have been extended to match the new functionality related to restrictions.
The node type definition for access control entries:

    [rep:ACE]
      - rep:principalName (STRING) protected mandatory
      - rep:privileges (NAME) protected mandatory multiple
      - rep:nodePath (PATH) protected /* deprecated in favor of restrictions */
      - rep:glob (STRING) protected   /* deprecated in favor of restrictions */
      - * (UNDEFINED) protected       /* deprecated in favor of restrictions */
      + rep:restrictions (rep:Restrictions) = rep:Restrictions protected

The new node type definition for restrictions:

    /**
     * @since oak 1.0
     */
    [rep:Restrictions]
      - * (UNDEFINED) protected
      - * (UNDEFINED) protected multiple

#### 3. API Extensions and Public Classes

org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol [1]

- `AbstractAccessControlList`
- `ImmutableACL`
- `ACE`

org.apache.jackrabbit.oak.spi.security.authorization.restriction [2]

- `RestrictionProvider`:
- `RestrictionDefinition`
- `RestrictionPattern`
- `Restriction`

#### 4. Configuration

The following access control related configuration options are present with the `AuthorizationConfiguration` as of OAK 1.0 [3]

- `getAccessControlManager`
- `getRestrictionProvider`

Differences to Jackrabbit 2.x:

- The "omit-default-permission" configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
- As of OAK no extra access control content is installed by default which renders that flag superfluous.

#### 5. Important Note

The following modification is most likely to have an effect on existing applications:

- `AccessControlManager#hasPrivilege()` and `AccessControlManager#getPrivileges()` will throw a
  `PathNotFoundException` if the node for the specified path is not accessible. The Jackrabbit 2
  implementation is wrong and we fixed that in OAK ([OAK-886](https://issues.apache.org/jira/browse/OAK-886)).
  If the new behaviour turns out to be a problem with existing applications we might consider
  adding backward compatible behaviour.

#### 6. References

[0] https://issues.apache.org/jira/browse/OAK-951

[1] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/restriction/

[2] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/accesscontrol/

[3] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.java
