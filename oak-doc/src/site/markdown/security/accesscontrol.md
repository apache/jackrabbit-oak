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

Access Control
--------------------------------------------------------------------------------

### JCR API

_todo_

- `AccessControlManager`
- `AccessControlPolicy`
    - `AccessControlList`
    - `NamedAccessControlPolicy`
- `AccessControlEntry`
- `AccessControlPolicyIterator`

### Jackrabbit API

_todo_

- `JackrabbitAccessControlManager`
- `JackrabbitAccessControlList`
- `JackrabbitAccessControlEntry`

### Oak API

_todo_


### Characteristics of the Default Implementation

#### General

In general the authorization related code in Oak clearly separates between access
control management (such as defined by the JCR and Jackrabbit API) and the internal
permission evaluation (see also [Permission Evaluation](differences_permissions.html)).

The default implementation of the access control management corresponds to the
resource-based implementation present with Jackrabbit 2.x. The former principal-base
access control management is no longer available but it's functionality has been
incorporated both in the default ac management implementation and the permission evaluation.

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](accesscontrol/differences.html).

#### Resource Based Access Control

_todo_

#### Principal Base Access Control

_todo_

#### Access Control Policies

_todo_

#### Access Control Entries

_todo_

#### Restrictions

see section [Restriction Management](accesscontrol/restriction.html) for details.


### Representation in the Repository

The node type definition used to represent access control content:

    [rep:AccessControllable]
      mixin
      + rep:policy (rep:Policy) protected IGNORE

    [rep:RepoAccessControllable]
      mixin
      + rep:repoPolicy (rep:Policy) protected IGNORE

    [rep:Policy]
      abstract

    [rep:ACL] > rep:Policy
      orderable
      + * (rep:ACE) = rep:GrantACE protected IGNORE

    [rep:ACE]
      - rep:principalName (STRING) protected mandatory
      - rep:privileges (NAME) protected mandatory multiple
      - rep:nodePath (PATH) protected /* deprecated in favor of restrictions */
      - rep:glob (STRING) protected   /* deprecated in favor of restrictions */
      - * (UNDEFINED) protected       /* deprecated in favor of restrictions */
      + rep:restrictions (rep:Restrictions) = rep:Restrictions protected /* since oak 1.0 */

    [rep:GrantACE] > rep:ACE

    [rep:DenyACE] > rep:ACE

    /**
     * @since oak 1.0
     */
    [rep:Restrictions]
      - * (UNDEFINED) protected
      - * (UNDEFINED) protected multiple


### XML Import

As of OAK 1.0 access control content can be imported both with Session and
Workspace import.

In addition the JCR XML import behavior has been extended to respect the
`o.a.j.oak.spi.xml.ImportBehavior` flags instead of just performing a best effort import.

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


### API Extensions

_todo_

org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol [1]

- `AbstractAccessControlList`
- `ImmutableACL`
- `ACE`

#### Restriction Management

- `RestrictionProvider`:
- `RestrictionDefinition`
- `RestrictionPattern`
- `Restriction`

See [Restriction Management](accesscontrol/restriction.html) for details.

### Configuration

The following access control related configuration options are present with the [AuthorizationConfiguration] as of Oak 1.0:

- `getAccessControlManager`
- `getRestrictionProvider`

#### Configuration Parameters

| Parameter                    | Type                | Default                  |
|------------------------------|---------------------|--------------------------|
| `PARAM_RESTRICTION_PROVIDER` | RestrictionProvider | RestrictionProviderImpl  |
| `PARAM_READ_PATHS`           | Set\<String\>       | paths to namespace, nodetype and privilege root nodes  |
| `PARAM_IMPORT_BEHAVIOR`      | String ("abort", "ignore", "besteffort") | "abort" |
| | | |

Differences to Jackrabbit 2.x:

- The "omit-default-permission" configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
- As of OAK no extra access control content is installed by default which renders that flag superfluous.


### Utilities

The jcr-commons module present with Jackrabbit provide some access control related
utilities that simplify the creation of new policies and entries such as for example:

- `AccessControlUtils.getAccessControlList(Session, String)`
- `AccessControlUtils.getAccessControlList(AccessControlManager, String)`
- `AccessControlUtils.addAccessControlEntry(Session, String, Principal, String[], boolean)`

See
[org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils] for
the complete list of methods.

#### Examples

    String path = node.getPath();
    JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, path);
    acl.addEntry(principal, privileges, true);
    acMgr.setPolicy(path, acl);
    session.save();

### Further Reading

- [Differences wrt Jackrabbit 2.x](accesscontrol/differences.html)
- [Restriction Management](accesscontrol/restriction.html)

<!-- hidden references -->
[1]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/restriction/
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-commons/src/main/java/org/apache/jackrabbit/commons/jackrabbit/authorization/AccessControlUtils.java