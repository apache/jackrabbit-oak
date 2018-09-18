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

Permissions
--------------------------------------------------------------------------------

<a name="jcr_api"/>
### JCR and Jackrabbit API

While access control management is a optional feature, a JCR implementation is
required to support the basic permission checking. The basic requirements for
the permission evalution are defines as follows

> Permissions encompass the restrictions imposed by any access control restrictions
> that may be in effect upon the content of a repository, either implementation
> specific or JCR-defined (Access Control Management)., which consists of

The methods defined to check permissions:

- `Session#hasPermission(String absPath, String actions)`
- `Session#checkPermission(String absPath, String actions)`
- `JackrabbitSession.hasPermission(String absPath, @Nonnull String... actions)` (since Jackrabbit API 2.11.0 and Oak 1.4)

The actions are expected to be a comma separated list of any of the following string constants:

- `Session.ACTION_READ`
- `Session.ACTION_ADD_NODE`
- `Session.ACTION_REMOVE`
- `Session.ACTION_SET_PROPERTY`

And defined by Jackrabbit API the following additional actions (since Jackrabbit API 2.11.0):

- `JackrabbitSession.ACTION_ADD_PROPERTY`
- `JackrabbitSession.ACTION_MODIFY_PROPERTY`
- `JackrabbitSession.ACTION_REMOVE_PROPERTY`
- `JackrabbitSession.ACTION_REMOVE_NODE`
- `JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT`
- `JackrabbitSession.ACTION_VERSIONING`
- `JackrabbitSession.ACTION_LOCKING`
- `JackrabbitSession.ACTION_READ_ACCESS_CONTROL`
- `JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL`
- `JackrabbitSession.ACTION_USER_MANAGEMENT`

**Note**: As of Oak 1.0 the these methods also handle the names of the permissions
defined by Oak (see `Permissions#getString(long permissions)`).

See also section [Permissions vs Privileges](permission/permissionsandprivileges.html) for 
a comparison of these permission checks and testing privileges on the `AccessControlManager`. 

##### Examples
###### Test if session has permission to add a new node (JCR API)

Important: `absPath` refers to the node to be created

    Node content = session.getNode("/content");
    if (session.hasPermission("/content/newNode", Session.ACTION_ADD_NODE)) {
         content.addNode("newNode");
         session.save();
    }

###### Test if session has permission to perform version and lock operations (Jackrabbit API)

    Node content = jrSession.getNode("/content");
    if (jrSession.hasPermission("/content", JackrabbitSession.ACTION_VERSIONING, JackrabbitSession.ACTION_LOCKING))) {
         content.checkin();
         session.save();
    }

###### Test if session has permission to perform version operations (Oak SPI)

    Node content = session.getNode("/content");
    if (session.hasPermission("/content", Permissions.getString(Permissions.VERSION_MANAGEMENT))) {
         content.checkin();
         session.save();
    }

<a name="oak_permissions"/>
### Oak Permissions

#### General Notes

As of Oak 1.0 Permission evaluation is intended to be completely separated from
the access control management as defined by JCR and Jackrabbit API. While
the evaluation and enforcing permissions is considered to be an internal feature
of the Oak core module, the package `org.apache.jackrabbit.oak.spi.security.authorization.permission`
provides some extensions points that allow to plug custom extensions or
implementations the evaluation (see [API Extensions](#api_extensions) below).

#### Built-in Permissions

Oak 1.0 defines the following [Permissions]:

##### Simple Permissions

Read operations:

- `READ_NODE`
- `READ_PROPERTY`
- `READ_ACCESS_CONTROL`

Write operations:

- `ADD_NODE`
- `REMOVE_NODE`
- `MODIFY_CHILD_NODE_COLLECTION`
- `ADD_PROPERTY`
- `MODIFY_PROPERTY`
- `REMOVE_PROPERTY`
- `NODE_TYPE_MANAGEMENT`
- `MODIFY_ACCESS_CONTROL`
- `LOCK_MANAGEMENT`
- `VERSION_MANAGEMENT`

Since Oak 1.0:

- `USER_MANAGEMENT`: : execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.
- `INDEX_DEFINITION_MANAGEMENT`: create, modify and remove the oak:index node and it's subtree which is expected to contain the index definitions.

Repository operations:

- `NODE_TYPE_DEFINITION_MANAGEMENT`
- `NAMESPACE_MANAGEMENT`
- `PRIVILEGE_MANAGEMENT`
- `WORKSPACE_MANAGEMENT`

Not used in Oak 1.0:

- `LIFECYCLE_MANAGEMENT`
- `RETENTION_MANAGEMENT`

##### Aggregated Permissions

- `READ`: aggregates `READ_NODE` and `READ_PROPERTY`
- `REMOVE`: aggregates `REMOVE_NODE` and `REMOVE_PROPERTY`
- `SET_PROPERTY`: aggregates `ADD_PROPERTY`, `MODIFY_PROPERTY` and `REMOVE_PROPERTY`
- `WRITE`: aggregates `ADD_NODE`, `REMOVE_NODE` and `SET_PROPERTY`
- `ALL`: aggregates all permissions

#### Mapping of JCR Actions to Oak Permissions

`ACTION_READ`:

- access control content: `Permissions.READ_ACCESS_CONTROL`
- regular nodes: `Permissions.READ_NODE`
- regular properties: `Permissions.READ_PROPERTY`
- non-existing items: `Permissions.READ`

`ACTION_ADD_NODE`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- regular nodes: `Permissions.ADD_NODE`

`ACTION_REMOVE`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- regular nodes: `Permissions.REMOVE_NODE`
- regular properties: `Permissions.REMOVE_PROPERTY`
- non-existing nodes: `Permissions.REMOVE`

`ACTION_SET_PROPERTY`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- regular properties: `Permissions.MODIFY_PROPERTY`
- non-existing properties: `Permissions.ADD_PROPERTY`

`ACTION_ADD_PROPERTY`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- other properties: `Permissions.ADD_PROPERTY`

`ACTION_MODIFY_PROPERTY`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- other properties: `Permissions.MODIFY_PROPERTY`

`ACTION_REMOVE_PROPERTY`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- other properties: `Permissions.REMOVE_PROPERTY`

`ACTION_REMOVE_NODE`:

- access control content: `Permissions.MODIFY_ACCESS_CONTROL`
- regular nodes: `Permissions.REMOVE_NODE`

`ACTION_NODE_TYPE_MANAGEMENT`

- `Permissions.NODE_TYPE_MANAGEMENT`

`ACTION_VERSIONING`

- `Permissions.VERSION_MANAGEMENT`

`ACTION_LOCKING`

- `Permissions.LOCK_MANAGEMENT`

`ACTION_READ_ACCESS_CONTROL`

- `Permissions.READ_ACCESS_CONTROL`

`ACTION_MODIFY_ACCESS_CONTROL`

- `Permissions.MODIFY_ACCESS_CONTROL`

`ACTION_USER_MANAGEMENT`

- `Permissions.USER_MANAGEMENT`


#### Permissions for Different Operations

##### Reading

- **Regular Items**:
    Due to the fine grained read permissions Oak read access can be separately granted/denied
    for nodes and properties. Granting the `jcr:read` privilege will result in a backwards compatible
    read access for nodes and their properties, while specifying `rep:readNodes` or
    `rep:readProperties` privileges allows to grant or deny access to
    nodes and properties (see also [Privilege Management](privilege.html) for changes
    in the privilege definitions).
    Together with the restrictions this new behavior now allows to individually grant/deny
    access to properties that match a given name/path/nodetype (and as a possible extension even property value).
- **Version Content**:
    The accessibility of version content located underneath `/jcr:system/jcr:versionStore`
    is defined by the permissions present with the versionable node. In case the version
    information does no longer have a versionable node in this workspace it's original
    versionable path is used to evaluate the effective permissions that would apply
    to that item if the version was restored. This change is covered by [OAK-444] and
    addresses concerns summarized in [JCR-2963].
- **Access Control Content**
    Read access to access control content such node storing policy or ACE information
    requires `READ_ACCESS_CONTROL` permission.

##### Writing

- **Property Modification**:
    Since Oak the former `SET_PROPERTY` permission has been split such to allow for
    more fined grained control on writing JCR properties. In particular Oak clearly
    distinguishes between creating a new property that didn't exist before, modifying
    or removing an existing property. This will allow to cover those cases where a given `Subject` is only allowed
    to create content without having the ability to modify/delete it later on.
- **Node Removal**:
    As of Oak `Node#remove()` only requires sufficient permissions to remove the
    target node. See below for configuration parameters to obtain backwards compatible behavior.
- **Rename**:
    Due to the nature of the diff mechanism in Oak it is no longer possible to distinguish
    between `JackrabbitNode#rename` and a move with subsequent reordering.
- **Move**:
    The current permission evaluation attempts to provide a best-effort handling to
    achieve a similar behavior that it was present in Jackrabbit 2.x by keeping track
    of transient move operations. The current implementation has the following limitations with respect to multiple
    move operations within a given set of transient operations:
    - Move operations that replace an node that has been moved away will not be
        detected as modification by the diff mechanism and regular permission checks for
        on the subtree will be performed.
    - Moving an ancestor of a node that has been moved will only detect the second
        move and will enforce regular permissions checks on the child that has been moved
        in a first step.
- **Managing Index Definitions**:
    Writing query index definitions requires the specific index definition management
    which is enforce on nodes named "oak:index" and the subtree defined by them.
    Note that the corresponding items are not protected in the JCR sense. Consequently
    any other modification in these subtrees like e.g. changing the primary type
    or adding mixin types is governed by the corresponding privileges.

##### Writing Protected Items
Writing protected items requires specific permissions and is not covered by
regular JCR write permissions. This affects:

- **Set/Modify Primary or Mixin Type**: `NODE_TYPE_MANAGEMENT`
- **Access Control Content**: `MODIFY_ACCESS_CONTROL`
- **Locking**: `LOCK_MANAGEMENT`
- **Versioning**:
    Executing version related operations and thus writing to the version store
    requires `VERSION_MANAGEMENT` permission instead of the regular JCR write permissions.
    Similarly, the content in the version store can only be modified using the dedicated
    version management API.
- **User Management**:
    By default user management operations require the specific user management related
    permission `USER_MANAGEMENT` to be granted for the editing subject. This permission (including a corresponding privilege)
    has been introduced with Oak 1.0. See below for configuration parameters to obtain backwards compatible behavior.

##### Observation

Permission evaluation is also applied when delivering observation events
respecting the effective permission setup of the `Session` that registered
the `EventListener`.

However, it is important to understand that events are only delivered once
the modifications have been successfully persisted and permissions will
be evaluated against the persisted state.

In other words: Changing the permission setup along with the modifications 
to be reported to the `EventListener` will result in events being included
or excluded according to the modified permissions. See [OAK-4196] for an example.


<a name="api_extensions"/>
### API Extensions

Due to the separation of access control management from permission evaluation,
Oak 1.0 comes with a dedicated API for permission discovery that is used both
for the repository internal permission evaluation as well as for permission
discovery at JCR level.

The package `org.apache.jackrabbit.oak.spi.security.authorization.permission`
defines the following interfaces and classes:

- [PermissionProvider]: Main entry point for permission discovery and evaluation.
    - [TreePermission]: Evaluates the permissions of a given Oak `Tree`, exposed by `PermissionProvider`.
    - [RepositoryPermission]: Evaluates the repository level permissions, exposed by `PermissionProvider`.
- [AggregatedPermissionProvider]: Extension of the [PermissionProvider] required for implementations that are intended to be used in an aggregation of multiple providers (since Oak 1.4)
- [Permissions]: The permissions defined, respected and evaluated by the repository.
- [PermissionConstants]: Constants used throughout the permission evaluation.

<a name="default_implementation"/>
### Characteristics of the Permission Evaluation

As explained above permission evaluation is completely separated from the access
control management and the associated ccontent.  The evaluation itself is done by
the configured `PermissionProvider`.

Each JCR `Session` (and Oak `ContentSession`) gets it's own `PermissionProvider`
associated with the current repository revision the session is operating on. The
evaluated permissions and caches are not shared between different sessions even
if they represent the same subject.

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](permission/differences.html).

#### Details on the Default Permission Evaluation

The behavior of the default permission implementation is described in sections 
[Permissions: The Default Implementation](permission/default.html) and 
[Permission Evaluation in Detail: The Default Implementation](permission/evaluation.html).

<a name="configuration"/>
### Configuration

The configuration of the permission evaluation implementation is handled
by the [AuthorizationConfiguration], which is used for all authorization
related matters. This class provides the following two permission related
methods:

- `getPermissionProvider(Root, String, Set<Principal>)`: get a new `PermissionProvider` instance.

#### Configuration Parameters

The supported configuration options of the default implementation are described in the corresponding [section](permission/default.html#configuration).

<a name="further_reading"/>
### Further Reading

- [Permissions vs Privileges](permission/permissionsandprivileges.html)
- [Differences wrt Jackrabbit 2.x](permission/differences.html)
- [Permissions : The Default Implementation](permission/default.html)
- [Permission Evaluation in Detail](permission/evaluation.html)
- [Multiplexed PermissionStore](permission/multiplexing.html)
- [Restriction Management](authorization/restriction.html)

<!-- references -->
[Permissions]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.html
[PermissionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/PermissionProvider.html
[TreePermission]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/TreePermission.html
[RepositoryPermission]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/RepositoryPermission.html
[PermissionConstants]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/PermissionConstants.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[PermissionHook]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authorization/permission/PermissionHook.html
[AggregatedPermissionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/AggregatedPermissionProvider.html
[OAK-444]: https://issues.apache.org/jira/browse/OAK-444
[JCR-2963]: https://issues.apache.org/jira/browse/JCR-2963
[OAK-1268]: https://issues.apache.org/jira/browse/OAK-1268
[OAK-4196]: https://issues.apache.org/jira/browse/OAK-4196