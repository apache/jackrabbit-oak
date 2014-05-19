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

### JCR API

While access control management is a optional feature, a JCR implementation is
required to support the basic permission checking. The basic requirements for
the permission evalution are defines as follows

> Permissions encompass the restrictions imposed by any access control restrictions
> that may be in effect upon the content of a repository, either implementation
> specific or JCR-defined (Access Control Management)., which consists of

The methods defined to check permissions:

- `Session#hasPermission(String absPath, String actions)`
- `Session#checkPermission(String absPath, String actions)`

The actions are expected to be a comma separated list of any of the following string constants:

- `Session.ACTION_READ`
- `Session.ACTION_ADD_NODE`
- `Session.ACTION_REMOVE`
- `Session.ACTION_SET_PROPERTY`

**Note**: As of Oak 1.0 the these methods also handle the names of the permissions
defined by Oak (see `Permissions#getString(long permissions)`).

##### Examples
###### Test if session has permission to add a new node

Important: `absPath` refers to the node to be created

    Node content = session.getNode("/content");
    if (session.hasPermission("/content/newNode", Session.ACTION_ADD_NODE)) {
         content.addNode("newNode");
         session.save();
    }

###### Test if session has permission to perform version operations

    Node content = session.getNode("/content");
    if (session.hasPermission("/content", Permissions.getString(Permissions.VERSION_MANAGEMENT))) {
         content.checkin();
         session.save();
    }

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


#### Permissions for Different Operations

##### Reading

- **Regular Items**:
    Due to the fine grained read permissions Oak read access can be separately granted/denied
    for nodes and properties. Granting the `jcr:read` privilege will result in a backwards compatible
    read access for nodes and their properties, while specifying `rep:readNodes` or
    `rep:readProperties` privileges allows to grant or deny access to
    nodes and properties (see also [Privilege Management](../privilege.html) for changes
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


### Characteristics of the Permission Evaluation

#### General Notes

As explained above permission evaluation is completely separated from the access
control management and the associated ccontent.  The evaluation itself is done by
the configured `PermissionProvider`.

The default implementation of the `PermissionProvider` interface evaluates permissions
based on the information stored in a dedicated part of the repository content call
the [permission store](#permissionStore).

Similar each JCR `Session` (and Oak `ContentSession`) gets it's own `PermissionProvider`
associated with the current repository revision the session is operating on. The
evaluated permissions and caches are not shared between different sessions even
if they represent the same subject.

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](permission/differences.html).

#### Details on Permission Evaluation

##### Administrative Access

In the default implementation following principals always have full access to
the whole content repository (except for hidden items that are not exposed
on the Oak API) irrespective of the access control content:

- `SystemPrincipal`
- All instances of `AdminPrincipal`
- All principals whose name matches the configured administrative principal names
(see Configuration section below). This configuration only applies to the permission
evaluation and is currently not reflected in other security models nor methods
that deal with the administrator (i.e. `User#isAdmin`).

##### Readable Trees

Oak 1.0 comes with a configurable set of subtrees that are read-accessible to all
subjects irrespective of other access control content taking effect. The original
aim of these readable trees is to assert full acccess to namespace, nodetype and
privilege information and the corresponding configuration therefore lists the
following paths:

- `/jcr:system/rep:namespaces`: stores all registered namespaces
- `/jcr:system/jcr:nodeTypes`: stores all registered node types
- `/jcr:system/rep:privileges`: stores all registered privileges

This default set can be changed or extended by setting the corresponding configuration
option. However, it is important to note that many JCR API calls rely on the
accessibility of the namespace, nodetype and privilege information. Removing the
corresponding paths from the configuration will most probably have undesired effects.

##### Regular Permission Evaluation

See section [Permission Evaluation in Detail](permission/evaluation.html).

#### Permission Representation in the Repository

<a name="permissionStore"/>
##### Permission Store

The permission evaluation present with Oak 1.0 keeps a dedicated location where
permissions are being stored for later evaluation. The store is kept in sync
with the access control content by a separated `PostValidationHook` implementation ([PermissionHook]).

The location of the permission store is `/jcr:system/rep:permissionStore`; in
accordance with other stores underneath `jcr:system` it is global to the whole
repository keeping a separate entry for each workspace present with the repository.

The permission entries are grouped by principal and stored below the store root
based on the hash value of the path of the access controlled node; hash collisions
are handled by adding subnodes accordingly.

    /jcr:system/rep:permissionStore/workspace-name [rep:PermissionStore]
        /principal-name [rep:PermissionStore]
            /1259237738 [rep:PermissionStore]
                /0     [rep:Permissions]
                /1     [rep:Permissions]
                /c0     [rep:PermissionStore]   # hash collision
                    /0      [rep:Permissions]
                    /1      [rep:Permissions]
                    /2      [rep:Permissions]
                /c1     [rep:PermissionStore]   # hash collision
                    /0      [rep:Permissions]
                    /1      [rep:Permissions]
                    /2      [rep:Permissions]
            /47    [rep:PermissionStore]
                /0     [rep:Permissions]
                /1     [rep:Permissions]

Each per path store looks as follows

    "1259237738" {
        "jcr:primaryType": "rep:PermissionStore",
        "rep:accessControlledPath": "/content",
        "0": {
            "jcr:primaryType": "rep:Permissions",
            "rep:isAllow": false,
            "rep:privileges": [32],
            "rep:ntNames": ["nt:unstructured", "oak:Unstructured"]
            ... /* additional restrictions as present in the entry */
        }
    }

##### Node Type Definitions

For the permission store the following built-in node types have been defined:

    [rep:PermissionStore]
      - rep:accessControlledPath (STRING) protected IGNORE
      - rep:numPermissions (LONG) protected IGNORE
      - rep:modCount (LONG) protected IGNORE
      + * (rep:PermissionStore) = rep:PermissionStore protected IGNORE
      + * (rep:Permissions) = rep:Permissions protected IGNORE

    [rep:Permissions]
      - * (UNDEFINED) protected IGNORE
      - * (UNDEFINED) protected multiple IGNORE
      + * (rep:Permissions) = rep:Permissions protected IGNORE

In addition Oak 1.0 defines a specific mixin type that allows to store the path(s)
of the versionable node with each version history. Adding this mixing and updating
the versionable path information is taken care of by a dedicated commit hook
implementation (`VersionablePathHook`).

    [rep:VersionablePaths]
      mixin
      - * (PATH) protected ABORT


<a name="api_extensions"/>
### API Extensions

Due to the separation of access control management from permission evaluation,
Oak 1.0 comes with a dedicated API for permission discovery that is used both
for the repository internal permission evaluation as well as for permission
discovery at JCR level.

The package `org.apache.jackrabbit.oak.spi.security.authorization.permission`
defines the following interface:

- [PermissionProvider]: Main entry point for permission discovery and evaluation.
    - [TreePermission]: Evaluates the permissions of a given Oak `Tree`, exposed by `PermissionProvider`.
    - [RepositoryPermission]: Evaluates the repository level permissions, exposed by `PermissionProvider`.
- [Permissions]: The permissions defined, respected and evaluated by the repository.
- [PermissionConstants]: Constants used throughout the permission evaluation.

### Configuration

The configuration of the permission evaluation implementation is handled
by the [AuthorizationConfiguration], which is used for all authorization
related matters. This class provides the following two permission related
methods:

- `getPermissionProvider(Root, String, Set<Principal>)`: get a new `PermissionProvider` instance.

#### Configuration Parameters

The default implementation supports the following configuration parameters:

| Parameter                         | Type                | Default  | Description |
|-----------------------------------|---------------------|----------|-------------|
| `PARAM_PERMISSIONS_JR2`           | String              | \-       | Enables backwards compatible behavior for the permissions listed in the parameter value containing the permission names separated by ','. Supported values are: `USER_MANAGEMENT`,`REMOVE_NODE` |
| `PARAM_READ_PATHS`                | Set\<String\>       | paths to namespace, nodetype and privilege root nodes  | Set of paths that are always readable to all principals irrespective of other permissions defined at that path or inherited from other nodes. |
| `PARAM_ADMINISTRATIVE_PRINCIPALS` | String[]            | \-       | The names of the additional principals that have full permission and for which the permission evaluation can be skipped altogether. |
| | | | |

##### Supported Values for PARAM_PERMISSIONS_JR2

- `REMOVE_NODE`: if present, the permission evaluation will traverse down the hierarchy upon node removal. This config flag is a best effort approach but doesn't guarantee an identical behavior.
- `USER_MANAGEMENT`: if set permissions for user related items will be evaluated the same way as regular JCR items irrespective of their protection status.

##### Differences to Jackrabbit 2.x

The `omit-default-permission` configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
Since there are no permissions installed by default this flag has become superfluous.


### Pluggability

There are two ways for plugging permission related custom implementations:

1. replace `AuthorizationConfiguration`: if you want to completely replace the way
   authorization is handled in the repository.  In OSGi-base setup this is achieved
   by making the configuration implementation a service. In a non-OSGi-base setup the
   custom configuration must be exposed by the `SecurityProvider` implementation.
2. extend `AuthorizationConfiguration`: it is planned to provide a `CompositeAuthorizationConfiguration`
   that allows to aggregate different authorization implementations (see [OAK-1268]).

<!-- references -->
[Permissions]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.html
[PermissionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/PermissionProvider.html
[TreePermission]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/TreePermission.html
[RepositoryPermission]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/RepositoryPermission.html
[PermissionConstants]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/PermissionConstants.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[PermissionHook]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authorization/permission/PermissionHook.html
[OAK-444]: https://issues.apache.org/jira/browse/OAK-444
[JCR-2963]: https://issues.apache.org/jira/browse/JCR-2963
[OAK-1268]: https://issues.apache.org/jira/browse/OAK-1268