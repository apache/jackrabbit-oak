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

#### 1. Characteristics of the Default Implementation

##### General
_TODO_

##### JCR API
###### `Session#hasPermission` and `Session#checkPermission`

Since Oak the permission related API calls not only allow to pass the action strings defined by JCR specification (see constants defined in `Session.java`) but also handles the names of the permission defined by Oak (see `Permissions#getString(long permissions)`).

##### Mapping of JCR Actions to Permissions
_TODO_

##### Permissions
The set of permissions supported by Oak are listed in [Permissions]. The following changes have been compared compared to Jackrabbit 2.x:

- `READ_NODE`: permission to read a node
- `READ_PROPERTY`: permission to read a property
- `ADD_PROPERTY`: permission to create a new property
- `MODIFY_PROPERTY`: permission to change an existing property
- `REMOVE`: aggregation of `REMOVE_NODE` and `REMOVE_PROPERTY`
- `USER_MANAGEMENT`: permission to execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.

The following permissions are now an aggregation of new permissions:

- `READ`: aggregates `READ_NODE` and `READ_PROPERTY`
- `SET_PROPERTY`: aggregates `ADD_PROPERTY`, `MODIFY_PROPERTY` and `REMOVE_PROPERTY`

#### 2. Permission Evaluation

##### Reading
Due to the fine grained read permissions Oak read access can be separately granted/denied
for nodes and properties. See also the section about extended restriction management
in [OAK-792]. Granting the `jcr:read` privilege will result in a backwards compatible
read access for nodes and their properties, while specifying `rep:readNodes` or
`rep:readProperties` privileges allows separately granting or denying access to
nodes and properties (see also [OAK-910] for changes in the privilege definitions).
Together with the restrictions this new behavior now allows to individually grant/deny
access to properties that match a given name/path/nodetype (and as a possible extension even property value).

The only break in terms of backwards compatibility is the accessibility of version
content underneath `/jcr:system/jcr:versionStore`. As of Oak the access to version
content depends on the read permissions present with the versionable node while
Jackrabbit 2.x doesn't apply any special rule. These changes are covered by [OAK-444]
and address the concerns summarized in [JCR-2963].

##### Property Modification
Since Oak the former `SET_PROPERTY` permission has been split such to allow for more fined grained control on writing JCR properties. In particular Oak clearly distinguishes between creating a new property that didn't exist before, modifying or removing an existing property.
This will allow to cover those cases where a given subject is only allowed to create content but doesn't have the ability to modify/delete it later on.

##### Node Removal
As of Oak `Node#remove()` only requires sufficient permissions to remove the target node. In contrast to Jackrabbit 2.x the validation will not traverse the tree and verify remove permission on all child nodes/properties.
In order to obtain backwards compatible behavior with respect to tree removal the permission evaluation can be configured to traverse down the hierarchy upon removal. This config flag is a best effort approach but doesn't guarantee an identical behavior.

##### Rename
Due to the nature of the diff mechanism in Oak it is not possible to distinguish
between `JackrabbitNode#rename` and a move with subsequent reordering. Consequently
the permission evaluation will no longer apply the special handling for the renaming
as it was present in Jackrabbit 2.x (renaming just required the ability to modify
the child collection of the parent node).

##### Move
Due to the nature of the diff mechanism in Oak it is no longer possible to treat
move operations the same way as it was implemented in Jackrabbit 2.x. The current
permission evaluation attempts to provide a best-effort handling to achieve a
similar behavior that it was present in Jackrabbit 2.x.

The current implementation has the following limitations with respect to multiple
move operations within a given set of transient operations:

- Move operations that replace an node that has been moved away will not be
detected as modification by the diff mechanism and regular permission checks for
on the subtree will be performed.
- Moving an ancestor of a node that has been moved will only detect the second
move and will enforce regular permissions checks on the child that has been moved
in a first step.

For API consumers and applications running on Jackrabbit Oak this means that
combinations of multiple moves can not always be properly resolved. Consequently
permissions will be evaluated as if the modifications did not include move
(in general being more restrictive): If the move leads to changes that are detected
by the diff mechanism, regular permissions will be evaluated for all items that
appear to be added, removed or modified, while a regular move operations just
requires `REMOVE_NODE` permission on the source, `ADD_NODE` and `NODE_TYPE_MANAGEMENT`
permissions at the destination.

##### Copy
_TODO: permission evaluation with copy is not yet implemented [OAK-920]_

##### User Management
By default user management operations require the specific user mgt related permission to be granted for the editing subject. This permission (including a corresponding privilege) has been introduced with Oak 1.0.
For backwards compatibility with Jackrabbit 2.x this behavior can be turned off by setting the corresponding configuration flag.

##### Version Management
Reading and writing items in the version store does not follow the regular permission evaluation but depends on access rights present on the corresponding versionable node. In case the version information does no longer have a versionable node in this workspace that original path is used to evaluate the effective permissions that would apply to that node if the version was restored.
Note, that as in Jackrabbit VERSION_MANAGEMENT permission instead of the regular JCR write permissions is required in order to execute version operations and thus modify the version store. These changes are covered by [OAK-444] and address the concerns summarized in [JCR-2963].

#### 3. Administrative Principals
The following principals always have full access to the whole content repository irrespective of the access control content:

- `SystemPrincipal`
- All instances of `AdminPrincipal`
- All principals whose name matches the configured administrative principal names (see Configuration section below). This configuration only applies to the permission evaluation and is currently not reflected in other security models nor methods that deal with the administrator (i.e. `User#isAdmin`).

#### 4. Node Types

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

    [rep:VersionablePaths]
      mixin
      - * (PATH) protected ABORT

#### 5. API Extensions

org.apache.jackrabbit.oak.spi.security.authorization.permission

- `PermissionProvider`: _TODO_
- `Permissions`: _TODO_
- `PermissionConstants`: _TODO_

#### 6. Configuration

Configuration Parameters supported by the default implementation

- `PARAM_PERMISSIONS_JR2`: Enables backwards compatible behavior for the permissions listed in the parameter value. Currently the following values are allowed: `USER_MANAGEMENT` and `REMOVE_NODE`. The parameter value must contain the permission names separated by ','.
- `PARAM_READ_PATHS`: default set of paths that are always readable to all principals irrespective of other permissions defined at that path or inherited from other nodes.
- `PARAM_ADMINISTRATIVE_PRINCIPALS`: The names of the additional principals that have full permission and for which the permission evaluation can be skipped altogether.

Differences to Jackrabbit 2.x
The `omit-default-permission` configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
Since there are no permissions installed by default this flag has become superfluous.

<!-- hidden references -->
[Permissions]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.java
[OAK-444]: https://issues.apache.org/jira/browse/OAK-444
[OAK-792]: https://issues.apache.org/jira/browse/OAK-792
[OAK-910]: https://issues.apache.org/jira/browse/OAK-910
[OAK-920]: https://issues.apache.org/jira/browse/OAK-920
[OAK-710]: https://issues.apache.org/jira/browse/OAK-710
[JCR-2963]: https://issues.apache.org/jira/browse/JCR-2963