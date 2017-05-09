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
### Permissions : Differences wrt Jackrabbit 2.x

#### General Notes

The permission evaluation as present in Oak 1.0 differs from Jackrabbit 2.x
in two fundamental aspects:

1. Permission evaluation has been completely separated from the access control
   content and is executed based on the information stored in the permission store.
2. Each JCR `Session` (or Oak `ContentSession`) gets it's own `PermissionProvider`
   associated with the current repository revision the session is operating on.

#### Permissions

The following permissions are now an aggregation of new permissions:

- `READ`: aggregates `READ_NODE` and `READ_PROPERTY`
- `SET_PROPERTY`: aggregates `ADD_PROPERTY`, `MODIFY_PROPERTY` and `REMOVE_PROPERTY`

The following permissions have been introduced with Oak 1.0:

- `USER_MANAGEMENT`: permission to execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.
- `INDEX_DEFINITION_MANAGEMENT`: permission to create, modify and remove the oak:index node and it's subtree which is expected to contain the index definitions.


#### Evaluation

##### Reading

The only break in terms of backwards compatibility is the accessibility of version
content underneath `/jcr:system/jcr:versionStore`. As of Oak the access to version
content depends on the read permissions present with the versionable node while
Jackrabbit 2.x doesn't apply any special rule. These changes are covered by [OAK-444]
and address the concerns summarized in [JCR-2963].

##### Node Removal
As of Oak `Node#remove()` only requires sufficient permissions to remove the target
node. In contrast to Jackrabbit 2.x the validation will not traverse the tree and
verify remove permission on all child nodes/properties.

In order to obtain backwards compatible behavior with respect to tree removal the
permission evaluation can be configured to traverse down the hierarchy upon removal.
This config flag is a best effort approach but doesn't guarantee an identical behavior.

##### Rename
Due to the nature of the diff mechanism in Oak it is not possible to distinguish
between `JackrabbitNode#rename` and a move with subsequent reordering. Consequently
the permission evaluation will no longer apply the special handling for the renaming
as it was present in Jackrabbit 2.x (renaming just required the ability to modify
the child collection of the parent node).

##### Move
Due to the nature of the diff mechanism in Oak it is no longer possible to treat
move operations the same way as it was implemented in Jackrabbit 2.x.

For API consumers and applications running on Jackrabbit Oak this means that
combinations of multiple moves can not always be properly resolved. Consequently
permissions will be evaluated as if the modifications did not include move
(in general being more restrictive): If the move leads to changes that are detected
by the diff mechanism, regular permissions will be evaluated for all items that
appear to be added, removed or modified, while a regular move operations just
requires `REMOVE_NODE` permission on the source, `ADD_NODE` and `NODE_TYPE_MANAGEMENT`
permissions at the destination.

##### User Management
By default user management operations require the specific user mgt related
permission to be granted for the editing subject. This permission (including a
corresponding privilege) has been introduced with Oak 1.0.
For backwards compatibility with Jackrabbit 2.x this behavior can be turned off
by setting the corresponding configuration flag.

##### Version Management
Reading items in the version store depends on access rights present on the
corresponding versionable node. In case the version information does no longer
have a versionable node in this workspace that original path is used to evaluate
the effective permissions that would apply to that node if the version was restored.
This changes is covered by [OAK-444] and addresses concerns summarized in [JCR-2963].

##### Repository Level Operations
Repository level operations such as namespace, nodetype, privilege and workspace
management require permissions to be defined at the repository level such as 
outlined by JSR 283. This implies that access control policies need to be set at
the `null` path. In contrast to Jackrabbit 2.x permissions defined at any regular
path such as e.g. the root path with be ignored.

#### Configuration

The `omit-default-permission` configuration option present with the Jackrabbit's AccessControlProvider 
implementations is no longer supported with Oak. Since there are no permissions 
installed by default this flag has become superfluous.

<!-- hidden references -->
[Permissions]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.java
[PermissionHook]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authorization/permission/PermissionHook.java
[OAK-444]: https://issues.apache.org/jira/browse/OAK-444
[JCR-2963]: https://issues.apache.org/jira/browse/JCR-2963