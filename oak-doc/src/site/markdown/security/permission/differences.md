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

_todo_

##### Permissions

The following permissions are now an aggregation of new permissions:

- `READ`: aggregates `READ_NODE` and `READ_PROPERTY`
- `SET_PROPERTY`: aggregates `ADD_PROPERTY`, `MODIFY_PROPERTY` and `REMOVE_PROPERTY`

The following permissions have been introduced with Oak 1.0:

- `USER_MANAGEMENT`: permission to execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.
- `INDEX_DEFINITION_MANAGEMENT`: permission to create, modify and remove the oak:index node and it's subtree which is expected to contain the index definitions.


#### Permission Evaluation

##### Reading

The only break in terms of backwards compatibility is the accessibility of version
content underneath `/jcr:system/jcr:versionStore`. As of Oak the access to version
content depends on the read permissions present with the versionable node while
Jackrabbit 2.x doesn't apply any special rule. These changes are covered by [OAK-444]
and address the concerns summarized in [JCR-2963].

##### Property Modification
Since Oak the former `SET_PROPERTY` permission has been split such to allow for
more fined grained control on writing JCR properties. In particular Oak clearly
distinguishes between creating a new property that didn't exist before, modifying
or removing an existing property.

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

##### User Management
By default user management operations require the specific user mgt related
permission to be granted for the editing subject. This permission (including a
corresponding privilege) has been introduced with Oak 1.0.
For backwards compatibility with Jackrabbit 2.x this behavior can be turned off
by setting the corresponding configuration flag.

##### Version Management
Reading and writing items in the version store does not follow the regular permission
evaluation but depends on access rights present on the corresponding versionable node.
In case the version information does no longer have a versionable node in this workspace
that original path is used to evaluate the effective permissions that would apply
to that node if the version was restored.
Note, that as in Jackrabbit VERSION_MANAGEMENT permission instead of the regular
JCR write permissions is required in order to execute version operations and thus
modify the version store. These changes are covered by [OAK-444] and address the
concerns summarized in [JCR-2963].

<!-- hidden references -->
[Permissions]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.java
[OAK-444]: https://issues.apache.org/jira/browse/OAK-444
[JCR-2963]: https://issues.apache.org/jira/browse/JCR-2963