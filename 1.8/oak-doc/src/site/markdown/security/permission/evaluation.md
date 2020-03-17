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

Permission Evaluation in Detail
--------------------------------------------------------------------------------

<a name="permissionentries"/>
### Order and Evaluation of Permission Entries

In order to evaluate the permissions for a given item, the `PermissionProvider`
lazily builds an iterator of `PermissionsEntry` representing the rep:Permission
present in the permission store that take effect for the given set of principals
at the given node (or property).

Each `PermissionsEntry` stores the privileges granted/denied together with any
restrictions that may be defined with the original access control entry.

This iterator is a concatenation between all entries associated with user principals
followed by the entries associated with group principals.

The order of precedence is as follows:

- permissions are inherited throughout the item hierarchy
- user principals always take precedence over group principals irrespective of
    - their order in the access control list
    - their position in the node hierarchy
- within a given type of principal (user vs. group principal) the order of executing is
    - order of entries as specified originally (the index of the permission entry)
    - entries associated with the target tree take precedence over inherited entries

##### Examples

###### Simple Inheritance

    /content
        allow - everyone - READ permission

Result:

- everyone is allowed to read the complete tree defined by /content

###### Simple Inheritance with Restrictions

    /content
        allow - everyone - READ permission
        deny - everyone - READ_PROPERTY permission - restriction rep:itemNames = ['prop1', 'prop2']

Result:

- everyone is can read the complete tree defined by /content _except_ for properties named 'prop1' or 'prop2' which are explicitly denied by the restricting entry.

###### Inheritance with Allow and Deny

    /content
        deny - everyone - READ permission

    /content/public
        allow - everyone - READ permission

Result:

- everyone cannot read items at the tree defined by /content
- except for tree defined by /content/public which is accessible.

###### Inheritance with Multiple Allows

    /content
        allow - everyone - READ permission

    /content/public
        allow - everyone - REMOVE permission

Result:

- everyonce can read item at /content and the complete subtree
- in addition everyone can remove items underneath /content/public


###### Inheritance with Different Principals

    /content
        allow - everyone - READ permission
        allow - authorGroup - REMOVE permission

Result:

- a subject being member of everyone is allowed to read at /content and the complete subtree
- a subject being member of authorGroup is only allowed to remove items at /content
- a subject being member of both everyone _and_ authorGroup
  has full read-access at /content _and_ can also remove items.



    /content
        allow - everyone - READ permission

    /content/private
        deny - everyone - READ permission
        allow - powerfulGroup - ALL permission

Result:

- a subject being member of everyone
    - is allowed to read at /content and the complete subtree
    - except for /content/private
- a subject being member of powerfulGroup
    - has full permission at /content/private
- a subject being member of both everyone _and_ powerfulGroup
    - has full read-access at /content
    - has full permission underneath /content/private


###### Interaction of User and Group Principals

    /home/jackrabbit
        allow - jackrabbit - ALL permission
        deny - everyone - ALL permission


Result:

- a subject containing the 'jackrabbit' user principal has full permission at /home/jackrabbit  irrespective of the presense of everyone group principal in the subject.
- any other subject has not access at /home/jackrabbit


    /home/jackrabbit
        allow - jackrabbit - ALL permission

    /home/jackrabbit/private
        deny - everyone - ALL permission


Result:

- a subject containing the 'jackrabbit' user principal has full permission at the tree defined by /home/jackrabbit irrespective of the presense of everyone group principal in the subject.
- any other subject is explicitly denied access to /home/jackrabbit/private


### Some Examples: Step by Step

#### Reading

##### Reading a Node

The following section describes what happens on `Session.getNode("/foo").getProperty("jcr:title")`
in terms of permission evaluation:

  1. `SessionImpl.getNode()` internally calls `SessionDelegate.getNode()`
     which calls `Root.getTree()` which calls `Tree.getTree()` on the `/foo` tree.
     This creates a bunch of linked `MutableTree` objects.

  1. The session delegate then checks if the tree really exists, by calling `Tree.exists()`
     which then calls `NodeBuilder.exists()`.

  1. If the session performing the operation is an _admin_ session, then the node builder from
     the persistence layer is directly used. In all other cases, the original node builder
     is wrapped by a `SecureNodeBuilder`. The `SecureNodeBuilder` performs permission
     checks before delegating the calls to the delegated builder.

  1. For non _admin_ sessions the `SecureNodeBuilder` fetches its _tree permissions_ via
     `getTreePermission()`.

  1. The `TreePermission` is responsible for evaluating the permissions granted or
     denied for a given Oak `Tree` and it's properties. In order to test if a the
     tree itself is accessible `TreePermission#canRead()` is called and checks the
     `READ_NODE` permission for normal trees (as in this example) or the `READ_ACCESS_CONTROL`
     permission on _AC trees_. The result is remembered in the `ReadStatus` kept
     with this `TreePermission` instance.

  1. The read status is based on the evaluation of the  _permission entries_ that
     are effective for this tree and the set of principals associated with the
     permission provider. They are retrieved internally by calling `getEntryIterator()`.

  1. The _permission entries_ are [analyzed](#entry_evaluation) if they include the respective permission and if so,
     the read status is set accordingly. Note that the sequence of the permission entries from
     the iterator is already in the correct order for this kind of evaluation. This is ensured
     by the way how they are stored in the [permission store](default.html#permissionStore) and how they
     are feed into the iterator (see [Order and Evaluation of Permission Entries](#permissionentries) above).

     The iteration also detects if the evaluated permission entries cover _this_ node and all
     its properties. If this is the case, subsequent calls that evaluate the property read
     permissions would then not need to do the same iteration again. In order to detect this,
     the iteration checks if a non-matching permission entry or privilege was skipped
     and eventually sets the respective flag in the `ReadStatus`. This flag indicates if the
     present permission entries are sufficient to tell if the session is allowed to read
     _this_ node and all its properties. If there are more entries present than the ones needed
     for evaluating the `READ_NODE` permission, then it's ambiguous to determine if all
     properties can be read.

  1. Once the `ReadStatus` is calculated (or was calculated earlier) the `canRead()` method
     returns `ReadStatus.allowsThis()` which specifies if _this_ node is allowed to be read.

##### Reading a Property

  1. `Node.getProperty()` internally calls `NodeDelegate.getPropertyOrNull()`
     which first resolves the parent node as indicated by the relative path without
     testing for it's existence. Then a new `PropertyDelegate` is created from
     the parent node and the name of the property, which internal obtains the
     `PropertyState` from the Oak `Tree`, which may return `null`.

  1. The node delegate then checks if the property really exists (or is accessible
     to the reading session by calling `PropertyDelegate.exists()` asserting if the
     underlying `PropertyState` is not `null`.

  1. If the session performing the operation is an _admin_ session, then the property state from
     the persistence layer is directly used. In all other cases, the original node builder
     is wrapped by a `SecureNodeBuilder`. The `SecureNodeBuilder` performs permission
     checks before delegating the calls to the delegated builder.

  1. For non _admin_ sessions the `SecureNodeBuilder` fetches its _tree permissions_ via
     `getTreePermission()`.

  1. The `TreePermission` is responsible for evaluating the permissions granted or
     denied for a given Oak `Tree` and it's properties. In order to test if the
     property is accessible `TreePermission#canRead(PropertyState)` is called and checks the
     `READ_PROPERTY` permission for regular properties or the `READ_ACCESS_CONTROL`
     permission for properties defining access control related content. In case
     all properties defined with the parent tree are accessible to the editing
     session the result is remembered in the `ReadStatus` kept with this `TreePermission`
     instance; otherwise the _permission entries_ are collected and evaluated as described 
     [above](#permissionentries).

#### Session Write-Operations

##### Adding a Node

  1. `Node.addNode(String)` will internally call `NodeDelegate.addChild` which
     in term, adds a new child to the corresponding Oak `Tree` and generate all
     autocreated child items.

  1. Once `Session.save()` is called all pending changes will be merged into the
     `NodeStore` present with the editing Oak `Root`. This is achieved by calling
     `Root#commit`.

  1. The permission evaluation is triggered by means of a specific `Validator`
     implementation that is passed over to the merge along with the complete set
     of validators and editors that are combined into a single `CommitHook`.

  1. The `PermissionValidator` will be notified about the new node being added.

  1. It again obtains the `TreePermission` object form the `PermissionProvider` and
     evaluates if `ADD_NODE` permission is being granted for the new target node.
     The evaluation follows the same principals as described [above](#permissionentries).

  1. If added the new node is granted the validation continues otherwise the `commit`
     will fail immediately with an `CommitFailedException` of type `ACCESS`.

##### Changing a Property

  1. `Property.setValue` will internally call `PropertyDelegate.setState` with
     an new `PropertyState` created from the new value (or the new set of values).

  1. Once `Session.save()` is called all pending changes will be merged into the
     `NodeStore` present with the editing Oak `Root`. This is achieved by calling
     `Root#commit`.

  1. The permission evaluation is triggered by means of a specific `Validator`
     implementation that is passed over to the merge along with the complete set
     of validators and editors that are combined into a single `CommitHook`.

  1. The `PermissionValidator` will be notified about the modified property.

  1. It again obtains the `TreePermission` object form the `PermissionProvider` and
     evaluates if `MODIFY_PROPERTY` permission is being granted.
     The evaluation follows the same principals as described [above](#permissionentries).

  1. If changing this property is allowed the validation continues otherwise the `commit`
     will fail immediately with an `CommitFailedException` of type `ACCESS`.

#### Workspace Operations

##### Copying Nodes

  1. `Workspac.copy` will internally call `WorkspaceDelegate.copy`.

  1. After some preliminary validation the delegate will create a new `WorkspaceCopy`
     and call it's `perform` method passing in the separate `Root` instance obtained
     from `ContentSession.getLatestRoot()`; in other words the modifications made
     by the copy operation will not show up as transient changes on the editing
     session.

  1. Upon completion of the copy operation `Root.commit` is called on that latest
     root instance and the delegated will refresh the editing session to reflect
     the changes made by the copy.

  1. The permission evaluation is triggered upon committing the changes associated
     with the copy by the same `Validator` that handles transient operations.

  1. The `PermissionValidator` will be notified about the new items created by the
     copy and checks the corresponding permissions with the `TreePermission` associated
     with the individual new nodes. The evaluation follows the same principals
     as described [above](#permissionentries).

  1. If a permission violation is detected the `commit` will fail immediately with
     an `CommitFailedException` of type `ACCESS`.

##### Locking a Node

  1. `LockManager.lock` will internally call `NodeDelegate.lock`, which will
     obtain a new `Root` from the editing `ContentSession` and perform the
     required changes on that dedicated root such that the editing session is
     not affected.

  1. Once the lock operation is complete the delegate will call `Root.commit`
     on the latest root instance in order to persist the changes. Finally the
     lock manager will refresh the editing session to reflect the changes made.

  1. The permission evaluation is triggered upon committing the changes associated
     with the lock operation by the same `Validator` that handles transient operations.

  1. The `PermissionValidator` will be notified about the new items created by the
     lock and identify that they are associated with a lock specific operations.
     Consequently it will checks for `LOCK_MANAGEMENT` permissions being granted
     at the affected tree. The evaluation triggered by calling `TreePermission.isGranted`
     and follows the same principals as described [above](#permissionentries).

  1. If a permission violation is detected the `commit` will fail immediately with
     an `CommitFailedException` of type `ACCESS`.

#### Repository Operations

##### Registering a Privilege

  1. `PrivilegeManager.registerPrivilege` will obtain a new `Root` from the editing
     `ContentSession` and pass it to a new `PrivilegeDefinitionWriter` that is
     in charge of writing the repository content associated with a new privilege
     definition. Finally the writer will persist the changes by calling `Root.commit`.

  1. Validation of the new privilege definition if delegated to a dedicated
     `PrivilegeValidator`.

  1. The permission evaluation is triggered upon committing the changes associated
     by the same `Validator` that handles transient operations.

  1. The `PermissionValidator` will be notified about changes being made to the
     dedicated tree storing privilege information and will specifically verify
     that `PRIVILEGE_MANAGEMENT` permissions being granted at the repository level.
     This is achieved by obtaining the `RepositoryPermission` object from the
     `PermissionProvider` and calling `RepositoryPermission.isGranted`. The evaluation
     follows the same principals as described [above](#permissionentries).

  1. If a permission violation is detected the `commit` will fail immediately with
     an `CommitFailedException` of type `ACCESS`.

  1. Once the registration is successfully completed the manager will refresh the
     editing session.

