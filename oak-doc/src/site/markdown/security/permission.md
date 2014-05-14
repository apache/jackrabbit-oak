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

_todo_

**`Session#hasPermission` and `Session#checkPermission`**

_todo_

**JCR Actions**

_todo_

##### Mapping of JCR Actions to Oak Permissions

`ACTION_READ':

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

**Note**
Since Oak the permission related API calls not only allow to pass the action strings
defined by JCR specification (see constants defined in `Session.java`) but also
handles the names of the permission defined by Oak (see `Permissions#getString(long permissions)`).


### Oak API

_todo_

#### Built-in Permissions

The set of permissions supported by Oak are listed in [Permissions]. The following changes have been compared compared to Jackrabbit 2.x:

- `READ_NODE`: permission to read a node
- `READ_PROPERTY`: permission to read a property
- `ADD_PROPERTY`: permission to create a new property
- `MODIFY_PROPERTY`: permission to change an existing property
- `REMOVE`: aggregation of `REMOVE_NODE` and `REMOVE_PROPERTY`
- `USER_MANAGEMENT`: permission to execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.
- `INDEX_DEFINITION_MANAGEMENT`: permission to create, modify and remove the oak:index node and it's subtree which is expected to contain the index definitions.

The following permissions are now an aggregation of new permissions:

- `READ`: aggregates `READ_NODE` and `READ_PROPERTY`
- `SET_PROPERTY`: aggregates `ADD_PROPERTY`, `MODIFY_PROPERTY` and `REMOVE_PROPERTY`

#### New Permissions

_todo_

- `USER_MANAGEMENT`: permission to execute user management related tasks such as e.g. creating or removing user/group, changing user password and editing group membership.
- `INDEX_DEFINITION_MANAGEMENT`: permission to create, modify and remove the oak:index node and it's subtree which is expected to contain the index definitions.


### Characteristics of the Permission Evaluation

#### General Notes

In general the permission evaluation related code in Oak is intended to be
more clearly separated from the access control management such as defined by the
JCR and Jackrabbit API. While permission evaluation is considered to be an
internal feature of the Oak core module, the package
`org.apache.jackrabbit.oak.spi.security.authorization.permission` provides some
extensions points that allow to plug custom extensions or implementations of
the permission evaluation.

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](permission/differences.html).


#### Permission Representation in the Repository

_todo_

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


#### Administrative Access
In the default implementation following principals always have full access to
the whole content repository (except for hidden items that are not exposed
on the Oak API) irrespective of the access control content:

- `SystemPrincipal`
- All instances of `AdminPrincipal`
- All principals whose name matches the configured administrative principal names (see Configuration section below). This configuration only applies to the permission evaluation and is currently not reflected in other security models nor methods that deal with the administrator (i.e. `User#isAdmin`).


#### Detains on Permission Evaluation

_todo_

see [details](permission/evaluation.html)


### API Extensions

_todo_

org.apache.jackrabbit.oak.spi.security.authorization.permission

- `PermissionProvider`: Main entry point for Oak internal permission evaluation.
- `Permissions`: The permissions defined, respected and evaluated by the repository.
- `PermissionConstants`: Constants used throughout the permission evaluation.


### Configuration

- [AuthorizationConfiguration]: _todo_


#### Configuration Parameters

| Parameter                         | Type                | Default  | Description |
|-----------------------------------|---------------------|----------|-------------|
| `PARAM_PERMISSIONS_JR2`           | String              | \-       | Enables backwards compatible behavior for the permissions listed in the parameter value containing the permission names separated by ','. Supported values are: `USER_MANAGEMENT`,`REMOVE_NODE` |
| `PARAM_READ_PATHS`                | Set\<String\>       | paths to namespace, nodetype and privilege root nodes  | Set of paths that are always readable to all principals irrespective of other permissions defined at that path or inherited from other nodes. |
| `PARAM_ADMINISTRATIVE_PRINCIPALS` | String[]            | \-       | The names of the additional principals that have full permission and for which the permission evaluation can be skipped altogether. |
| | | | |


Differences to Jackrabbit 2.x

The `omit-default-permission` configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
Since there are no permissions installed by default this flag has become superfluous.

#### Write Custom Permission Evaluation

_todo_



<!-- references -->
[Permissions]: /oak/docs/apidocs/org/apache/jackrabbit/org/apache/jackrabbit/oak/spi/security/authorization/permission/Permissions.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
