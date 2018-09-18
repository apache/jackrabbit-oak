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

Permissions : The Default Implementation
--------------------------------------------------------------------------------

### General Notes

The default implementation of the `PermissionProvider` interface evaluates permissions
based on the information stored in a dedicated part of the repository content call
the [permission store](#permissionStore).

<a name="default_implementation"/>
### Characteristics of the Permission Evaluation

#### Regular Permission Evaluation

See section [Permission Evaluation in Detail](evaluation.html).

#### Readable Trees

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

#### Administrative Access

In the default implementation following principals always have full access to
the whole content repository (except for hidden items that are not exposed
on the Oak API) irrespective of the access control content:

- `SystemPrincipal`
- All instances of `AdminPrincipal`
- All principals whose name matches the configured administrative principal names
(see Configuration section below). This configuration only applies to the permission
evaluation and is currently not reflected in other security models nor methods
that deal with the administrator (i.e. `User#isAdmin`).

#### Permission Evaluation in Multiplexed Stores

See section [Multiplexing support in the PermissionStore](multiplexing.html).

<a name="representation"/>
### Representation in the Repository

<a name="permissionStore"/>
#### Permission Store

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

##### Accessing the Permission Store

It is important to understand that the permission store is a implementation
specific structure that is maintained by the system itself. For this reason
access to the permission store is additionally restricted superimposing the
regular permissions being enforced for regular repository items.

In detail this means that the permission store cannot be written by JCR nor Oak
API method calls. It's immutability is enforced by a dedicated `FailingValidator`
that prevents any modifications underneath `/jcr:system/rep:permissionStore`.
Similarly read access is not allowed except for system principals. In order to
discover and display access control related information API consumers should
use the regular JCR and Jackrabbit permission and access control management API
as listed in the [introduction](../permission.html#jcr_api) and in section 
[Using the Access Control Management API](../accesscontrol/editing.html).

#### Node Type Definitions

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

<a name="validation"/>
### Validation

The consistency of this content structure is asserted by a dedicated `PermissionValidator`.
The corresponding errors are all of type `Access` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0000              | Generic access violation                                 |
| 0021              | Version storage: Node creation without version history   |
| 0022              | Version storage: Removal of intermediate node            |

<a name="configuration"/>
### Configuration

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
