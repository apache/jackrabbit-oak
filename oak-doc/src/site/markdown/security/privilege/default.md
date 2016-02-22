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

Privilege Management : The Default Implementation
--------------------------------------------------------------------------------

### General Notes
As of Oak the built-in and custom privileges are stored in the repository
underneath `/jcr:system/rep:privileges`. Similar to other repository level date
(node types, namespaces and versions) this location is shared by all workspaces
present in the repository. The nodes and properties storing the privilege
definitions are protected by their node type definition and cannot be modified
using regular JCR write methods. In addition a specific `Validator` and `CommitHook`
implementations assert the consistency of the privilege store. The built-in
privileges are installed using a dedicated implementation of the `RepositoryInitializer`.

### Differences wrt Jackrabbit 2.x
A comprehensive list of changes compared to Jackrabbit 2.x can be found in the
corresponding [documentation](differences.html).

### Built-in Privileges

- All Privileges as defined by JSR 283

        jcr:read (NOTE: Aggregate since Oak 1.0)
        jcr:modifyProperties (NOTE: Aggregate since Oak 1.0)
        jcr:addChildNodes
        jcr:removeNode
        jcr:removeChildNodes
        jcr:readAccessControl
        jcr:modifyAccessControl
        jcr:lockManagement
        jcr:versionManagement
        jcr:nodeTypeManagement
        jcr:retentionManagement (NOTE: retention management not implemented in Oak 1.0)
        jcr:lifecycleManagement (NOTE: lifecycle management not implemented in Oak 1.0)
        jcr:write
        jcr:all

- All Privileges defined by JSR 333

        jcr:workspaceManagement (NOTE: wsp management not yet implemented)
        jcr:nodeTypeDefinitionManagement
        jcr:namespaceManagement

- All Privileges defined by Jackrabbit 2.x

        rep:write
        rep:privilegeManagement

- New Privileges defined by OAK 1.0:

        rep:userManagement
        rep:readNodes
        rep:readProperties
        rep:addProperties
        rep:alterProperties
        rep:removeProperties
        rep:indexDefinitionManagement

Please note the following differences with respect to Jackrabbit 2.x definitions:

- `jcr:read` is now an aggregation of `rep:readNodes` and `rep:readProperties`
- `jcr:modifyProperties` is now an aggregation of `rep:addProperties`, `rep:alterProperties` and `rep:removeProperties`

#### New Privileges

The new Privileges introduced with Oak 1.0 have the following effect:

- `rep:userManagement`: Privilege required in order to write items that define user or group specific content.
- `rep:readNodes`: Privilege used to allow/deny read access to nodes (aggregate of `jcr:read`)
- `rep:readProperties`: Privilege used to allow/deny read access to properties (aggregate of `jcr:read`)
- `rep:addProperties`: Privilege required in order to create new properties (aggreate of `jcr:modifyProperties`)
- `rep:alterProperties`: Privilege required in order to change existing properties (aggreate of `jcr:modifyProperties`)
- `rep:removeProperties`: Privilege required in order to remove existing properties (aggreate of `jcr:modifyProperties`)
- `rep:indexDefinitionManagement`: Privilege required to create, modify or deleate index definitions.

#### Mapping Privileges to Items and API Calls
An overview on how the built-in privileges map to API calls and individual items
can be found in ['Mapping Privileges to Items'](mappingtoitems.html)
and ['Mapping API Calls to Privileges'](mappingtoprivileges.html)

<a name="representation"/>
### Representation in the Repository

As of Oak 1.0 all privilege definitions are stored in the repository itself
underneath `/jcr:system/rep:privileges`. The following privilege related built-in
node types have been added in OAK 1.0 in order to represent built-in and custom
privilege definitions.

    [rep:Privileges]
      + * (rep:Privilege) = rep:Privilege protected ABORT
      - rep:next (LONG) protected multiple mandatory

    [rep:Privilege]
      - rep:isAbstract (BOOLEAN) protected
      - rep:aggregates (NAME) protected multiple
      - rep:bits (LONG) protected multiple mandatory

Note the protection status of all child items defined by these node type definitions
as they prevent modification of the privilege definitions using regular JCR
write operations.

<a name="validation"/>
### Validation

The consistency of this content structure is asserted by a dedicated `PrivilegeValidator`.
The corresponding errors are all of type `Constraint` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0041              | Modification of existing privilege definition X          |
| 0042              | Un-register privilege X                                  |
| 0043              | Next bits not updated                                    |
| 0044              | Privilege store not initialized                          |
| 0045              | Modification of existing privilege definition X          |
| 0046              | Modification of existing privilege definition X          |
| 0047              | Invalid declared aggregate name X                        |
| 0048              | PrivilegeBits are missing                                |
| 0049              | PrivilegeBits already in used                            |
| 0050              | Singular aggregation is equivalent to existing privilege.|
| 0051              | Declared aggregate X is not a registered privilege       |
| 0052              | Detected circular aggregation                            |
| 0053              | Custom aggregate privilege X is already covered.         |

<a name="configuration"/>
### Configuration

There are no implementation specific configuration options associated with the 
privilege management implementation.