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
### Privilege Management : Mapping Privileges to Items

The following table allows to identify which items will be affected by the
invididual built in privileges.

Note: the term _regular_ is used on contrast to _protected_ items that are written
using special API calls and thus mandate special privileges or are maintained
by the system only and cannot be modified by the API consumer.

#### Read

| Privilege             | Affected Items                                        |
|-----------------------|-------------------------------------------------------|
| rep:readNodes         | all nodes except for access control content           |
| rep:readProperties    | all properties except for access control content      |
| jcr:readAccessControl | all items defining access control content (see below) |

#### Writing Properties

| Privilege             | Affected Items                                        |
|-----------------------|-------------------------------------------------------|
| rep:addProperties     | creation of new regular properties                    |
| rep:alterProperties   | changing existing regular properties                  |
| rep:removeProperties  | removing existing regular properties                  |

#### Writing Nodes

| Privilege             | Affected Items                                        |
|-----------------------|-------------------------------------------------------|
| jcr:addChildNodes     | granted on parent to create new regular child nodes   |
| jcr:removeChildNodes  | granted on parent to remove regular child nodes       |
| rep:removeNode        | required to be granted on regular nodes for removal   |
| jcr:nodeTypeManagement| explicitly setting or modifying node type information on a regular (non-protected) node; affected properties are `jcr:primaryType`, `jcr:mixinTypes` |

#### Access Control Management

| Privilege               | Affected Items                                      |
|-------------------------|-----------------------------------------------------|
| jcr:readAccessControl   | all items defining access control content [1]       |
| jcr:modifyAccessControl | all items defining access control content [1]       |
| rep:privilegeManagement | implementation specific; in Oak everything below `/jcr:system/rep:privileges` |

#### Other Session and Workspace Operations

| Privilege               | Affected Items                                      |
|-------------------------|-----------------------------------------------------|
| jcr:versionManagement   | all items defining version content [2]              |
| jcr:lockManagement      | Properties `jcr:lockIsDeep`, `jcr:lockOwner`        |
| jcr:lifecycleManagement | `jcr:lifecyclePolicy`, `jcr:currentLifecycleState`  |
| jcr:retentionManagement | implementation specific, in Jackrabbit 2.x the following properties: `rep:hold`, `rep:retentionPolicy`, Oak: NA |
| rep:userManagement      | all items defining user/group content [3]           |
| rep:indexDefinitionManagement | implementation specific; in Oak trees starting with an `oak:index` node |

#### Repository Operations

| Privilege               | Affected Items                                      |
|-------------------------|-----------------------------------------------------|
| jcr:namespaceManagement | implementation specific; in Oak everything below `/jcr:system/rep:namespaces` |
| jcr:nodeTypeDefinitionManagement | implementation specific; in Oak everything below `/jcr:system/jcr:nodeTypes` |
| rep:privilegeManagement | implementation specific; in Oak everything below `/jcr:system/rep:privileges` |
| jcr:workspaceManagement | NA                                                  |


#### Annotations

[1] In Oak reading/writing nodes with the following node types provided by the implementations present: `rep:Policy`, `rep:ACL`, `rep:ACE`, `rep:GrantACE`, `rep:DenyACE`, `rep:Restrictions` and `rep:CugPolicy` and all protected items defined therein.
    See [Default Access Control Management](../accesscontrol/default.html) and [Managing Access Control with CUG](../authorization/cug.html), respectively.

[2] Granting jcr:versionManagement privilege at a given versionable node will allow writing items through JCR version management API which writes below `/jcr:system/jcr:versionStorage`, `/jcr:system/jcr:activities`, `/jcr:system/jcr:configurations` and the following properties both in the storage(s) and with the versionable node: `jcr:activity`, `jcr:activityTitle`, `jcr:baseVersion`, `jcr:childVersionHistory`, `jcr:configuration`, `jcr:copiedFrom`, `jcr:frozenMixinTypes`, `jcr:frozenPrimaryType`, `jcr:frozenUuid`, `jcr:isCheckedOut`, `jcr:mergeFailed`, `jcr:predecessors`,`jcr:successors`,`jcr:root`,`jcr:versionableUuid`, `jcr:versionHistory`

[3] in Oak creating nodes with the following primary types: `rep:User`, `rep:SystemUser`, `rep:Group`, `rep:Impersonatable`, `rep:Members`, `rep:MemberReferences`, `rep:MemberReferencesList`, `rep:Password` and all protected properties defined therein
