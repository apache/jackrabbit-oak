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
### Privilege Management : Mapping Privileges to API Calls and Items

The following table allows to identify which API calls require which type of
privilege(s) and also list the affect items.

#### Read

| Privilege             | API Calls                            | Affected Items    |
|-----------------------|----------------------------------------------------------|
| rep:readNodes         | all operations reading nodes         | all nodes except for access control content (see below) |
| rep:readProperties    | all operations reading properties    | all properties except for access control content (see below) |
| jcr:readAccessControl | `AccessControlManager.getApplicablePolicies`,`AccessControlManager.getPolicies`, `AccessControlManager.getEffectivePolicies` | all nodes and properties that defined access control content (details are implementation specific) |

#### Writing Properties

| Privilege             | API Calls                            | Affected Items    |
|-----------------------|----------------------------------------------------------|
| rep:addProperties     | `Node.setProperty`                   | all regular (non-protected) properties that do not yet exist |
| rep:alterProperties   | `Property.setValue`, `Node.setProperty` | all regular (non-protected) properties that already exist |
| rep:removeProperties  | `Property.remove`, `Node.setProperty(String, null)`, `JackrabbitSession.removeItem` (if item is a property) | all regular (non-protected) properties that do exist |


#### Writing Nodes

| Privilege             | API Calls                            | Affected Items    |
|-----------------------|----------------------------------------------------------|
| jcr:addChildNodes     | `Node.addNode`                       | granted on parent to create regular (non-protected) nodes |
| jcr:removeChildNodes  | `Node.remove`, `JackrabbitSession.removeItem` (if item is a node) | granted on parent to remove regular (non-protected) nodes |
| rep:removeNode        | `Node.remove`, `JackrabbitSession.removeItem` (if item is a node) | granted on the regular (non-protected) node to be removed |
| jcr:nodeTypeManagement| `Node.addNode(String, String)`, `Node.setPrimaryType`, `Node.addMixin`, `Node.removeMixin` | explicitly setting or modifying node type information on a regular (non-protected) node; affected properties are `jcr:primaryType`, `jcr:mixinTypes` |

Please node the following special cases:

- Reorder: `Node.orderBefore` requires `jcr:removeChildNodes` and `jcr:addChildNodes` on the parent.
- Move: `Session.move`, `Workspace.move` require `jcr:removeChildNodes` at the source parent and `jcr:addChildNodes` at the target parent.
- Copy: `Workspace.copy` requires  require same privileges as if items would be created using regular API calls.
- Import: `Session.importXml`, `Workspace.importXml` require same privileges as if items would be created using regular API calls.

#### Access Control Management

| Privilege             | API Calls                            | Affected Items    |
|-----------------------|----------------------------------------------------------|
| jcr:readAccessControl | `AccessControlManager.getApplicablePolicies`,`AccessControlManager.getPolicies`, `AccessControlManager.getEffectivePolicies` | all nodes and properties defining access control content |
| jcr:modifyAccessControl | `AccessControlManager.setPolicy`, `AccessControlManager.removePolicy` | all nodes and properties defining access control content |
| rep:privilegeManagement | `PrivilegeManager.registerPrivilege` | implementation specific |

#### Other Session and Workspace Operations

| Privilege               | API Calls                            | Affected Items    |
|-------------------------|----------------------------------------------------------|
| jcr:versionManagement   | `VersionManager.*` (writing)         | writing `/jcr:system/jcr:versionStorage`, `/jcr:system/jcr:activities`, `/jcr:system/jcr:configurations` and the following properties `jcr:activity`, `jcr:activityTitle`, `jcr:baseVersion`, `jcr:childVersionHistory`, `jcr:configuration`, `jcr:copiedFrom`, `jcr:frozenMixinTypes`, `jcr:frozenPrimaryType`, `jcr:frozenUuid`, `jcr:isCheckedOut`, `jcr:mergeFailed`, `jcr:predecessors`,`jcr:successors`,`jcr:root`,`jcr:versionableUuid`, `jcr:versionHistory` |
| jcr:lockManagement      | `LockManager.*` (writing)            | `jcr:lockIsDeep`, `jcr:lockOwner` |
| jcr:lifecycleManagement | `Node.followLifecycleTransition`     | `jcr:lifecyclePolicy`, `jcr:currentLifecycleState` |
| jcr:retentionManagement | `RetentionManager.*` (all writing)   | implementation specific, in Jackrabbit 2.x the following properties: `rep:hold`, `rep:retentionPolicy` |
| rep:userManagement      | all user mgt operations writing protected items | implementation specific; in Oak creating nodes with the following primary types: `rep:User`, `rep:SystemUser`, `rep:Group`, `rep:Impersonatable`, `rep:Members`, `rep:MemberReferences`, `rep:MemberReferencesList`, `rep:Password` and all protected properties defined therein |
| rep:indexDefinitionManagement | all write operations affecting index definitions | implementation specific; in Oak trees starting with an `oak:index` node |

#### Repository Operations

| Privilege               | API Calls                            | Affected Items    |
|-------------------------|----------------------------------------------------------|
| jcr:namespaceManagement | `NamespaceRegistry.registerNamespace`, `NamespaceRegistry.unregisterNamespace` | implementation specific; in Oak everything below `/jcr:system/rep:namespaces` |
| jcr:nodeTypeDefinitionManagement | `NodeTypeManager.registerNodeType`, `NodeTypeManager.registerNodeTypes`, `NodeTypeManager.unregisterNodeType`, `NodeTypeManager.unregisterNodeTypes` | implementation specific; in Oak everything below `/jcr:system/jcr:nodeTypes` |
| rep:privilegeManagement | `PrivilegeManager.registerPrivilege` | implementation specific; in Oak everything below `/jcr:system/rep:privileges` |
| jcr:workspaceManagement | `Workspace.createWorkspace`, `Workspace.deleteWorkspace` | NA |
