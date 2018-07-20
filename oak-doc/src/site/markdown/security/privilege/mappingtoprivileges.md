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
### Privilege Management : Mapping API Calls to Privileges

The following table allows to identify which API calls require which type of
privilege(s)

#### Transient Operations

##### Read

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Session.itemExists`                         | `jcr:read`                     |
| `Session.getItem`                            | `jcr:read`                     |
| `Session.nodeExists`                         | `rep:readNodes`                |
| `Session.nodeExists`                         | `rep:readNodes`                |
| `Session.getNode`                            | `rep:readNodes`                |
| `Session.getRootNode`                        | `rep:readNodes`                |
| `Session.getNodeByUUID`                      | `jcr:read`                     |
| `Session.getNodeByIdentifier`                | `jcr:read`                     |
| `Session.getNode`                            | `rep:readNodes`                |
| `Session.propertyExists`                     | `rep:readProperties`           |
| `Session.getProperty`                        | `rep:readProperties`           |
| `Item.getParent`                             | `rep:readNodes` on parent      |
| `Item.getAncestor`                           | `rep:readNodes` on ancestor    |
| `Node.hasNode`                               | `rep:readNodes`                |
| `Node.hasNodes`                              | `rep:readNodes`                |
| `Node.getNode`                               | `rep:readNodes`                |
| `Node.getNodes`                              | `rep:readNodes`                |
| `Node.hasProperty`                           | `rep:readProperties`           |
| `Node.hasProperties`                         | `rep:readProperties`           |
| `Node.getProperty`                           | `rep:readProperties`           |
| `Node.getProperties`                         | `rep:readProperties`           |
| `Node.getUUID`                               | `rep:readProperties`           |
| `Node.getIdentifier`                         | `rep:readProperties`           |
| `Node.getReferences`                         | `rep:readProperties`           |
| `Node.getWeakReferences`                     | `rep:readProperties`           |
| `Node.getPrimaryItem`                        | `jcr:read`                     |
| `Node.getPrimaryNodeType`                    | `rep:readProperties` on jcr:primaryType |
| `Node.getMixinNodeTypes`                     | `rep:readProperties` on jcr:mixinTypes |
| `Property.getValue`                          | `rep:readProperties`           |
| `Property.getValues`                         | `rep:readProperties`           |
| `Property.get*`                              | `rep:readProperties`           |
| `Property.getNode`                           | `rep:readProperties`, `rep:readNodes` on ref-target |
| `Session.exportSystemView`                   | `jcr:read`                     |
| `Session.exportDocumentView`                 | `jcr:read`                     |

##### Writing Properties

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Node.setProperty` (new)                     | `rep:addProperties`            |
| `Node.setProperty` (existing)                | `rep:alterProperties`          |
| `Property.setValue`                          | `rep:alterProperties`          |
| `Property.remove`                            | `rep:removeProperties`         |
| `Node.setProperty(String, null)`             | `rep:removeProperties`         |
| `JackrabbitSession.removeItem` (item is a property) | `rep:removeProperties`  |

##### Writing Nodes

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Node.addNode(String)`                       | `jcr:addChildNodes` (on parent) |
| `Node.remove`                                | `jcr:removeChildNodes` (on parent), `jcr:removeNode` |
| `JackrabbitSession.removeItem` (if item is a node) | `jcr:removeChildNodes` (on parent), `jcr:removeNode` |
| `Node.addNode(String, String)`               | `jcr:addChildNodes` (on parent), `jcr:nodeTypeManagement` |
| `Node.setPrimaryType`                        | `jcr:nodeTypeManagement`       |
| `Node.addMixin`                              | `jcr:nodeTypeManagement`       |
| `Node.removeMixin`                           | `jcr:nodeTypeManagement`       |
| `Node.orderBefore`                           | `jcr:addChildNodes` and `jcr:removeChildNodes` (on parent) |

##### Writing Index Definition

While covered by regular JCR API write operations the target items require a
dedicated privilege despite the fact that the item definitions are not protected
(see Oak JIRA for corresponding discussions).

All items located within a path that contains `oak:index` will be considered part
of the special index definition.

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Node.addNode(String, String)`               | `rep:indexDefinitionManagement`|
| `Node.addNode(String)`                       | `rep:indexDefinitionManagement`|
| `Node.orderBefore`                           | `rep:indexDefinitionManagement`|
| `Node.setProperty`                           | `rep:indexDefinitionManagement`|
| `Property.setValue`                          | `rep:indexDefinitionManagement`|
| `Item.remove` (i.e. Node and Property)       | `rep:indexDefinitionManagement`|
| `JackrabbitSession.removeItem`               | `rep:indexDefinitionManagement`|

##### Move and Import

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Session.move`                               | `jcr:removeChildNodes` (source parent) and `jcr:addChildNodes` (target parent) |
| `Session.importXml`                          | same privileges as if items would be created using regular API calls |

##### Access Control Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `AccessControlManager.getApplicablePolicies` | `jcr:readAccessControl`        |
| `AccessControlManager.getPolicies`           | `jcr:readAccessControl`        |
| `AccessControlManager.getEffectivePolicies`  | `jcr:readAccessControl`        |
| `AccessControlManager.setPolicy`             | `jcr:modifyAccessControl`      |
| `AccessControlManager.removePolicy`          | `jcr:modifyAccessControl`      |
| `PrivilegeManager.registerPrivilege`         | `rep:privilegeManagent` at 'null' path |

##### User Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `UserManager.getAuthorizable`                | `jcr:read`                     |
| `UserManager.findAuthorizable`               | `jcr:read`                     |
| `UserManager.createUser`                     | `rep:userManagement`           |
| `UserManager.createSystemUser`               | `rep:userManagement`           |
| `UserManager.createGroup`                    | `rep:userManagement`           |
| `User.isDisabled`                            | `jcr:read`                     |
| `User.getDisabledReason`                     | `jcr:read`                     |
| `User.disable`                               | `rep:userManagement`           |
| `User.changePassword`                        | `rep:userManagement`           |
| `User.getCredentials`                        | `jcr:read`                     |
| `User.getImpersonation`                      | `jcr:read`                     |
| `Impersonation.getImpersonators`             | `jcr:read`                     |
| `Impersonation.allows`                       | `jcr:read`                     |
| `Impersonation.grantImpersonation`           | `rep:userManagement`           |
| `Impersonation.revokeImpersonation`          | `rep:userManagement`           |
| `Group.getDeclaredMembers`                   | `jcr:read`                     |
| `Group.getMembers`                           | `jcr:read`                     |
| `Group.isDeclaredMember`                     | `jcr:read`                     |
| `Group.isMember`                             | `jcr:read`                     |
| `Group.addMember`                            | `rep:userManagement`           |
| `Group.removeMember`                         | `rep:userManagement`           |
| `Authorizable.getID`                         | `jcr:read`                     |
| `Authorizable.getPrincipal`                  | `jcr:read`                     |
| `Authorizable.getPath`                       | `jcr:read`                     |
| `Authorizable.declaredMemberOf`              | `jcr:read` (on groups listing this user/group as member) |
| `Authorizable.memberOf`                      | `jcr:read` (on groups listing this user/group as member) |
| `Authorizable.remove`                        | `rep:userManagement`           |
| `Authorizable.getPropertyNames`              | `jcr:read` or `rep:readProperties` (no relPath) |
| `Authorizable.hasProperty`                   | `jcr:read` or `rep:readProperties` (no relPath) |
| `Authorizable.getProperty`                   | `jcr:read` or `rep:readProperties` (no relPath) |                     |
| `Authorizable.setProperty` (no relPath)      | `rep:addProperties` and/or `rep:alterProperties` |
| `Authorizable.setProperty` (with relPath     | `rep:addProperties` and/or `rep:alterProperties`, `jcr:addChildNodes` |
| `Authorizable.removeProperty`                | `rep:removeProperties`         |

##### LifeCycle Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Node.followLifecycleTransition`             | `jcr:lifecycleManagement`      |

##### Retention Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `RetentionManager.getHolds`                  | `jcr:read`                     |
| `RetentionManager.getRetentionPolicy`        | `jcr:read`                     |
| `RetentionManager.addHold`                   | `jcr:retentionManagement`      |
| `RetentionManager.removeHold`                | `jcr:retentionManagement`      |
| `RetentionManager.setRetentionPolicy`        | `jcr:retentionManagement`      |
| `RetentionManager.removeRetentionPolicy`     | `jcr:retentionManagement`      |

#### Workspace Operations

##### Move, Copy and Import

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Workspace.move`                             | `jcr:removeChildNodes` (source parent) and `jcr:addChildNodes` (target parent) |
| `Workspace.copy`                             | same privileges as if items would be created using regular API calls |
| `Workspace.importXml`                        | same privileges as if items would be created using regular API calls |

##### Version Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `VersionManager.isCheckedOut`                | `rep:readNodes` on versionable node and `rep:readProperties` on its property `jcr:isCheckedOut` |
| `VersionManager.getVersionHistory`           | `rep:readNodes` on versionable node and `rep:readProperties` on its property `jcr:versionHistory` |
| `VersionManager.getBaseVersion`              | `rep:readNodes` on versionable node and `rep:readProperties` on its property `jcr:baseVersion` |
| `VersionManager.checkin`                     | `jcr:versionManagement` on versionable node |
| `VersionManager.checkout`                    | `jcr:versionManagement` on versionable node |
| `VersionManager.checkpoint`                  | `jcr:versionManagement` on versionable node |
| `VersionManager.restore`                     | _TODO_                         |
| `VersionManager.restoreByLabel`              | _TODO_                         |
| `VersionManager.merge`                       | _TODO_                         |
| `VersionManager.cancelMerge`                 | _TODO_                         |
| `VersionManager.doneMerge`                   | _TODO_                         |
| `VersionManager.createConfiguration`         | _TODO_                         |
| `VersionManager.setActivity`                 | _TODO_                         |
| `VersionManager.createActivity`              | _TODO_                         |
| `VersionManager.removeActivity`              | _TODO_                         |
| `VersionHistory.*` (read)                    | `rep:readNodes` on versionable node |
| `VersionHistory.removeVersion`               | `jcr:versionManagement` on versionable node |
| `Version.*` (read)                           | `rep:readNodes` on versionable node |

NOTE: since Oak 1.0 read/write access to version storage is defined by accessibility of the versionable node and _not_ to the version store items.

##### Lock Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `LockManager.getLock` = `Node.getLock`       | `jcr:read`                     |
| `LockManager.isLocked` = `Node.isLocked`     | `jcr:read`                     |
| `LockManager.holdsLock` = `Node.holdsLock`   | `jcr:read`                     |
| `LockManager.lock` = `Node.lock`             | `jcr:lockManagement`           |
| `LockManager.unlock` = `Node.unlock`         | `jcr:lockManagement`           |

#### Repository Operations 

Note: privileges for repository operations need to be granted|denied on the _null_ path.

##### Namespace Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `NamespaceRegistry.getPrefix`                | `jcr:read`                     |
| `NamespaceRegistry.getPrefixes`              | `jcr:read`                     |
| `NamespaceRegistry.getURI`                   | `jcr:read`                     |
| `NamespaceRegistry.getURIs`                  | `jcr:read`                     |
| `NamespaceRegistry.registerNamespace`        | `jcr:namespaceManagement`      |
| `NamespaceRegistry.unregisterNamespace`      | `jcr:namespaceManagement`      |

##### NodeType Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `NodeTypeManager.hasNodeType`                | `jcr:read`                     |
| `NodeTypeManager.getNodeType`                | `jcr:read`                     |
| `NodeTypeManager.getAllNodeTypes`            | `jcr:read`                     |
| `NodeTypeManager.getPrimaryNodeTypes`        | `jcr:read`                     |
| `NodeTypeManager.getMixinNodeTypes`          | `jcr:read`                     |
| `NodeTypeManager.createNodeTypeTemplate`     | NA                             |
| `NodeTypeManager.createNodeDefinitionTemplate`     | NA                       |
| `NodeTypeManager.createPropertyDefinitionTemplate` | NA                       |
| `NodeTypeManager.registerNodeType`           | `jcr:nodeTypeDefinitionManagement` |
| `NodeTypeManager.registerNodeTypes`          | `jcr:nodeTypeDefinitionManagement` |
| `NodeTypeManager.unregisterNodeType`         | `jcr:nodeTypeDefinitionManagement` |
| `NodeTypeManager.unregisterNodeTypes`        | `jcr:nodeTypeDefinitionManagement` |

##### Privilege Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `PrivilegeManager.getRegisteredPrivileges`   | `jcr:read`                     |
| `PrivilegeManager.getPrivilege`              | `jcr:read`                     |
| `PrivilegeManager.registerPrivilege`         | `rep:privilegeManagement`      |

##### Workspace Management

| API Call                                     | Privilege(s)                   |
|----------------------------------------------|--------------------------------|
| `Workspace.createWorkspace`                  | `jcr:workspaceManagement`      |
| `Workspace.deleteWorkspace`                  | `jcr:workspaceManagement`      |
