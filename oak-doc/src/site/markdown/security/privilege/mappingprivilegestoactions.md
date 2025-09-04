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
### Mapping Jcr Actions to Privileges

| Jcr/Jackrabbit Action        | Privilege                                                              |
|------------------------------|------------------------------------------------------------------------|
| ACTION_READ                  | jcr:read                                                               | 
| ACTION_READ on node          | rep:readNodes                                                          |
| ACTION_READ on prop          | rep:readProperties                                                     |
| ACTION_SET_PROPERTY          | jcr:modifyProperties                                                   |
| ACTION_ADD_PROPERTY          | rep:addProperties                                                      |
| ACTION_MODIFY_PROPERTY       | rep:alterProperties                                                    |
| ACTION_REMOVE_PROPERTY       | rep:removeProperties                                                   |
| ACTION_ADD_NODE              | jcr:addChildNodes on parent                                            |    
| ACTION_REMOVE_NODE           | jcr:removeNode on target + jcr:removeChildNodes on parent              |
| ACTION_REMOVE on prop        | rep:removeProperties                                                   |
| ACTION_REMOVE on node        | jcr:removeNode on target + jcr:removeChildNodes on parent              |
| ACTION_NODE_TYPE_MANAGEMENT  | jcr:nodeTypeManagement                                                 |
| - (combination of actions)   | jcr:write (NOTE: add/remove node requires privileges granted on parent) |
| - (combination of actions)   | rep:write (NOTE: add/remove node requires privileges granted on parent) |                                        
| ACTION_USER_MANAGEMENT       | rep:userManagement                                                     |
| ACTION_LOCKING               | jcr:lockManagement                                                     |
| ACTION_VERSIONING            | jcr:versionManagement                                                  |
| -                            | rep:indexDefinitionManagement                                          |
| ACTION_READ_ACCESS_CONTROL   | jcr:readAccessControl                                                  |
| ACTION_MODIFY_ACCESS_CONTROL | jcr:modifyAccessControl                                                |
| -                            | rep:privilegeManagement                                                |
| -                            | jcr:nodeTypeDefinitionManagement                                       |
| -                            | jcr:namespaceManagement                                                |
| -                            | jcr:all                                                                |

Mapping for unsupported operations in Oak

| Jcr/Jackrabbit Action | Privilege               |
|-----------------------|-------------------------|
| - | jcr:retentionManagement |
| - | jcr:lifecycleManagement |
| - | jcr:workspaceManagement |

### Further Reading

- [Mapping Privileges to Items](mappingtoitems.html)
- [Mapping API Calls to Privileges](mappingtoprivileges.html)
- [Mapping of JCR Actions to Oak Permissions](../permission.html#mapping-of-jcr-actions-to-oak-permissions)

