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
### Privilege Management : Differences wrt Jackrabbit 2.x

#### 1. Characteristics of the Privilege Management Implementation

##### General Notes
As of OAK the built-in and custom privileges are stored in the repository 
underneath `/jcr:system/rep:privileges`. Similar to other repository level date
(node types, namespaces and versions) this location is shared by all workspaces
present in the repository. The nodes and properties storing the privilege
definitions are protected by their node type definition.  In addition a specific
privilege `Validator` and `CommitHook` implementations assert the consistency of
the privilege store. The built-in privileges are installed using a dedicated
implementation of the `RepositoryInitializer` [0].

##### Registration of Custom Privileges
As far as registration of custom privileges the OAK implementation behaves
different to Jackrabbit 2.x in the following aspects:
- Registration of new privileges fails with `IllegalStateException` if the editing session has pending changes.
- Any validation is performed by CommitHooks in order to make sure that modifications made on the OAK API directly is equally verified. Subsequently any violation (permission, privilege consistency) is only detected at the end of the registration process. The privilege manager itself does not perform any validation.

#### 2. Built-in Privilege Definitions

- All Privileges as defined by JSR 283
    - jcr:read
    - jcr:modifyProperties
    - jcr:addChildNodes
    - jcr:removeNode
    - jcr:removeChildNodes
    - jcr:readAccessControl
    - jcr:modifyAccessControl
    - jcr:lockManagement
    - jcr:versionManagement
    - jcr:nodeTypeManagement
    - jcr:retentionManagement (NOTE: retention management not yet implemented)
    - jcr:lifecycleManagement (NOTE: lifecycle management not yet implemented)
    - jcr:write
    - jcr:all


- All Privileges defined by JSR 333
    - jcr:workspaceManagement (NOTE: wsp management not yet implemented)
    - jcr:nodeTypeDefinitionManagement
    - jcr:namespaceManagement


- All Privileges defined by Jackrabbit 2.x
    - rep:write
    - rep:privilegeManagement


- New Privileges defined by OAK 1.0:
    - rep:userManagement
    - rep:readNodes
    - rep:readProperties
    - rep:addProperties
    - rep:alterProperties
    - rep:removeProperties
    - rep:indexDefinitionManagement


Note the following differences with respect to Jackrabbit 2.x definitions:
- jcr:read is now an aggregation of rep:readNodes and rep:readProperties
- jcr:modifyProperties is now an aggregation of rep:addProperties, rep:alterProperties and rep:removeProperties

#### 3. Node Type Definitions

The following privilege related built-in node types have been added in OAK 1.0.
They are used to represent built-in and custom privilege definitions in the repository.

    [rep:Privileges]
      + * (rep:Privilege) = rep:Privilege protected ABORT
      - rep:next (LONG) protected multiple mandatory

    [rep:Privilege]
      - rep:isAbstract (BOOLEAN) protected
      - rep:aggregates (NAME) protected multiple
      - rep:bits (LONG) protected multiple mandatory

#### 4. API Extensions

org.apache.jackrabbit.oak.spi.security.privilege

- `PrivilegeBitsProvider` : Provider implementation to read `PrivilegeBits` from the repository content and map names to internal representation (and vice versa) [2].
- `PrivilegeBits`: Internal representation of JCR privileges [3].

#### 5. Configuration

##### PrivilegeConfiguration [1]:
- `getPrivilegeManager` -> returns a new instance of the `PrivilegeManager`
   interface such as exposed by `JackrabbitWorkspace#getPrivilegeManager`.
   Note that the default implementation is based on OAK API and can equally be
   used for privilege related tasks in the OAK layer.

#### 6. References

[0] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/privilege/PrivilegeInitializer.java

[1] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeConfiguration.java

[2] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeBitsProvider.java

[3] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeBits.java