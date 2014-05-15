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

Using the Access Control Management API
--------------------------------------------------------------------------------

_todo_: add examples

### Reading

#### Privilege Discovery

- `AccessControlManager`
    - `hasPrivileges(String, Privilege[])`
    - `getPrivileges(String)`


- `JackrabbitAccessControlManager`
    - `hasPrivileges(String, Set<Principal>, Privilege[])`
    - `getPrivileges(String, Set<Principal>, Privilege[])`

#### Reading Policies

- `AccessControlManager`
    - `getApplicablePolicies(String)`
    - `getPolicies(String)`


- `JackrabbitAccessControlManager`
    - `getApplicablePolicies(Principal)`
    - `getPolicies(Principal)`

#### Reading Policy Content

- `AccessControlList`
    - `getAccessControlEntries()`

- `JackrabbitAccessControlList`
    - `getRestrictionNames()`
    - `getRestrictionType(String)`
    - `isEmpty()`
    - `size()`

#### Reading Effective Policies

- `AccessControlManager`
    - `getEffectivePolicies(String)`


- `JackrabbitAccessControlManager`
    - `getEffectivePolicies(Set<Principal>)`


### Writing

#### Adding Policies

- `AccessControlManager`
    - `setPolicy(String, AccessControlPolicy)`


#### Modifying Policies

Modification of policies is specific to the policy type. JCR/Jackrabbit API only
define a single mutable type of policies: the access control list. Depending on
the access control implementation there may be other mutable policies.

- `AccessControlList`
    - `addAccessControlEntry(Principal, Privilege[])`
    - `removeAccessControlEntry(AccessControlEntry)`


- `JackrabbitAccessControlList`
    - `addAccessControlEntry(Principal, Privilege[], boolean)`
    - `addAccessControlEntry(Principal, Privilege[], boolean, Map<String, Value>)`
    - `addAccessControlEntry(Principal, Privilege[], boolean, Map<String, Value>, Map<String, Value[]>)`
    - `orderBefore(AccessControlEntry, AccessControlEntry)`


- `AccessControlUtils`
    - `getAccessControlList(Session, String)`
    - `getAccessControlList(AccessControlManager, String)`
    - `addAccessControlEntry(Session, String, Principal, String[], boolean)`
    - `addAccessControlEntry(Session, String, Principal, Privilege[], boolean)`
    - `grantAllToEveryone(Session, String)`
    - `denyAllToEveryone(Session, String)`

##### Retrieve Principals

- `PrincipalManager` (see section [Principal Management](../principal.html))
      - `getPrincipal(String)`
      - `getPrivilege(String)`


- `Authorizable` (see section [User Management](../user.html))
      - `getPrincipal()`

##### Retrieve Privileges

- `PrivilegeManager` (see section [Privilege Management](../privilege.html))
      - `getRegisteredPrivileges()`
      - `getPrivilege(String)`


- `AccessControlManager`
    - `getSupportedPrivileges(String)`
    - `privilegeFromName(String)`


- `AccessControlUtils`
    - `privilegesFromNames(Session session, String... privilegeNames)`
    - `privilegesFromNames(AccessControlManager accessControlManager, String... privilegeNames)`


- `Privilege`: defines name constants for the privileges defined by JCR


#### Removing Policies

- `AccessControlManager`
    - `removePolicy(String, AccessControlPolicy)`


### Access Control on Repository Level

_todo_

