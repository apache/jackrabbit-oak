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

### Reading

#### Privilege Discovery

Discover/test privileges for the editing session:

- `AccessControlManager`
    - `hasPrivileges(String, Privilege[])`
    - `getPrivileges(String)`

Discover/test privileges for a set of principal that may differ from those associated
with the reading subject. Note that this method requires editing session to be
able to have `READ_ACCESS_CONTROL` permission on the node associated with the specified
path.

- `JackrabbitAccessControlManager`
    - `hasPrivileges(String, Set<Principal>, Privilege[])`
    - `getPrivileges(String, Set<Principal>, Privilege[])`

##### Note

Usually it is not required for a application to check the privileges/permissions
of a given session (or set of principals) as this evaluation can be left
to the repository.
For rare cases where the application needs to understand if a given set of
principals is actually allowed to perform a given action, it is recommend to
use `Session.hasPermission(String, String)` and either pass the actions strings
defined by JCR or the names of the Oak permissions.

See section [Permissions vs Privileges](../permission/permissionsandprivileges.html) for an
comprehensive overview on the differences between testing permissions on `Session`
and privileges on `AccessControlManager`.


#### Reading Policies

- `AccessControlManager`
    - `getApplicablePolicies(String)`
    - `getPolicies(String)`

- `JackrabbitAccessControlManager`
    - `getApplicablePolicies(Principal)`
    - `getPolicies(Principal)`

##### Examples

###### Read policies bound to a node

    AccessControlManager acMgr = session.getAccessControlManager();
    AccessControlPolicy[] policies = acMgr.getPolicies("/content");

###### Read policies that have not yet been bound to the node

    AccessControlManager acMgr = session.getAccessControlManager();
    AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/content");


#### Reading Policy Content

- `AccessControlList`
    - `getAccessControlEntries()`

- `JackrabbitAccessControlList`
    - `getRestrictionNames()`
    - `getRestrictionType(String)`
    - `isEmpty()`
    - `size()`
    
- `PrincipalSetPolicy`
    - `getPrincipals()` 

#### Reading Effective Policies

- `AccessControlManager`
    - `getEffectivePolicies(String)`


- `JackrabbitAccessControlManager`
    - `getEffectivePolicies(Set<Principal>)`

### Writing

#### Adding Policies

- `AccessControlManager`
    - `setPolicy(String, AccessControlPolicy)`

##### Examples

###### Bind a policy to a node

    AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/content");
    while (it.hasNext()) {
        AccessControlPolicy policy = it.nextPolicy();
        if (policy instanceof NamedAccessControlPolicy && "myPolicy".equals((NamedAccessControlPolicy) policy).getName()) {
            acMgr.setPolicy("/content", policy);
            session.save();
        }
    }

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

- `PrincipalSetPolicy`
    - `addPrincipals(Principal...)`
    - `removePrincipals(Principal...)`
    
- `AccessControlUtils`
    - `getAccessControlList(Session, String)`
    - `getAccessControlList(AccessControlManager, String)`
    - `addAccessControlEntry(Session, String, Principal, String[], boolean)`
    - `addAccessControlEntry(Session, String, Principal, Privilege[], boolean)`
    - `grantAllToEveryone(Session, String)`
    - `denyAllToEveryone(Session, String)`

##### Retrieve Principals

The default and recommended ways to obtain `Principal`s for access control management 
is through the principal management API:

- `PrincipalManager` (see section [Principal Management](../principal.html))
      - `getPrincipal(String)`
      - `getPrivilege(String)`

One way of representing principals in the repository is by the means of user management:
If user management is supported in a given Oak repository (see [OPTION_USER_MANAGEMENT_SUPPORTED] 
repository descriptor), principals associated with a given user/group can be obtained
by calling:

- `Authorizable` (see section [User Management](../user.html))
      - `getPrincipal()`
      
Note however, that this will only work for principals backed by a user/group. 
Principals provided by a different principal management implementation won't be 
accessible through user management.
      
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

##### Examples

###### Modify an AccessControlList

    JackrabbitAccessControlList acl = null;
    // try if there is an acl that has been set before
    for (AccessControlPolicy policy : acMgr.getPolicies("/content")) {
        if (policy instanceof JackrabbitAccessControlList) {
            acl = (JackrabbitAccessControlList) policy;
            break;
        }
    }
    if (acl != null) {
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("jackrabbit");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);

        acl.addEntry(principal, privileges, true);
        acMgr.setPolicy(acl.getPath(), acl);
        session.save();
    }


###### Create or Modify an AccessControlList

    JackrabbitAccessControlList acl = null;
    // try if there is an acl that has been set before
    for (AccessControlPolicy policy : acMgr.getPolicies("/content")) {
        if (policy instanceof JackrabbitAccessControlList) {
            acl = (JackrabbitAccessControlList) policy;
            break;
        }
    }
    if (acl == null) {
        // try if there is an applicable policy
        AccessControlPolicyIterator itr = accessControlManager.getApplicablePolicies("/content");
        while (itr.hasNext()) {
            AccessControlPolicy policy = itr.nextAccessControlPolicy();
            if (policy instanceof JackrabbitAccessControlList) {
                acl = (JackrabbitAccessControlList) policy;
                break;
            }
        }
    }
    if (acl != null) {
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("jackrabbit");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);

        acl.addEntry(principal, privileges, true);
        acMgr.setPolicy(acl.getPath(), acl);
        session.save();
    }

or alternatively use `AccessControlUtils`:

    JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, "/content");
    if (acl != null) {
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("jackrabbit");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(session, Privilege.JCR_READ, Privilege.JCR_WRITE);

        policy.addEntry(principal, privileges, true);
        acMgr.setPolicy(acl.getPath(), acl);
        session.save();
    }

#### Removing Policies

- `AccessControlManager`
    - `removePolicy(String, AccessControlPolicy)`

##### Examples

###### Remove a policy

    for (AccessControlPolicy policy : acMgr.getPolicies("/content");
        if (policy instanceof NamedAccessControlPolicy && "myPolicy".equals((NamedAccessControlPolicy) policy).getName()) {
            acMgr.removePolicy("/content", policy);
            session.save();
        }
    }

### Access Control on Repository Level

##### Examples

###### Allow a Principal to Register Namespaces

    JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, null);
    if (acl != null) {
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("dinosaur");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(session, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT);

        policy.addEntry(principal, privileges, true);
        acMgr.setPolicy(null, acl);
        session.save();
    }

<!-- hidden references -->
[OPTION_USER_MANAGEMENT_SUPPORTED]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/JackrabbitRepository.java
