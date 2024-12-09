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

# Using the Access Control Management API

<!-- MACRO{toc} -->

<a name="read"></a>
## Reading

### Privilege Discovery

#### Discover/test privileges for the editing session
Note that these methods require the editing session to have `READ_ACCESS_CONTROL` permission for the specified path.

- `AccessControlManager`
    - `hasPrivileges(String, Privilege[])`
    - `getPrivileges(String)`
    
- `JackrabbitAccessControlManager`
    - `getPrivilegeCollection(String absPath)` : improved, performant way to inspect privileges (since Oak 1.42.0, see [OAK-9494])     

#### Discover/test privileges for a set of principal
The following methods can be used to discover privileges for a set of principals different from those associated with the reading subject.

- `JackrabbitAccessControlManager`
    - `hasPrivileges(String, Set<Principal>, Privilege[])`
    - `getPrivileges(String, Set<Principal>, Privilege[])`
    - `getPrivilegeCollection(String absPath, Set<Principal> principals)`: improved, performant way to inspect privileges (since Oak 1.42.0, see [OAK-9494])
    
#### PrivilegeCollection for improved discovery 
The `PrivilegeCollection` interface wraps around a set of privileges. It allows for efficient and repeated privilege testing and avoids manual resolution of privilege aggregation. 
Since Oak 1.42.0. For additional details see [OAK-9494].

- `PrivilegeCollection`
    - `getPrivileges()`
    - `includes(@NotNull String... privilegeNames)`

#### Note

Usually it is not required for an application to check the privileges/permissions of a given session (or set of principals) as this evaluation can be left to the repository.
For rare cases where the application needs to understand if a given set of principals is actually allowed to perform a given action, it is recommend to use `Session.hasPermission(String, String)` and either pass the actions strings defined by JCR or the names of the Oak permissions.
If evaluation of multiple privileges is required (e.g. custom privileges), `JackrabbitAccessControlManager.getPrivilegeCollection` is an improved variant the avoids manually resolving aggregation and repeated expensive calls.

See section [Permissions vs Privileges](../permission/permissionsandprivileges.html) for a comprehensive overview on the differences between testing permissions on `Session` and privileges on `AccessControlManager`.

### Reading Policies

- `AccessControlManager`
    - `getApplicablePolicies(String)`
    - `getPolicies(String)`

- `JackrabbitAccessControlManager`
    - `getApplicablePolicies(Principal)`
    - `getPolicies(Principal)`

#### Examples

##### Read policies bound to a node

    AccessControlManager acMgr = session.getAccessControlManager();
    AccessControlPolicy[] policies = acMgr.getPolicies("/content");

##### Read policies that have not yet been bound to the node

    AccessControlManager acMgr = session.getAccessControlManager();
    AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/content");
    
##### Read policies bound to a principal

Note: depending on the setup the array of policies may contain `PrincipalAccessControlList` (see below)

    JackrabbitAccessControlManager acMgr = ...
    PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
    Principal principal = principalManager.getPrincipal("principalName");
    AccessControlPolicy[] policies = acMgr.getPolicies(principal);
    
##### Read policies that have not yet been bound to th principal

Note: depending on the setup the array of policies may contain non-JCR types like `PrincipalAccessControlList` (see below)

    JackrabbitAccessControlManager acMgr = ...
    PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
    Principal principal = principalManager.getPrincipal("principalName");
    AccessControlPolicy[] policies = acMgr.getApplicablePolicies(principal);

### Reading Policy Content

- `AccessControlList`
    - `getAccessControlEntries()`

- `JackrabbitAccessControlList`
    - `getRestrictionNames()`
    - `getRestrictionType(String)`
    - `isEmpty()`
    - `size()`
    
- `PrincipalAccessControlList` : extension of `JackrabbitAccessControlList` for principal-based access control setup
    - `getPrincipal()` : the principal for which all entries in the list are being defined

- `PrincipalSetPolicy`
    - `getPrincipals()` : the set of principals for which the policy applies

### Reading Effective Policies

Inspect which policies take effect in order to help understand why certain permissions are granted or denied.

- `AccessControlManager`
    - `getEffectivePolicies(String absPath)` : All policies that take effect at the specified path

- `JackrabbitAccessControlManager`
    - `getEffectivePolicies(Set<Principal> principals)` : All policies that take effect for the given set of principals (e.g. as they would be added to the `Subject` upon repository login) 
    - `getEffectivePolicies(Set<Principal> principals, String... absPaths)` : All policies that take effect for the given set of principals filtered for the specified paths (since Oak 1.52.0, see [OAK-10130])
    
#### Note

- Retrieving effective policies is best-effort  
- The list of effective policies will also include policies that are defined by a configuration or implementation and have not been explicit bound to a node/principal through access control write operations.
- Therefore, make sure to not rely on effective policies to have a specific type   

<a name="write"></a>
## Writing

### Adding Policies

- `AccessControlManager`
    - `setPolicy(String, AccessControlPolicy)`

#### Examples

##### Bind a policy to a node

    AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/content");
    while (it.hasNext()) {
        AccessControlPolicy policy = it.nextPolicy();
        if (policy instanceof NamedAccessControlPolicy && "myPolicy".equals((NamedAccessControlPolicy) policy).getName()) {
            acMgr.setPolicy("/content", policy);
            session.save();
        }
    }

##### Bind a principal-based policy to a node

    JackrabbitAccessControlManager acMgr = ...
    Privilege[] privileges = ...
    for (JackrabbitAccessControlPolicy policy : acMgr.getApplicablePolicies(principal)) {
        if (policy instanceof PrincipalAccessControlList) {
            // NOTE: the path to bind the policy is retrieved from the policy and is not the path where the added entry takes effect!
            acMgr.setPolicy(policy.getPath(), policy);
            session.save();
        }    
    }
    

### Modifying Policies

Modification of policies is specific to the policy type. JCR/Jackrabbit API defines the following mutable 
types of policies. Depending on the configured access control setup (and the level of customization) there may be other 
mutable policies.

- `AccessControlList`
    - `addAccessControlEntry(Principal, Privilege[])`
    - `removeAccessControlEntry(AccessControlEntry)`

- `JackrabbitAccessControlList`
    - `addAccessControlEntry(Principal, Privilege[], boolean)`
    - `addAccessControlEntry(Principal, Privilege[], boolean, Map<String, Value>)`
    - `addAccessControlEntry(Principal, Privilege[], boolean, Map<String, Value>, Map<String, Value[]>)`
    - `orderBefore(AccessControlEntry, AccessControlEntry)`

- `PrincipalAccessControlList` : extension of `JackrabbitAccessControlList` for principal-based access control setup
    - `addEntry(String effectivePath, Privilege[] privileges)` : binds an entry to the path where it is expected to take effect.
    - `addEntry(String effectivePath, Privilege[] privileges, Map<String, Value> restrictions, Map<String, Value[]> mvRestrictions)` : binds an entry with restrictions to the path where it is expected to take effect  
  
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

#### Retrieve Principals

The default and recommended ways to obtain `Principal`s for access control management 
is through the principal management API:

- `PrincipalManager` (see section [Principal Management](../principal.html))
      - `getPrincipal(String)`

One way of representing principals in the repository is by the means of user management:
If user management is supported in a given Oak repository (see [OPTION_USER_MANAGEMENT_SUPPORTED] 
repository descriptor), principals associated with a given user/group can be obtained
by calling:

- `Authorizable` (see section [User Management](../user.html))
      - `getPrincipal()`
      
Note however, that this will only work for principals backed by a user/group. 
Principals provided by a different principal management implementation won't be 
accessible through user management.
      
#### Retrieve Privileges

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

#### Examples

##### Modify an AccessControlList

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


##### Create or Modify an AccessControlList

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

Alternatively, use `AccessControlUtils`:

    JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, "/content");
    if (acl != null) {
        PrincipalManager principalManager = jackrabbitSession.getPrincipalManager();
        Principal principal = principalManager.getPrincipal("jackrabbit");
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(session, Privilege.JCR_READ, Privilege.JCR_WRITE);

        policy.addEntry(principal, privileges, true);
        acMgr.setPolicy(acl.getPath(), acl);
        session.save();
    }
    
##### Edit a principal-based AccessControlList

    JackrabbitAccessControlManager acMgr = ...
    ValueFactory vf = ...
    for (JackrabbitAccessControlPolicy policy : acMgr.getPolicies(principal)) {
        if (policy instanceof PrincipalAccessControlList) {
            PrincipalAccessControlList acl = (PrincipalAccessControlList) policy;
            // add entries specifying the path where they take effect
            acl.addEntry("/content", AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ));
            Map<String, Value[]> mvRestrictions = Map.of("rep:subtrees", new Value[] {vf.createValue("subtree1"), vf.createValue("subtree2"});
            acl.addEntry("/var", AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ), Collections.emptyMap(), mvRestrictions);
            
            // NOTE: the path to bind the policy is retrieved from the policy and is not the path where the added entry takes effect!
            acMgr.setPolicy(policy.getPath(), policy);
            session.save();
        }    
    }    

### Removing Policies

- `AccessControlManager`
    - `removePolicy(String, AccessControlPolicy)`

#### Examples

##### Remove a policy

    for (AccessControlPolicy policy : acMgr.getPolicies("/content");
        if (policy instanceof NamedAccessControlPolicy && "myPolicy".equals((NamedAccessControlPolicy) policy).getName()) {
            acMgr.removePolicy("/content", policy);
            session.save();
        }
    }

<a name="repository_level"></a>
### Access Control on Repository Level

A `null` path serves as placeholder to retrieve and edit policies that take effect at the repository as a whole instead 
of being bound to or taking effect at a specific node path. For example being able to administer the namespace registry.

#### Examples

##### Allow a Principal to Register Namespaces

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
[OPTION_USER_MANAGEMENT_SUPPORTED]: /oak/docs/apidocs/org/apache/jackrabbit/api/JackrabbitRepository.html
[OAK-10130]: https://issues.apache.org/jira/browse/OAK-10130
[OAK-9494]: https://issues.apache.org/jira/browse/OAK-9494