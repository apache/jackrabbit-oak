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

# Best Practices for Authorization

<!-- MACRO{toc} -->

## Before you get started
### Threat Model

Before you start coding, creating content or setting up access control set aside some time to consider what is needed 
when it comes to securing your application (and what could go wrong). In other words: write a threat model and 
make sure you keep updating it as you continue developing.

The following references provide a good overview as well as guidance on how to build a threat model:

- https://shostack.org/resources/whitepapers/threat-modeling-what-why-how
- https://owasp.org/www-community/Threat_Modeling
- https://owasp.org/www-community/Threat_Modeling_Process

### Content Modelling

As suggested in [Jackrabbbit Wiki](https://jackrabbit.apache.org/archive/wiki/JCR/DavidsModel_115513389.html#DavidsModel-Rule#2:Drivethecontenthierarchy,don'tletithappen)
the content hierarchy in your JCR repository should be designed and not just happen. Access control requirements tend to be a good driver.

Make sure the content design allows for a readable and manageable access control setup later on to secure your data. 
Excessive complexity is often a strong indicator for problems with your content model, making its security error prone 
and difficult to reason about (and might ultimately might lead to issues with scaling).

Here is an example of an access control setup (with [Sling RepoInit](https://sling.apache.org/documentation/bundles/repository-initialization.html)) 
illustrating why content with different access requirements should be kept in separate trees and how complexity may yield undesired
effects (see also section 'Remember inheritance' below):

      # Content design that results in complex in ac-setup and a vulnerability
      # ----------------------------------------------------------------------
      
      create path /content
      create path /content/public
      create path /content/content2/also_public   # extra folder with public information
      create path /content/sensitive_info         # sensitive data mixed with regular non-sentive content

      set ACL on /content 
         deny  everyone   jcr:all    # most likely redundant
         allow readers    jcr:read
         allow editors    jcr:read, jcr:write
         deny  readers    jcr:read restriction(rep:subtrees, /sensitive_info)        # what about editors or a subject being both reader and editor?
         allow everyone   jcr:read restriction(rep:subtrees, /public, /also_public) # different public folders??
         
         # ... and what happens with a new node /content/public/abc/sensitive_info?
      end 
      
      # Improved content design
      # ------------------------------------------------------------------------------
      
      create path /content
      create path /content/public
      create path /sensitive_info
      
      set ACL on /content 
          allow readers    jcr:read
          allow editors    jcr:read, jcr:write
      end
      
      set ACL on /content/public     
          allow everyone   jcr:read
      end
      
      # NOTE: no ac setup for sensitive info as neither of the roles must have access

### Define Roles and Tasks

Finally, write down basic characteristics and demands of your application without getting into access control details 
or making any assumptions on how your needs will reflected in the repository:

- what roles are present
- what kind of tasks are those roles designed to perform
- define if you have services accessing the repository and what kind of tasks they need to complete 

Note, that this document should be human readable not go into implementation details:
Instead of writing principal 'content-authors' needs jcr:write on /content, define that you have an asset 'content',
define what kind of data it contains and how sensitive the data is (similar to the threat model).
Then identify what roles are going to interact with this data and how they interact: for example you may identify 
a role that just reading data, a second role that is expected to read and write, and a third one that will only 
approve new content and publish it).

## General Best Practices

### Know how to get what you need

Familiarize yourself with JCR access control management and Oak authorization design and extensions before starting 
to edit the permission setup of your Oak installation. This will help you avoid common pitfalls. If you find yourself 
granting your _content-writers_ role full access to just make it work, you probably left your application vulnerable.

- JCR Specification sections [Access Control Management](https://s.apache.org/jcr-2.0-spec/16_Access_Control_Management.html) 
and [Permissions and Capabilities](https://s.apache.org/jcr-2.0-spec/9_Permissions_and_Capabilities.html)
- [Oak Authorization Documentation](../authorization.html) with separate sections for [Access Control Management](../accesscontrol.html) and [Permission Evaluation](../permission.html).
- Exercises for authorization topics below https://github.com/apache/jackrabbit-oak/tree/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization

### Principle of least privilege

Keep in mind that not having any permissions granted is equivalent to denying everything (which is in 
this case redundant). Start without any access and then keep granting permissions as needed, following the 
[principle of least privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege).
In other words: only grant the minimal set of privileges required to perform a particular task.

### Verification

Write tests upfront and verify that for each role and task the expected effective permissions (see definition of roles) are 
granted. Neither less nor more.

Ideally, your tests will fail as soon as someone is attempting to make any change to the permission setup.
Granting additional permissions may open up the door for a privilege escalation and revoking permissions will break
your application (if it doesn't you did not follow the principle of least privilege).

This may also include assertions that no permissions are granted at resources that are outside the scope of a given role/task.

#### Example: Pseud-code for a permission validator

      PermissionTestRule ptr = ...
      ptr.newPermissionValidator(EveryonePrincipal.NAME)
            .hasNoPermission("/")
            .hasNoPermission("/content")
            .hasNoPermission("/content/protected", READ)
            .hasOnlyPermission("/content/public", READ)
            .hasOnlyPermission("/content/public/child-item, READ)
            .hasNoPermission("/sensitive_info")
            .validate(); 

## Oak Specific Best Practices

### Avoid deny

All authorization models present with Apache Jackrabbbit Oak start without any access granted by default i.e. 
implicit deny everywhere. It is therefore recommended to only grant access where needed and avoid adding explicit 
deny access control entries. In particular in combination with subsequent ```allow```  rules  the overall effect will be hard to 
understand as soon as multiple principals are contained in a given subject.

Be wary if you find yourself adding combinations of denies and allows as it might highlight problematic patterns in 
your content model that will be hard to understand and secure over time.

### Avoid redundancy

Don't specify redundant access control setup just to be on the safe side:

- If access is granted, avoid repeating the same setup down the hierarchy.
- Avoid setup for principals with administrative access for which permission evaluation is omitted. It might even create a false sense of security.

### Principal by principle

Oak authorization is designed to work with `java.security.Principal` which is an abstract representation of any kind of 
entity like e.g. individual, a role, a corporation, a login id or even a service.

While JCR specification does not define how the repository knows about principals, Jackrabbit API defines a
[Principal Management](../principal.html) extension.

#### Not every principal is a user/group

Oak allows plugging custom sources of principals which are all reflected through the principal management API.
Therefore, don't assume that every principal is backed by a user or a group. The repository's user management is just 
one potential source of principals.

##### Example : everyone

     // everyone always exists even if there is no such group in the user management
     
     PrincipalManager principalMgr = ((JackrabbitSession) session).getPrincipalManager();
     Principal everyone = principalManager.getEveryone();

#### Membership is no guarantee

Similarly, make sure you always evaluate permissions to verify if a subject has access granted instead of checking if 
a user is member of a group. How access control defined for a particular group principal affects its members is an 
implementation detail of the authorization setup.

##### Example : administrative access

In the default authorization model full access to the repository can be configured for selected user or group principals.
(see [Configuration Parameters](../permission/default.html#configuration) for the default permission evaluation).
If you wish to determine if a given subject has full access, don't assume that there is a group 'administrators' and that 
its members have full access.

#### Stick with group principals

It is preferable to set up access control for group principals instead of individual user principals and then make sure 
your `PrincipalProvider` resolves principal membership according to your needs.

Further, note that the default authorization model will give precedence to user principals upon evaluation in other words 
default access control entries for user principals will overwrite the effect of groups irrespective of the order in the list (see next section).

The above rule is particularly important for the anonymous user marking access with `GuestCredentials`.
If you setup access control for anonymous it will result in the guest account to have effective permissions that do 
not apply for any authenticated session.

What is usually intended instead is setting up permissions for the _everyone_ group. See also
[PrincipalManager.getEveryone()](/oak/docs/apidocs/org/apache/jackrabbit/api/security/principal/PrincipalManager.html#getEveryone)

### Understand default access control and permission management

#### Remember inheritance

When designing your access control setup keep in mind that effective permissions are inherited
down the node hierarchy: allowing `jcr:read` for _content-readers_ role on /content will also grant _content-readers_ 
access to all nodes and properties in the subtree (e.g. /content/project1 or /content/project1/jcr:title).

In addition, effective permissions get inherited through (nested) group principals according to the set of 
principals resolved and added to the `javax.security.auth.Subject` upon repository login.

See [Permission Evaluation in Detail](../permission/evaluation.html) for additional information as well as the 
exercises at [L3_PrecedenceRulesTest](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/permission/L3_PrecedenceRulesTest.java)

#### Built-in privileges

JSR 382 defines a set of built-in privileges and how they apply to repository operations (see https://s.apache.org/jcr-2.0-javadoc/javax/jcr/security/Privilege.html).
The default set has been extended by Oak to cover additional features outside of the scope defined by JCR (like e.g. index 
or user management). The complete list can be found in [Privilege Management : The Default Implementation](../privilege/default.html). 

The minimal set of privileges required for each operation are outlined in [Mapping API Calls to Privileges](../privilege/mappingtoprivileges.html) 
and [Mapping Privileges to Items](../privilege/mappingtoitems.html). 

##### Privileges affecting the parent node

Note in particular for add/removing a node `jcr:addChildNodes` and `jcr:removeChildNodes` are required on 
the parent node respectively i.e. allowing for modification of the child-node collection. 
In addition `jcr:removeNode` needs to be granted on the target node of the removal.

Thus, the following subtle difference apply when evaluation effective permissions vs. privileges (see 
also [Permissions vs Privileges](../permission/permissionsandprivileges.html)) and exercises at 
[L4_PrivilegesAndPermissionsTest.java](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/permission/L4_PrivilegesAndPermissionsTest.java)):

        String parentPath = "/content/parent";
        String toRemove = "/content/parent/child";
        String toAdd = "/content/parent/newchild";
        
        Session session = ...
                
        // Testing Privileges
        // ----------------------------------------------------------------------------------------
        
        AccessControlManager acMgr = session.getAccessControlManager();
        Privilege jcrAddChildNodes = acMgr.privilegeFromName(Privilege.JCR_ADD_CHILD_NODES);
        Privilege jcrRemoveChildNodes = acMgr.privilegeFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        Privilege jcrRemoveNode = acMgr.privilegeFromName(JCR_REMOVE_NODE);
        
        // test if (unspecified) child nodes can be added/removed from the parent
        boolean canModifyChildCollection = acMgr.hasPrivileges(parentPath, new Privilege[]{jcrAddChildNodes, jcrRemoveChildNodes});
        
        # test if existing child node can be removed
        boolean canRemoveNode = acMgr.hasPrivileges(toRemove, new Privilege[]{jcrRemoveNode});
        
        
        // Testing Permissions (on the target node NOT on the parent)
        // ----------------------------------------------------------------------------------------

        // test if not-yet existing node could be added at /content/parent/newchild
        boolean canAddNode = session.hasPermission(toAdd, Session.ACTION_ADD_NODE);
        
        // test if the existing child node can be removed 
        boolean canRemoveItem = session.hasPermission(toRemove, Session.ACTION_REMOVE);
        boolean canRemoveNode = session.hasPermission(toRemove, JackrabbitSession.ACTION_REMOVE_NODE);
        
        // test if a non-existing node could be removed (not possible with privilege evaluation);
        boolean canRemoveNode = session.hasPermission(toAdd, JackrabbitSession.ACTION_REMOVE_NODE);
  
#### Leverage `PrivilegeCollection`

Since Oak 1.42.0 the Jackrabbit API defines a new interface `PrivilegeCollection` the offers improved support for 
testing effective privileges (see also [OAK-9494](https://issues.apache.org/jira/browse/OAK-9494)). It 
allows avoiding repeated calls to `AccessControlManager.hasPrivileges` and manual resolution of aggregated privileges when 
dealing with the privilege array returned by `AccessControlManager.getPrivileges`.

        // Using PrivilegeCollection
        // ----------------------------------------------------------------------------------------
        
        JackrabbitAccessControlManager acMgr = ...

        PrivilegeCollection pc = acMgr.getPrivilegeCollection(parentPath);
        
        boolean canRemoveChildNodes = pc.includes(Privilege.JCR_REMOVE_CHILD_NODES);
        boolean canModifyChildCollection = pc.includes(Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_ADD_CHILD_NODES);
        
        boolean hasAllPrivileges = pc.includes(Privilege.JCR_ALL);
        assertFalse(hasAllPrivileges);
        
        Privilege[] privilegesOnParentNode = pc.getPrivileges();  

#### Use restrictions to limit effect

Apart from picking the minimal set of privileges you can further minimize the risk of privilege escalation by 
narrowing the effect of a given access control setup on certain items in the subtree. This is achieved by creating 
access control entries that come with an additional restriction.

Note though, that restrictions can affect readability. So, you may want to find a balance between enhanced security 
and simplicity. Revisiting your content design early on will likely be the better choice.

See section [Restriction Management](restriction.html) for additional details as well as lessons [L7_RestrictionsTest](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/accesscontrol/L7_RestrictionsTest.java)
and [L8_GlobRestrictionTest](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/accesscontrol/L8_GlobRestrictionTest.java) 
in the Oak exercise module.

#### Access control setup for system user

If your Oak setup supports principal-based authorization (see [Managing Access by Principal](principalbased.html)) it is 
recommended to leverage it for system sessions associated with OSGi service. It helps to keep application owned 
access control setup apart from regular content.

See also [Service Authentication](https://sling.apache.org/documentation/the-sling-engine/service-authentication.html) in 
Apache Sling.

### Leverage customizations

Oak allows for customization and extensions of all parts of the authorization setup.
If you find yourself struggling to reflect your needs with the built-in functionality, consider extending and customizing
the authorization configuration of the repository.

#### Leverage custom privileges

If you identify application specific operations that cannot be reflected using the built-in privileges, Oak allows  
registering custom privileges (see section [Privilege Management](../privilege.html#jackrabbit_api)). 

However, note that the built-in permission evaluation will not enforce those 
custom privileges. Instead, you have to enforce it in your application or write a custom authorization model 
(see section [Combining Multiple Authorization Models](composite.html))

In the example above you might find that publishing content cannot easily be secured using built-in privileges and end 
up registering a custom _myapp:publish_ privilege.

#### Leverage custom restrictions

Default authorization in Oak allows to limit the effect of individual JCR access control entries by means of restrictions.
See section [Restriction Management](restriction.html) for the built-in restrictions and instructions on how to plug 
custom restrictions into the security setup. Be aware though of the potential performance impact of any additional evaluation.

#### Leverage a custom authorization model

If you find that that built-in authorization model is not suited to reflect your needs and setting up access control 
becomes cumbersome and overly complex, consider customizing authorization setup (see section [Combining Multiple Authorization Models](composite.html)).

The _oak-exercise_ module defines a couple of [examples](https://github.com/apache/jackrabbit-oak/tree/trunk/oak-exercise/src/main/java/org/apache/jackrabbit/oak/exercise/security/authorization/models)
to illustrate alternative approaches. The corresponding training material is located in section 
[Advanced Authorization Topics](https://github.com/apache/jackrabbit-oak/tree/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/advanced).
