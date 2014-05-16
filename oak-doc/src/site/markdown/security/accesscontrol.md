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

Access Control
--------------------------------------------------------------------------------

### JCR API

Access Control Management is an optional feature defined by [JSR 283] consisting of

> • Privilege discovery: Determining the privileges that a user has in relation to a node.
>
> • Assigning access control policies: Setting the privileges that a user has in relation to a node using access control policies specific to the implementation.

Whether or not a given implementation supports access control management is defined
by the `Repository.OPTION_ACCESS_CONTROL_SUPPORTED` descriptor.

Since Oak comes with a dedicated [privilege management](privilege.html) this section
focuses on reading and editing access control information. The main interfaces defined
by JSR 283 are:

- `AccessControlManager`: Main entry point for access control related operations
- `AccessControlPolicy`: Marker interface for any kind of policies defined by the implementation.
    - `AccessControlList`: mutable policy that may have a list of entries.
    - `NamedAccessControlPolicy`: opaque immutable policy with a JCR name.
- `AccessControlEntry`: association of privilege(s) with a given principal bound to a given node by the `AccessControlList`.

The JCR access control management has the following characteristics:

- *path-based*: policies are bound to nodes; a given node may have multiple policies; the `null` path identifies repository level policies.
- *transient*: access control related modifications are always transient
- *binding*: policies are decoupled from the repository; in order to bind a policy to a node or apply modifications made to an existing policy `AccessControlManager.setPolicy` must be called.
- *effect*: policies bound to a given node only take effect upon `Session.save()`. Access to properties is defined by the their parent node.
- *scope*: a given policy may not only affect the node it is bound to but may have an effect on accessibility of items elsewhere in the workspace.

### Jackrabbit API

The Jackrabbit API defines various access control related extensions to the
JCR API in order to cover common needs such as for example:

- *deny access*: access control entries can be defined to deny privileges at a given path (JCR only defines allowing access control entries)
- *restrictions*: limit the effect of a given access control entry by the mean of restrictions
- *convenience*:
    - reordering of access control entries in a access control list
    - retrieve the path of the node a given policy is (or can be) bound to
- *principal-based*:
    - principal-based access control management API (in contrast to the path-based default specified by JSR 283)
    - privilege discovery for a set of principals

The following interfaces and extensions are defined:

- `JackrabbitAccessControlManager`
- `JackrabbitAccessControlPolicy`
- `JackrabbitAccessControlList`
- `JackrabbitAccessControlEntry`

### Edit Access Control

see section [Using the Access Control Management API](accesscontrol/editing.html) for
a comprehensive list of method calls as well as examples that may be used to
edit the access control content of the repository.

### Characteristics of the Default Implementation

#### General

In general the authorization related code in Oak clearly separates between access
control management (such as defined by the JCR and Jackrabbit API) and the internal
permission evaluation (see also [Permission Evaluation](permission/differences.html)).

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](accesscontrol/differences.html).

#### Resource vs Principal Based Access Control

The default implementation present with Oak 1.0 is natively resource-based which
corresponds to the way JCR defines access control. Nevertheless the principal
based approach as defined by the Jackrabbit API is supported using a best-effort
approach: principal-based policies are created using the Oak query API and
fully respect the access rights imposed on the different policies that contain
entries for a given principal. These principal-based policies can also be modified
using the corresponding methods provided by the access control, except for
`JackrabbitAccessControlList.orderBefore`.

Thus the default implementation corresponds to the default implementation present
with Jackrabbit 2.x. Note however, that the former principal-base approach that
stored policies per principal in a dedicated tree is no longer available.

#### Access Control Policies

The Oak access control management exposes two types of policies that cover all
use case defined by the specification and required by the default setup:

| Name            | Policy                        | Description                |
|-----------------|-------------------------------|----------------------------|
| Default ACL     | `JackrabbitAccessControlList` | access control on individual nodes |
| Repo-Level ACL  | `JackrabbitAccessControlList` | repo-level access control for the `null` path |
| Read Policy     | `NamedAccessControlPolicy`    | trees that are configured to be readable to everyone |
| | | |

##### Default ACL

The default access control lists are bound to individual nodes.
They may be used to grant/deny access for all operations that are in some way
related to JCR items: regular read/write, access control management, versioning,
locking and as of Oak 1.0 user management and writing index definitions.

These policies are designed to take effect on the complete subtree spanned by
the node they are bound to. The individual access control entries are evaluated
in strict order (first entries in a given list, second entries inherited from
list bound to parent nodes) with one notable exception: access control entries
created for non-group principals always take precedence irrespective of their
inheritance status.

Further details are described in section [Permissions](permission.html).

##### Repo-Level ACL

The access control lists bound to the `null` path can be used to grant/deny privileges
associated with operations on repository-level such as namespace, node type, privilege
and workspace management.

The effect of these entries is limited to the repository operations and is
no inherited to any items inside the repository.

##### Read Policy

These immutable policy has been introduced in Oak 1.0 in order to allow for opening
up trees that need to be readable to all sessions irrespective of other effective
policies.

By default these policies are bound to the following trees:

- `/jcr:system/rep:namespaces`: stores all registered namespaces
- `/jcr:system/jcr:nodeTypes`: stores all registered node types
- `/jcr:system/rep:privileges`: stores all registered privileges

The default set can be changed or extended by setting the corresponding configuration
option. However, it is important to note that many JCR API calls rely on the
accessibility of the namespace, nodetype and privilege information. Removing the
corresponding paths from the configuration will most probably have undesired effects.


#### Access Control Entries

The access control entries present in a given list are subject to the following
rules applied upon editing but not enforced by `CommitHook`s:

- *uniqueness*: a given entry may only appear onces in a list
- *merging*: if an entry exists for a given principal with the same allow-status and restrictions, the existing entry will be updated without being moved in the list.
- *redundancy*: if an new entry makes an existing entry (partially) redundant the existing entry will be updated or removed altogether.


#### Restrictions

Access control entries may be created by limiting their effect by adding
restrictions as mentioned by JSR 283. Details about the restriction management
in Oak 1.0 as well as a list of built-in restrictions and extensibility can be
found in section [Restriction Management](accesscontrol/restriction.html).


#### Representation in the Repository

All access control policies defined with an Oak repository are stores child of
the node they are bound to. The node type definition used to represent access control content:

    [rep:AccessControllable]
      mixin
      + rep:policy (rep:Policy) protected IGNORE

    [rep:RepoAccessControllable]
      mixin
      + rep:repoPolicy (rep:Policy) protected IGNORE

    [rep:Policy]
      abstract

    [rep:ACL] > rep:Policy
      orderable
      + * (rep:ACE) = rep:GrantACE protected IGNORE

    [rep:ACE]
      - rep:principalName (STRING) protected mandatory
      - rep:privileges (NAME) protected mandatory multiple
      - rep:nodePath (PATH) protected /* deprecated in favor of restrictions */
      - rep:glob (STRING) protected   /* deprecated in favor of restrictions */
      - * (UNDEFINED) protected       /* deprecated in favor of restrictions */
      + rep:restrictions (rep:Restrictions) = rep:Restrictions protected /* since oak 1.0 */

    [rep:GrantACE] > rep:ACE

    [rep:DenyACE] > rep:ACE

    /**
     * @since oak 1.0
     */
    [rep:Restrictions]
      - * (UNDEFINED) protected
      - * (UNDEFINED) protected multiple

##### Examples

###### Regular ACL at /content

    "": {
        "jcr:primaryType": "rep:root",
        "content": {
            "jcr:primaryType": "oak:Unstructured",
            "jcr:mixinTypes": "rep:AccessControllable",
            "rep:policy": {
                "jcr:primaryType": "rep:ACL",
                "allow": {
                    "jcr:primaryType": "rep:GrantACE",
                    "rep:principalName": "jackrabbit",
                    "rep:privileges": ["jcr:read", "rep:write"]
                },
                "deny": {
                    "jcr:primaryType": "rep:DenyACE",
                    "rep:principalName": "jackrabbit",
                    "rep:privileges": ["jcr:addNodes", "rep:addProperties"],
                    "rep:restrictions" {
                        "jcr:primaryType": "rep:Restrictions",
                        "rep:ntNames": ["nt:hierarchyNode", "nt:resource"]
                    }
                }
            }
        }
    }

###### Repo-Level Policy

    "": {
        "jcr:primaryType": "rep:root",
        "jcr:mixinTypes": "rep:RepoAccessControllable",
        "rep:repoPolicy": {
            "jcr:primaryType": "rep:ACL",
            "allow": {
                "jcr:primaryType": "rep:GrantACE",
                "rep:principalName": "elefant",
                "rep:privileges": ["rep:privilegeManagement"]
            }
        }
    }


#### XML Import

As of OAK 1.0 access control content can be imported both with Session and
Workspace import.

In addition the JCR XML import behavior has been extended to respect the
`o.a.j.oak.spi.xml.ImportBehavior` flags instead of just performing a best effort import.

Currently the `ImportBehavior` is only used to switch between different ways of
handling principals unknown to the repository. For consistency and in order to
match the validation requirements as specified by `AccessControlList#addAccessControlEntry`
the default behavior is ABORT (while in Jackrabbit 2.x the behavior always was BESTEFFORT).

The different `ImportBehavior` flags are implemented as follows:
- `ABORT`: throws an `AccessControlException` if the principal is unknown
- `IGNORE`: ignore the entry defining the unknown principal
- `BESTEFFORT`: import the access control entry with an unknown principal.

In order to get the same best effort behavior as present with Jackrabbit 2.x
the configuration parameters of the `AuthorizationConfiguration` must contain
the following entry:

    importBehavior = "besteffort"

See also ([OAK-1350](https://issues.apache.org/jira/browse/OAK-1350)))


### API Extensions

Oak defines the following interfaces extending the access control management API:

- `PolicyOwner`: Interface to improve pluggability of the access control management
   and allows to termine if a giving manager handles a given policy.
- `AccessControlConstants`: Constants related to access control management.

In addition it provides some access control related base classes in `org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol`
that may be used for a custom implementation:

- `AbstractAccessControlList`: abstract base implementation of the `JackrabbitAccessControlList` interface
    - `ImmutableACL`: immutable subclass of `AbstractAccessControlList`
    - `ACE`: abstract subclass that implements common methods of a mutable access control list.

#### Restriction Management

Oak 1.0 defines a dedicated restriction management API. See
[Restriction Management](accesscontrol/restriction.html) for details and further
information regarding extensibility and pluggability.

### Utilities

The jcr-commons module present with Jackrabbit provide some access control related
utilities that simplify the creation of new policies and entries such as for example:

- `AccessControlUtils.getAccessControlList(Session, String)`
- `AccessControlUtils.getAccessControlList(AccessControlManager, String)`
- `AccessControlUtils.addAccessControlEntry(Session, String, Principal, String[], boolean)`

See
[org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils] for
the complete list of methods.

##### Examples

    String path = node.getPath();
    JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, path);
    acl.addEntry(principal, privileges, true);
    acMgr.setPolicy(path, acl);
    session.save();

### Configuration

The configuration of the access control management implementation is handled
within the [AuthorizationConfiguration], which is used for all authorization
related matters. This class provides the following two access control related
methods:

- `getAccessControlManager`: get a new ac manager instance.
- `getRestrictionProvider`: get a new instance of the restriction provider.

#### Configuration Parameters

The default implementation supports the following configuration parameters:

| Parameter                    | Type                | Default                  |
|------------------------------|---------------------|--------------------------|
| `PARAM_RESTRICTION_PROVIDER` | RestrictionProvider | RestrictionProviderImpl  |
| `PARAM_READ_PATHS`           | Set\<String\>       | paths to namespace, nodetype and privilege root nodes  |
| `PARAM_IMPORT_BEHAVIOR`      | String ("abort", "ignore", "besteffort") | "abort" |
| | | |

Differences to Jackrabbit 2.x:

- The "omit-default-permission" configuration option present with the Jackrabbit's AccessControlProvider implementations is no longer supported with Oak.
- As of OAK no extra access control content is installed by default which renders that flag superfluous.

### Pluggability

There are multiple levels for plugging access control related custom implementations:

1. replace `AuthorizationConfiguration`: if you want to completely replace the way
   authorization is handled in the repository.  In OSGi-base setup this is achieved
   by making the configuration implementation a service. In a non-OSGi-base setup the
   custom configuration must be exposed by the `SecurityProvider` implementation.
2. extend `AuthorizationConfiguration`: it is planned to provide a `CompositeAuthorizationConfiguration`
   that allows to aggregate different authorization implementations (see [OAK-1268]).
3. extend the existing implementation by providing custom restrictions (see [RestrictionManagement](authorization/restriction.html).

### Further Reading

- [Differences wrt Jackrabbit 2.x](accesscontrol/differences.html)
- [Restriction Management](accesscontrol/restriction.html)

<!-- hidden references -->
[JSR 283]: http://www.day.com/specs/jcr/2.0/16_Access_Control_Management.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-commons/src/main/java/org/apache/jackrabbit/commons/jackrabbit/authorization/AccessControlUtils.java
[OAK-1268]: https://issues.apache.org/jira/browse/OAK-1268