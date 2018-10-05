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

Access Control Management
--------------------------------------------------------------------------------

### General

This section covers fundamental concepts of the access control related APIs provided 
by JCR and Jackrabbit as well as the extensions points defined by Oak. 

If you are already familiar with the API and looking for examples you may directly
read [Using the Access Control Management API](accesscontrol/editing.html) for
a comprehensive list of method calls as well as examples that may be used to
edit the access control content of the repository.

<a name="jcr_api"/>
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

<a name="jackrabbit_api"/>
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

<a name="api_extensions"/>
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
[Restriction Management](authorization/restriction.html) for details and further
information regarding extensibility and pluggability.

<a name="utilities"/>
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

<a name="default_implementation"/>
### Characteristics of the Default Implementation

The behavior of the default access control implementation is described in sections 
[Access Control Management: The Default Implementation](accesscontrol/default.html)  
and [Restriction Management](authorization/restriction.html).

<a name="configuration"/>
### Configuration

The configuration of the access control management implementation is handled
within the [AuthorizationConfiguration], which is used for all authorization
related matters. This class provides the following two access control related
methods:

- `getAccessControlManager`: get a new ac manager instance.
- `getRestrictionProvider`: get a new instance of the restriction provider.

#### Configuration Parameters

The supported configuration options of the default implementation are described in the corresponding [section](accesscontrol/default.html#configuration).

<a name="further_reading"/>
### Further Reading

- [Differences wrt Jackrabbit 2.x](accesscontrol/differences.html)
- [Access Control Management: The Default Implementation](accesscontrol/default.html)
- [Using the Access Control Management API](accesscontrol/editing.html)
- [Restriction Management](authorization/restriction.html)

<!-- hidden references -->
[JSR 283]: http://www.day.com/specs/jcr/2.0/16_Access_Control_Management.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-commons/src/main/java/org/apache/jackrabbit/commons/jackrabbit/authorization/AccessControlUtils.java
[OAK-1268]: https://issues.apache.org/jira/browse/OAK-1268