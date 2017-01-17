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

Implementations of the PrincipalProvider Interface
--------------------------------------------------------------------------------

Oak contains by default the following implementations of the `PrincipalProvider`
interface:

### [PrincipalProviderImpl]

This is the default implementation of the `PrincipalProvider`, which makes use
of the fact that `Authorizable`s as defined by the Jackrabbit user management
API are always paired with a `Principal`.

The implementation is not tied to a particular user management implementation
and doesn't need to be rewritten if the security setup would be configured with
different implementation of `UserConfiguration`.

### [UserPrincipalProvider]

The introduction of the optional `UserConfiguration.getUserPrincipalProvider`
extension (since Oak 1.3.4) allows for an optimized variant of the default 
principal provider, which is reading principal information from user and group 
accounts using generic user management API. _Note:_ every single user/group 
exposes a `Principal` instance through the call `Authorizable.getPrincipal()`

This extension allows for a provider based on implementation details of the user
management implementation it is associated with, offering the ability to optimize
the principal lookup and the associated overhead in terms of user/group object creation.

While the implementation is located along with the user management implementation
present in Oak this provider implementation should not be considered being
part of the user management API implementation.

Another benefit of this optimized implementation is the ability to specifically
cache the results of the principal resolution in order to improve login performance.
See section [Caching Results of Principal Resolution](cache.html) for further details.

### [CompositePrincipalProvider]

This implementation is a simple wrapper implementation that combines different
principals from different source providers. It is used in
`CompositePrincipalConfiguration` held by the default `SecurityProvider` to
collect all configured/plugged principal configurations i.e. the various
implementations of principal management.

Custom `PrincipalProvider` implementations may be used to combine principals from
different source i.e. detaching principal management from the user management,
where principals are backed by an existing user/group account.

### [ExternalGroupPrincipalProvider]

Implementation of the `PrincipalProvider` interface that exposes _external_ principals 
of type `java.security.acl.Group`. _External_ refers to the fact that these
principals are defined and managed by an external identity provider in contrast to
the default implementation that represents principals native to the repository.
This implies that the principals known and exposed by this provider implementation
does not expect principals to be backed by an authorizable group. As such they
can only be retrieved using Jackrabbit Principal Management API but not with 
User Management calls.

For performance reasons the `ExternalGroupPrincipalProvider` doesn't lookup 
principals on the IDP but relies data persisted inside the repository where
the names of these external principals are synchronized based on a configurable
expiration time.

See section [User and Group Synchronization : The Default Implementation](../authentication/external/defaultusersync.html)
for additional details.

Since Oak 1.5.3

<!-- references -->
[PrincipalProviderImpl]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/principal/PrincipalProviderImpl.java
[CompositePrincipalProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/CompositePrincipalProvider.html
[UserPrincipalProvider]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/user/UserPrincipalProvider.java
[ExternalGroupPrincipalProvider]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-auth-external/src/main/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/principal/ExternalGroupPrincipalProvider.java
