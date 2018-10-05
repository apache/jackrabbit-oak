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

External Identity Management
--------------------------------------------------------------------------------

### General

Jackrabbit Oak provides interfaces and some base classes to ease custom implementation 
of the external authentication with optional user/group synchronization to the repository.

### Identity Management API

- [ExternalIdentityProviderManager]: factory for the `ExternalIdentityProvider`
- [ExternalIdentityProvider]: used to authenticate against the third party system. 
Additionally provides method to obtain information about external user/group 
accounts such as needed for the [synchronization](usersync.html) into the repository.
- [ExternalIdentity]: base interface for an external user/group
    - [ExternalUser]
    - [ExternalGroup]
- [ExternalIdentityRef]: reference to an external user/group consisting of id and provider name.
- [PrincipalNameResolver]: optimized lookup of principal name from [ExternalIdentityRef]; see section [Dynamic Membership](external/dynamic.html) and [OAK-5210] for details)

### Default Implementation

The `oak-auth-external` module come with a default implementation of the 
`ExternalIdentityProviderManager` and OSGi component that tracks all
external IDPs that are registered via OSGi. While OSGi setup is recommended
way, it can equally be used in non-OSGi environments by manually adding 
and removing the providers.

There exists not default implementation for the other interfaces related
to external identity management. However, Oak 1.0 provides support for third 
party authentication and identity management against LDAP covered by
a separate module `oak-auth-ldap`. This is covered by section [LDAP Integration](ldap.html).

### Pluggability

#### Custom External Identity Management

In order to plug a custom implementation of the external identity management
the following steps are required:

- Write your own implementation [ExternalIdentityProvider] including your implementations of the external identities. 
  _Note:_ If you are running Oak in an OSGi based setup, make sure the provider gets registered as OSGi service in which case it will be automatically tracked by the default [ExternalIdentityProviderManager].
- Deploy the bundle containing your implementation such that the IDP gets
  tracked by the [ExternalIdentityProviderManager]. In an non-OSGi environment
  you have to register it manually
- Link your identity provider to the `ExternalLoginModule` by configuring the IDP name accordingly (see section [Configuration](externalloginmodule.html#configuration))  

##### Examples

See [CustomExternalIdentityProvider] in the `oak-exercise` module for a
very simplistic implementation for an OSGi-based Oak setup.

#### Custom ExternalIdentityProviderManager

Since `oak-auth-external` provides a default [ExternalIdentityProviderManager] 
a custom identity management doesn't need provide a separate implementation 
of this interface. 

If you wish to provider your own [ExternalIdentityProviderManager] in an
OSGi environment, please make sure it gets properly referenced by the
`ExternalLoginModuleFactory`.

<!-- references -->
[ExternalIdentityProviderManager]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProviderManager.html
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[ExternalIdentity]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentity.html
[ExternalUser]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalUser.html
[ExternalGroup]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalGroup.html
[ExternalIdentityRef]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityRef.html
[CustomExternalIdentityProvider]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-exercise/src/main/java/org/apache/jackrabbit/oak/exercise/security/authentication/external/CustomExternalIdentityProvider.java
[PrincipalNameResolver]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/PrincipalNameResolver.html
[OAK-5210]: https://issues.apache.org/jira/browse/OAK-5210