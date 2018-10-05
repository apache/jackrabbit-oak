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

User Management
--------------------------------------------------------------------------------

<a name="jcr_api"/>
### JCR API

JCR itself doesn't come with a dedicated user management API. The only method
related and ultimately used for user management tasks is `Session.getUserID()`.
Therefore an API for user and group management has been defined as part of the
extensions present with Jackrabbit API.

<a name="jackrabbit_api"/>
### Jackrabbit API

The Jackrabbit API provides the user management related extensions that are
missing in JCR. The relevant interfaces are defined in the
`org.apache.jackrabbit.api.security.user' package space:

- `UserManager`
- `Authorizable`
    - `User`
    - `Group`
- `Impersonation`
- `QueryBuilder`
    - `Query`

<a name="api_extensions"/>
### API Extensions

The Oak project introduces the following user management related public
interfaces and classes:

- `AuthorizableType`: ease handling with the different authorizable types.
- `AuthorizableAction` and `AuthorizableActionProvider`: see [Authorizable Actions](user/authorizableaction.html) for details.
- `AuthorizableNodeName`: see section  [Authorizable Node Name Generation](user/authorizablenodename.html).
- `GroupAction` (via `AuthorizableActionProvider`): see [Group Actions](user/groupaction.html) for details.
- `UserAuthenticationFactory`: see sections [pluggability](user/default.html#pluggability) 
and [user authentication](authentication/default.html#user_authentication) for additional details.

<a name="utilities"/>
### Utilities

`org.apache.jackrabbit.oak.spi.security.user.*`

- `UserConstants` : Constants (NOTE: OAK names/paths)
- `UserIdCredentials` : Simple credentials implementation that might be used for `User.getCredentials' without exposing pw information. 

`org.apache.jackrabbit.oak.spi.security.user.util.*`

- `PasswordUtil` : Utilities for password generation. This utility corresponds
  to the internal jackrabbit utility.
  As of OAK it also supports Password-Based Key Derivation Function 2 (PBKDF2)
  function for password generation.
- `UserUtil` : Utilities related to general user management tasks.

<a name="default_implementation"/>
### Oak User Management Implementation

The behavior of the default user management implementation is described in section 
[User Management: The Default Implementation](user/default.html).

<a name="configuration"/>
### Configuration

The Oak user management comes with a dedicated entry point called [UserConfiguration]. 
This class is responsible for passing configuration options to the implementation
and provides the following two methods:

- `getUserManager(Root, NamePathMapper)`: get a new `UserManager` instance
- `getUserPrincipalProvider(Root, NamePathMapper)`: optional method that allows for optimized principal look-up from user/group accounts (since Oak 1.3.4).

#### Configuration Parameters

The supported configuration options of the default implementation are described in the corresponding [section](user/default.html#configuration).

<a name="pluggability"/>
### Pluggability

The default security setup as present with Oak 1.0 is able to have the default
user management implementation replaced as follows:

The complete user management implementation can be changed by plugging a different
`UserConfiguration` implementations. In OSGi-base setup this is achieved by making
the configuration a service which must take precedence over the default. 
In a non-OSGi-base setup the custom configuration must be exposed by the 
`SecurityProvider` implementation.

Alternatively the default user management implementation can be extended and
adjusted using various means. See the corresponding [section](user/default.html#pluggability)
for further details.

<a name="further_reading"/>
### Further Reading

- [Differences wrt Jackrabbit 2.x](user/differences.html)
- [User Management : The Default Implementation](user/default.html)
    - [Group Membership](user/membership.html)
    - [Authorizable Actions](user/authorizableaction.html)
    - [Authorizable Node Name](user/authorizablenodename.html)
    - [Password Expiry and Force Initial Password Change](user/expiry.html)
    - [Password History](user/history.html)
- [Searching Users and Groups](user/query.html)

<!-- hidden references -->
[UserConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserConfiguration.html
