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

Authentication : Implementation Details
--------------------------------------------------------------------------------

### General

Jackrabbit Oak covers different authentication requirements by providing default 
implementations and extension points for different setup scenarios.

### Differences wrt Jackrabbit 2.x

See the corresponding [documentation](differences.html).

### Authentication Requirements

Jackrabbit Oak covers the following login requirements and provides 
dedicated `LoginModule` implementation(s) for each scenario:

- [Guest Login](#guest)
- [UserId/Password Login](#uid_pw)
- [Impersonation Login](#impersonation)
- [Token Login](#token)
- [Pre-Authenticated Login](#pre_authenticated)
- [External Login](#external)

<a name="guest"/>
#### Guest Login

The proper way to obtain an guest session as of Oak is as specified by JSR 283:

    String wspName = null;
    Session anonymous = repository.login(new GuestCredentials(), wspName);

As of Oak 1.0 `Repository#login()` and `Repository#login(null, wspName)` is no
longer treated as guest login. This behavior of Jackrabbit-core is violating the
specification, which defines that null-login should be used for those cases where
the authentication process is handled outside of the repository (see [Pre-Authentication](preauthentication.html)).

Similarly, any special treatment that Jackrabbit core applied for the guest (anonymous)
user has been omitted altogether from the default [LoginModuleImpl]. In the default
setup the built-in anonymous user will be created without any password. Therefore
explicitly uid/pw login using the anonymous userId will no longer work. This behavior
is now consistent with the default login of any other user which doesn't have a
password set.

##### GuestLoginModule

The aim of the [GuestLoginModule] implementation is to provide backwards compatibility
with Jackrabbit 2.x with respect to the guest (anonymous) login: the `GuestLoginModule`
can be added as _optional_ entry to the chain of login modules in the JAAS (or
corresponding OSGi) configuration.

Example JAAS Configuration:

    jackrabbit.oak {
       org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule  optional;
       org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
    };


The behavior of the `GuestLoginModule` is as follows:

*Phase 1: Login*

- tries to retrieve JCR credentials from the [CallbackHandler] using the [CredentialsCallback]
- in case no credentials could be obtained it pushes a new instance of [GuestCredentials] to the shared stated
  and **returns** `true`
- otherwise it **returns** `false`

*Phase 2: Commit*

- if the phase 1 succeeded it will add the `GuestCredentials` created above and
  `EveryonePrincipal` the `Subject` in phase 2 of the login process and **returns** `true`
- otherwise it **returns** `false`

<a name="uid_pw"/>
#### UserId/Password Login

Oak 1.0 comes with 2 different login module implementations that can handle
[SimpleCredentials]:

- Default (`LoginModuleImpl`) as described below
- `ExternalLoginModule` as described in section [External Authentication](externalloginmodule.html)

##### LoginModuleImpl

The [LoginModuleImpl] defines a regular userId/password login and requires a
repository setup that supports [User Management](../user.html) and is designed to
supports the following `Credentials`:

- `SimpleCredentials`
- `GuestCredentials` (see above)
- `ImpersonationCredentials` (see below)

This login module implementations behaves as follows:

*Phase 1: Login*

* if a user does not exist in the repository (i.e. cannot be provided by the user manager) it **returns `false`**.
* if an authorizable with the respective userId exists but is a group or a disabled users, it **throws `LoginException`**
* if a user exists in the repository and the credentials don't match, it **throws `LoginException`**
* if a user exists in the repository and the credentials match, it **returns `true`**
    * also, it adds the credentials to the shared state
    * also, it adds the login name to the shared state
    * also, it calculates the principals and adds them to the private state
    * also, it adds the credentials to the private state

*Phase 2: Commit*

* if the private state contains the credentials and principals, it adds them (both) to the subject and **returns `true`**
* if the private state does not contain credentials and principals, it clears the state and **returns `false`**

<a name="user_authentication"/>
###### User Authentication

The `LoginModuleImpl` uses a configured `Authentication`-implementation for 
performing the login step. Which implementation to use is determined by the 
[UserAuthenticationFactory] obtained by the given `UserConfiguration`. It is
expected to provides an `Authentication` implementation if the given 
`UserConfiguration` is accepted.

In case multiple implementations of the `UserAuthenticationFactory` are available, 
the precedence depends on its OSGi service ranking property. The default factory 
implementation has a ranking of 0 (OSGi default). Services with the highest ranking 
will take precedence.

See also section [user management](../user/default.html#pluggability).

<a name="impersonation"/>
#### Impersonation Login

Another flavor of the Oak authentication implementation is covered by
`javax.jcr.Session#impersonate(Credentials)`, which allows to obtain an new
`Session` for a user identified by the specified credentials. As of JSR 333
this method can also be used in order to clone the existing session (i.e.
self-impersonation of the user that holds the session.

With Oak 1.0 impersonation is implemented as follows:

1. `Session#impersonate` takes any kind of `Credentials`
2. the specified credentials are wrapped in a new instance of [ImpersonationCredentials]
   along with the current `AuthInfo` object.
3. these `ImpersonationCredentials` are passed to `Repository.login`

Whether or not impersonation succeeds consequently both depends on the authentication
setup and on some implementation specific validation that make sure the
editing session is allowed to impersonate the user identified by the credentials
passed to the impersonate call.

With Oak 1.0 only the default login module ([LoginModuleImpl]) is able to deal
with `ImpersonationCredentials` and applies the following logic:

- **Self-Impersonation**: Any attempt to impersonate the same session will succeed
  as long as the user is still valid (i.e. exists and has not been disabled).
- **Regular Impersonation**: Impersonation another user will only succeed if
  the impersonated user is valid (i.e. exists and is not disabled) _and_ the
  the user associated with the editing session is allowed to impersonate this
  user. The latter depends on the [User Management](../user.html) implementation
  specifically on the return value of `User.getImpersonation().allows(Subject subject)`.

##### ImpersonationCredentials

Since the implementation of `Session.impersonate` no longer uses `SimpleCredentials`
to transport the original `Subject` but rather performs the login with dedicated
[ImpersonationCredentials], impersonation is no longer restricted to `SimpleCredentials`
being passed to `Session#impersonate` call. Instead the specified credentials are
passed to a new instance of `ImpersonationCredentials` delegating the evaluation
and validation of the specified `Credentials` to the configured login module(s).

This modification will not affect applications that used JCR API to impersonate
a given session. Note however that applications relying on the Jackrabbit
implementation and manually creating `SimpleCredentials` with a
`SecurityConstants.IMPERSONATOR_ATTRIBUTE`, would need to be refactor after
migration to Oak.

##### Impersonation with Custom Authentication Setup

Applications that wish to use a custom authentication setup need to ensure the
following steps in order to get JCR impersonation working:

- Respect `ImpersonationCredentials` in the authentication setup.
- Identify the impersonated from `ImpersonationCredentials.getBaseCredentials`
  and verify if it can be authenticated.
- Validate that the editing session is allowed to impersonate: The user associated
  with the editing session can be identified by the [AuthInfo] obtained from
  from `ImpersonationCredentials.getImpersonatorInfo()`.

<a name="token"/>
#### Token Login

See section [Token Authentication](tokenmanagement.html) for details
regarding token based authentication.

##### TokenLoginModule

The `TokenLoginModule` is in charge of creating new login tokens and validate
repository logins with `TokenCredentials`. The exact behavior of this login module is
described in section [Token Authentication](tokenmanagement.html).

<a name="pre_authenticated"/>
#### Pre-Authenticated Login

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation.

- Pre-Authentication combined with Login Module Chain
- Pre-Authentication without Repository Involvement (aka `null` login)

See section [Pre-Authentication Login](preauthentication.html) for
further details and examples.

<a name="external"/>
#### External Login

While the default setup in Oak is solely relying on repository functionality to
ensure proper authentication it quite common to authenticate against different
systems (e.g. LDAP). For those setups that wish to combine initial authentication
against a third party system with repository functionality, Oak provides a default
implementation with extension points:

- [External Authentication](externalloginmodule.html): Summary of
  the external authentication and details about the `ExternalLoginModule`.
- [User and Group Synchronization](usersync.html): Details regarding
  user and group synchronization as well as a list of configuration options provided
  by the the default implementations present with Oak.
- [Identity Management](identitymanagement.html): Further information regarding extenal identity management.
- [LDAP Integration](ldap.html): How to make use of the `ExternalLoginModule`
  with the LDAP identity provider implementation. This combination is aimed to replace
  [com.day.crx.security.ldap.LDAPLoginModule], which relies on Jackrabbit internals
  and will no longer work with Oak.

##### ExternalLoginModule

The [ExternalLoginModule] is a base implementation that allows easy integration
of 3rd party authentication and identity systems, such as [LDAP](ldap.html). The
general mode of the external login module is to use the external system as authentication
source and as a provider for users and groups that may also be synchronized into
the repository.

This login module implementation requires an valid `SyncHandler` and `IdentityProvider`
to be present. The detailed behavior of the `ExternalLoginModule` is described in
section [External Authentication](externalloginmodule.html).

<!-- hidden references -->
[GuestCredentials]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/GuestCredentials.html
[SimpleCredentials]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/SimpleCredentials.html
[ImpersonationCredentials]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/ImpersonationCredentials.html
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[GuestLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/GuestLoginModule.html
[LoginModuleImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/user/LoginModuleImpl.html
[com.day.crx.security.ldap.LDAPLoginModule]: http://dev.day.com/docs/en/crx/current/administering/ldap_authentication.html
[AbstractLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AbstractLoginModule.html
[UserAuthenticationFactory]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserAuthenticationFactory.html