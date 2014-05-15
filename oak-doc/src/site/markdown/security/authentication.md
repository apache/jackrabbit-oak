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

Authentication
--------------------------------------------------------------------------------

### JAAS Authentication and Login Modules

#### General Concepts

In order to understand how login modules work and how Oak can help providing extension points we need to look at how
JAAS authentication works in general and discuss where the actual credential-verification is performed.

##### Brief recap of the JAAS authentication
The following section is copied and adapted from the javadoc of [javax.security.auth.spi.LoginModule].
The authentication process within the `LoginModule` proceeds in two distinct phases,
login and commit phase:

_Phase 1: Login_

1. In the first phase, the `LoginModule`'s `login` method gets invoked by the `LoginContext`'s `login` method.
2. The `login` method for the `LoginModule` then performs the actual authentication (prompt for and verify a
   password for example) and saves its authentication status as private state information.
3. Once finished, the `LoginModule`'s login method either returns `true` (if it succeeded) or `false` (if it should
   be ignored), or throws a `LoginException` to specify a failure. In the failure case, the `LoginModule` must not
   retry the authentication or introduce delays. The responsibility of such tasks belongs to the application.
   If the application attempts to retry the authentication, the `LoginModule`'s `login` method will be called again.

_Phase 2: Commit_

1. In the second phase, if the `LoginContext`'s overall authentication succeeded (the relevant REQUIRED, REQUISITE,
   SUFFICIENT and OPTIONAL LoginModules succeeded), then the `commit` method for the `LoginModule` gets invoked.
2. The `commit` method for a `LoginModule` checks its privately saved state to see if its own authentication
   succeeded.
3. If the overall `LoginContext` authentication succeeded and the `LoginModule`'s own authentication succeeded, then
   the `commit` method associates the relevant Principals (authenticated identities) and Credentials (authentication
   data such as cryptographic keys) with the Subject located within the `LoginModule`.
4. If the `LoginContext`'s overall authentication failed (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL
   LoginModules did not succeed), then the `abort` method for each `LoginModule` gets invoked. In this case, the
   `LoginModule` removes/destroys any authentication state originally saved.
      
##### Login module execution order
Very simply put, all the login modules that participate in JAAS authentication are configured in a list and can have
flags indicating how to treat their behaviors on the `login()` calls.

JAAS defines the following module flags:  
(The following section is copied and adapted from the javadoc of [javax.security.auth.login.Configuration])

- **Required**:  The LoginModule is required to succeed. If it succeeds or fails,
  authentication still continues to proceed down the LoginModule list.
- **Requisite**: The LoginModule is required to succeed. If it succeeds, authentication
  continues down the LoginModule list. If it fails, control immediately returns
  to the application (authentication does not proceed down the LoginModule list).
- **Sufficient**: The LoginModule is not required to succeed. If it does succeed,
  control immediately returns to the application (authentication does not proceed
  down the LoginModule list). If it fails, authentication continues down the LoginModule list.
- **Optional**: The LoginModule is not required to succeed. If it succeeds or
  fails, authentication still continues to proceed down the LoginModule list.
 
The overall authentication succeeds **only** if **all** Required and Requisite LoginModules succeed. If a Sufficient 
LoginModule is configured and succeeds, then only the Required and Requisite LoginModules prior to that Sufficient 
LoginModule need to have succeeded for the overall authentication to succeed. If no Required or Requisite LoginModules 
are configured for an application, then at least one Sufficient or Optional LoginModule must succeed.

### JCR Authentication

Within the scope of JCR `Repository.login` is used to authenticate a given user.
This method either takes a `Credentials` argument if the validation is performed
by the repository itself or `null` in case the user has be pre-authenticated by
an external system.

Furthermore JCR defines two types of `Credentials` implementations:

- [javax.jcr.GuestCredentials]: used to obtain a "guest", "public" or "anonymous" session.
- [javax.jcr.SimpleCredentials]: used to login a user with a userId and password.

The following variants exist for the repository login itself:

- `Repository.login()`: equivalent to passing `null` credentials and the default workspace name.
- `Repository.login(Credentials credentials): login with credentials to the default workspace.
- `Repository.login(String workspace): login with `null` credentials to the workspace with the specified name.
- `Repository.login(Credentials credentials, String workspaceName`)
- `JackrabbitRepository.login(Credentials credentials, String workspaceName, Map<String, Object> attributes)`:
  in addition allows to pass implementation specific session attributes.

See [javax.jcr.Repository] and [org.apache.jackrabbit.api.JackrabbitRepository]
for further details.

In addition JCR defines `Session.impersonate(Credentials)` to impersonate another
user or - as of JSR 333 -  clone an existing session.


### Oak Authentication

#### General Notes

_todo_

#### Oak API

_todo_

- ContentRepository.login
- AuthInfo
- ContentSession.getAuthInfo


#### Differences wrt Jackrabbit 2.x

See section [differences](authentication/differences.html) for complete list of
differences wrt authentication between Jackrabbit 2.x and Oak.

#### Guest Login

The proper way to obtain an guest session as of Oak is as specified by JSR 283:

    String wspName = null;
    Session anonymous = repository.login(new GuestCredentials(), wspName);

As of Oak 1.0 `Repository#login()` and `Repository#login(null, wspName)` is no
longer treated as guest login. This behavior of Jackrabbit-core is violating the
specification, which defines that null-login should be used for those cases where
the authentication process is handled outside of the repository (see [Pre-Authentication](authentication/preauthentication.html)).

Similarly, any special treatment that Jackrabbit core applied for the guest (anonymous)
user has been omitted altogether from the default [LoginModuleImpl]. In the default
setup the built-in anonymous user will be created without any password. Therefore
explicitly uid/pw login using the anonymous userId will no longer work. This behavior
is now consistent with the default login of any other user which doesn't have a
password set.

##### Guest Login Module

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

#### UserId/Password Login

Oak 1.0 comes with 2 different login module implementations that can handle
`SimpleCredentials`:

- Default (`LoginModuleImpl`) as described below
- `ExternalLoginModule` as described in section [External Authentication](authentication/externalloginmodule.html)

##### Default Login Module

The [LoginModuleImpl] defines a regular userId/password login and requires a
repository setup that supports [User Management](user.html) and is designed to
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

#### Impersonation

Another flavor of the Oak authentication implementation is covered by
`javax.jcr.Session#impersonate(Credentials)`, which allows to obtain an new
`Session` for user identitified by the specified credentials. As of JSR 333
this method can also be used in order to clone the existing session (i.e.
self-impersonation of the user that holds the session.

With Oak 1.0 impersonation is implemented as follows:

1. `Session#impersonate` takes any kind of `Credentials`
2. the specified credentials are wrapped in a new instance of [ImpersonationCredentials]
   along with the current [AuthInfo] object.
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
  user. The latter depends on the [User Management](user.html) implementation
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


#### Token Login

See section [Token Authentication](authentication/tokenmanagement.html) for details
regarding token based authentication.

##### Token Login Module

The `TokenLoginModule` is in charge of creating new login tokens and validate
repository logins with `TokenCredentials`. The exact behavior of this login module is
described in section [Token Authentication](authentication/tokenmanagement.html).


#### Pre-Authenticated Login

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation.

- Pre-Authentication combined with Login Module Chain
- Pre-Authentication without Repository Involvement (aka `null` login)

See section [Pre-Authentication Login](authentication/preauthentication.html) for
further details and examples.

#### External Login

While the default setup in Oak is solely relying on repository functionality to
ensure proper authentication it quite common to authenticate against different
systems (e.g. LDAP). For those setups that wish to combine initial authentication
against a third party system with repository functionality, Oak provides a default
implementation with extension points:

- [External Authentication](authentication/externalloginmodule.html): Summary of
  the external authentication and details about the `ExternalLoginModule`.
- [User and Group Synchronization](authentication/usersync.html): Details regarding
  user and group synchronization as well as a list of configuration options provided
  by the the default implementations present with Oak.
- [Identity Management](authentication/identitymanagement.html): Further information regarding extenal identity management.
- [LDAP Integration](authentication/ldap.html): How to make use of the `ExternalLoginModule`
  with the LDAP identity provider implementation. This combination is aimed to replace
  [com.day.crx.security.ldap.LDAPLoginModule], which relies on Jackrabbit internals
  and will no longer work with Oak.

##### External Login Module

The external login module is a base implementation that allows easy integration
of 3rd party authentication and identity systems, such as [LDAP](ldap.html). The
general mode of the external login module is to use the external system as authentication
source and as a provider for users and groups that may also be synchronized into
the repository.

This login module implementation requires an valid `SyncHandler` and `IdentityProvider`
to be present. The detailed behavior of the `ExternalLoginModule` is described in
section [External Authentication](authentication/externalloginmodule.html).


### API Extension

#### Oak Authentication

In the the package `org.apache.jackrabbit.oak.spi.security.authentication` Oak 1.0
defines some extensions points that allow for further customization of the authentication.

- `LoginContextProvider`: Configurable provider of the `LoginContext` (see below)
- `LoginContext`: Interface version of the JAAS LoginContext aimed to ease integration with non-JAAS components
- `Authentication`: Aimed to validate credentials during the first phase of the (JAAS) login process.
_todo_

##### Abstract Login Module

This package also contains a abstract `LoginModule` implementation ([AbstractLoginModule])
providing common functionality. In particular it contains Oak specific methods that allow
subclasses to retrieve the `SecurityProvider`, a `Root` and accesss to various
security related interfaces (e.g. `PrincipalManager`).

Subclasses are required to implement the following methods:

- `getSupportedCredentials(): return a set of supported credential classes.
- `login()`: The login method defined by `LoginModule`
- `commit()`: The commit method defined by `LoginModule`

###### Example: Extending AbstractLoginModule

    public class TestLoginModule extends AbstractLoginModule {

        private Credentials credentials;
        private String userId;
        private Set<? extends Principal> principals;

        @Nonnull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return ImmutableSet.of(TestCredentials.class);
        }

        @Override
        public boolean login() throws LoginException {
            credentials = getCredentials();
            if (validCredentials(credentials)) {
                this.credentials = credentials;
                this.userId = getUserId(credentials);
                this.principals = getPrincipals(userId);
                return true;
            }
            return false;
        }

        @Override
        public boolean commit() throws LoginException {
            if (credentials != null) {
                if (!subject.isReadOnly()) {
                    subject.getPublicCredentials().add(credentials);
                    if (principals != null) {
                        subject.getPrincipals().addAll(principals);
                    }
                    AuthInfo authInfo = new AuthInfoImpl(userId, Collections.EMPTY_MAP, principals);
                    setAuthInfo(authInfo, subject);
                }
                return true;
            }
            return false;
        }
    }




#### Token Management

See section [token management](authentication/tokenmanagement.html) for details.

- `TokenConfiguration`: Interface to obtain a `TokenProvider` instance.
- `TokenProvider`: Interface to manage login tokens.
- `TokenInfo`: Information related to a login token and token validity.

#### User and Group Synchronization

_todo_ [Synchronization](authentication/usersync.html)

#### External Identity Management

Oak in addition provides interfaces to ease custom implementation of the external
authentication with optional user/group synchronization to the repository.
See section [identity management](authentication/identitymanagement.html) for details.

### Configuration

- [AuthenticationConfiguration]: _todo_ `getLoginContextProvider` -> configuration of the login context
- [TokenConfiguration]: `getTokenProvider`. See section [Token Management](tokenmanagement.html) for details.

#### JAAS Configuration Utilities
There also exists a utility class that allows to obtain different
`javax.security.auth.login.Configuration` for the most common setup [11]:

- `ConfigurationUtil#getDefaultConfiguration`: default OAK configuration supporting uid/pw login configures `LoginModuleImpl` only
- `ConfigurationUtil#getJackrabbit2Configuration`: backwards compatible configuration that provides the functionality covered by jackrabbit-core DefaultLoginModule, namely:
    - `GuestLoginModule`: null login falls back to anonymous
    - `TokenLoginModule`: covers token base authentication
    - `LoginModuleImpl`: covering regular uid/pw login

### Pluggability

_todo_

### Further Reading

- [Differences wrt Jackrabbit 2.x](authentication/differences.html)
- [Token Authentication and Token Management](authentication/tokenmanagement.html)
- [External Authentication](authentication/externalloginmodule.html)
    - [User and Group Synchronization](authentication/usersync.html)
    - [Identity Management](authentication/identitymanagement.html)
    - [LDAP Integration](authentication/ldap.html)
- [Pre-Authentication](authentication/preauthentication.html)

<!-- references -->
[javax.security.auth.spi.LoginModule]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/spi/LoginModule.html
[javax.security.auth.login.Configuration]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/login/Configuration.html
[javax.jcr.GuestCredentials]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/GuestCredentials.html
[javax.jcr.SimpleCredentials]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/SimpleCredentials.html
[javax.jcr.Repository]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Repository.html
[org.apache.jackrabbit.api.JackrabbitRepository]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/JackrabbitRepository.java
[ImpersonationCredentials]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/ImpersonationCredentials.html
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthInfo.html
[GuestLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/GuestLoginModule.html
[LoginModuleImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/user/LoginModuleImpl.html
[com.day.crx.security.ldap.LDAPLoginModule]: http://dev.day.com/docs/en/crx/current/administering/ldap_authentication.html
[AuthenticationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthenticationConfiguration.html
[AbstractLoginModoule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AbstractLoginModule.html