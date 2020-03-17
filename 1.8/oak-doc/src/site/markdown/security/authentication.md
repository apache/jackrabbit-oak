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

<a name="jcr_api"/>
### JCR API

Within the scope of JCR `Repository.login` is used to authenticate a given user.
This method either takes a `Credentials` argument if the validation is performed
by the repository itself or `null` in case the user has be pre-authenticated by
an external system.

Furthermore JCR defines two types of `Credentials` implementations:

- [javax.jcr.GuestCredentials]: used to obtain a "guest", "public" or "anonymous" session.
- [javax.jcr.SimpleCredentials]: used to login a user with a userId and password.

The following variants exist for the repository login itself:

- `Repository.login()`: equivalent to passing `null` credentials and the default workspace name.
- `Repository.login(Credentials credentials)`: login with credentials to the default workspace.
- `Repository.login(String workspace)`: login with `null` credentials to the workspace with the specified name.
- `Repository.login(Credentials credentials, String workspaceName)`
- `JackrabbitRepository.login(Credentials credentials, String workspaceName, Map<String, Object> attributes)`:
  in addition allows to pass implementation specific session attributes.

See [javax.jcr.Repository] and [org.apache.jackrabbit.api.JackrabbitRepository]
for further details.

In addition JCR defines `Session.impersonate(Credentials)` to impersonate another
user or - as of JSR 333 -  clone an existing session.

<a name="oak_api"/>
### Oak API

The Oak API contains the following authentication related methods and interfaces

- [AuthInfo]: Immutable object created upon successful login providing information about the authenticated `Subject.`
- `ContentRepository.login(Credentials, String)`: The Oak counterpart of the JCR login.
- `ContentSession.getAuthInfo()`: exposes the `AuthInfo` associated with the `ContentSession`.

<a name="api_extensions"/>
### API Extension

#### Oak Authentication

In the the package `org.apache.jackrabbit.oak.spi.security.authentication` Oak 1.0
defines some extensions points that allow for further customization of the authentication.

- `LoginContextProvider`: Configurable provider of the `LoginContext` (see below)
- `LoginContext`: Interface version of the JAAS LoginContext aimed to ease integration with non-JAAS components
- `Authentication`: Aimed to validate credentials during the first phase of the (JAAS) login process.

In addition this package contains various utilities and base implementations.
Most notably an abstract login module implementation ([AbstractLoginModule]) as
described below and a default implementation of the AuthInfo interface ([AuthInfoImpl]).

##### Abstract Login Module

This package also contains a abstract `LoginModule` implementation ([AbstractLoginModule])
providing common functionality. In particular it contains Oak specific methods that allow
subclasses to retrieve the `SecurityProvider`, a `Root` and accesss to various
security related interfaces (e.g. `PrincipalManager`).

Subclasses are required to implement the following methods:

- `getSupportedCredentials()`: return a set of supported credential classes. See also section [Supported Credentials](#supported_credentials)
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

<a name="supported_credentials"/>
#### Supported Credentials

Since Oak 1.5.1 the extensions additionally contain a dedicated interface that
eases the support for different `Credentials` in the package space 
`org.apache.jackrabbit.oak.spi.security.authentication.credentials`:
                                                   
- [CredentialsSupport]: Interface definition exposing the set of supported `Credentials` classes and some common utility methods.
- [SimpleCredentialsSupport]: Default implementation for the widely used `SimpleCredentials`

<a name="default_implementation"/>
### Oak Authentication Implementation

A description of the various requirements covered by Oak by default as well
as the characteristics of the corresponding implementations can be found in
section [Authentication: Implementation Details](authentication/default.html).

See section [differences](authentication/differences.html) for comprehensive list 
of differences wrt authentication between Jackrabbit 2.x and Oak.

<a name="configuration"/>
### Configuration

The configuration of the authentication setup is defined by the [AuthenticationConfiguration].
This interface provides the following method:

- `getLoginContextProvider()`: provides the login contexts for the desired authentication mechanism.

#### JAAS Configuration Utilities
There also exists a utility class that allows to obtain different
`javax.security.auth.login.Configuration` for the most common setup [11]:

- `ConfigurationUtil#getDefaultConfiguration`: default OAK configuration supporting uid/pw login configures `LoginModuleImpl` only
- `ConfigurationUtil#getJackrabbit2Configuration`: backwards compatible configuration that provides the functionality covered by jackrabbit-core DefaultLoginModule, namely:
    - `GuestLoginModule`: null login falls back to anonymous
    - `TokenLoginModule`: covers token based authentication
    - `LoginModuleImpl`: covering regular uid/pw login

<a name="pluggability"/>
### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
implementation on various levels:

1. The complete authentication setup can be changed by plugging a different
   `AuthenticationConfiguration` implementations. In OSGi-base setup this is
   achieved by making the configuration a service. In a non-OSGi-base setup the
   custom configuration must be exposed by the `SecurityProvider` implementation.
2. Within the default authentication setup you replace or extend the set of
   login modules and their individual settings. In an OSGi-base setup is achieved
   by making the modules accessible to the framework and setting their execution
   order accordingly. In a Non-OSGi setup this is specified in the [JAAS config].

<a name="further_reading"/>
### Further Reading

- [Authentication: Implementation Details](authentication/default.html)
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
[AuthInfoImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthInfoImpl.html
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[AbstractLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AbstractLoginModule.html
[AuthenticationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthenticationConfiguration.html
[JAAS config]: http://docs.oracle.com/javase/6/docs/technotes/guides/security/jaas/JAASRefGuide.html
[CredentialsSupport]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/credentials/CredentialsSupport.html
[SimpleCredentialsSupport]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/credentials/SimpleCredentialsSupport.html
