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
### Authentication : Differences wrt Jackrabbit 2.x

#### 1. Characteristics of the Default Implementation

##### Null Login

As of Oak 1.0 `Repository#login()` and `Repository#login(null, wspName)` is no
longer treated as guest login. This behavior of Jackrabbit-core is violating the
specification, which defines that null-login should be used for those cases where
the authentication process is handled outside of the repository (-> see pre-authentication below).

In order to get a full backwards compatible behavior OAK provides a specific
`GuestLoginModule` [0] that can be added to the JAAS (or corresponding OSGI) configuration.

Example JAAS Configuration:

    jackrabbit.oak {
       org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule  optional;
       org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
    };


##### Guest Login

With respect to guest login (aka anonymous login) the OAK content repository
out of the box contains the following modifications:

- null login != guest login
- no anonymous login with uid/pw

As explained in 1) the null login will not longer fall back to a guest login
unless explicitly configured (-> `GuestLoginModule`).
The proper way to obtain an guest session as of OAK is as specified by JSR 283:

    String wspName = null;
    Session anonymous = repository.login(new GuestCredentials(), wspName);

Similarly, the special treatment that jackrabbit core applied for
the guest (anonymous) user has been omitted altogether in OAK. In the
default setup the anonymous user will created without any password. Therefore
explicitly uid/pw login using the anonymous userId will no longer work.
This behavior is now consistent with the default login of any other user
which doesn't have a password set.


##### Pre-Authentication

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation. See the corresponding section [Pre-Authentication](security/authentication/preauthentication.html)
for details and examples.

- Pre-Authentication combined with Login Module Chain
- Pre-Authentication without Repository Involvement: the `Subject` must be available
  with the current `java.security.AccessControlContext`.

#### 2. Impersonation

##### Self-Impersonation (aka Cloning a Session)

As of OAK 1.0 the latest changes made to JSR 333 with respect to `Session#impersonate`
have been adopted [3]: Any attempt to impersonate the same session (self-impersonation)
will succeed as long as the user is still valid.

##### Impersonation Credentials

The OAK implementation of `Session#impersonate` no longer uses `SimpleCredentials`
to transport the original `Subject` but rather performs the login with dedicated
`ImpersonationCredentials` [4].

With this change the impersonation feature no longer relies on `SimpleCredentials`
being passed to `Session#impersonate` call. Instead the specified credentials are
passed to a new instance of `ImpersonationCredentials` delegating the evaluation
and validation of the specified `Credentials` to the configured login module(s).

This modification will not affect applications that used JCR API to impersonate
a given session. However the following example which 'manually' builds impersonation
credentials the way jackrabbit core was handling it will no longer work to
impersonate an existing session:

     SessionImpl sImpl = (SessionImpl) mySession;
     SimpleCredentials jrImpCreds = new SimpleCredentials("someUserId, new char[0]);
     creds.setAttribute(SecurityConstants.IMPERSONATOR_ATTRIBUTE, sImpl.getSubject());
     Session impersonated = sImpl.getRepository().login(jrImpCreds, sImpl.getWorkspace().getName());

#### 3. Token based Authentication

The token based authentication has been completely refactor in OAK.

- Dedicated API for managing login tokens [5]
- Pluggable configuration of the new token management API
- Complete separation of token based authentication from regular
  uid/pw authentication into a separate `LoginModule` [6]

The default implementation differs from jackrabbit as follows
- token node is referenceable with a dedicated node type (rep:Token)
- expiration and key properties are mandatory and protected
- expiration time is obtained from `PARAM_TOKEN_EXPIRATION` specified in the login
attributes and falls back to the same configuration parameter.

The definition of the new built-in node type "rep:Token":

    [rep:Token] > mix:referenceable
    - rep:token.key (STRING) protected mandatory
    - rep:token.exp (DATE) protected mandatory
    - * (UNDEFINED) protected
    - * (UNDEFINED) multiple protected

Please note the following difference with respect to Jackrabbit core:
- the `TokenLoginModule` is responsible for creating new login tokens. Other login modules should not attempt to do so.
- token characteristics such as expiration time only need to be configured with the `TokenLoginModule`
- Other `LoginModule` implementations consequently no longer need to have the same config options set.


#### 4. External Authentication

While the default setup in OAK is solely relying on repository functionality to
ensure proper authentication it quite common to authenticate against different
systems (e.g. LDAP). For those setups that wish to combine initial authentication
against a third party system with repository functionality, OAK provides some
basic implementation and extension points [7] and ship an example setup for LDAP authentication.

This is aimed to become the replacement for `com.day.crx.security.ldap.LDAPLoginModule` [8],
which relies on jackrabbit internals and will no longer work with OAK.

#### 5. API Extensions

The OAK project introduces the following authenticated related service provider interfaces:

org.apache.jackrabbit.oak.spi.security.authentication:

- `LoginContextProvider`: Configurable provider of the `LoginContext` (see below)
- `LoginContext`: Interface version of the JAAS LoginContext aimed to ease integration with non-JAAS components
- `Authentication`: Aimed to validate credentials during the first phase of the (JAAS) login process.

org.apache.jackrabbit.oak.spi.security.authentication.token:

- `TokenConfiguration`: Interface to obtain a `TokenProvider` instance.
- `TokenProvider`: Interface to manage login tokens.
- `TokenInfo`: Information related to a login token and token validity.

org.apache.jackrabbit.oak.spi.security.authentication.external:

- interfaces to ease custom implementation of the external authentication with
  optional user/group synchronization to the repository (see [7]).

#### 6. Configuration

##### AuthenticationConfiguration [9]:
- `getLoginContextProvider` -> configuration of the login context

##### TokenConfiguration [10]:
- `getTokenProvider`

##### Utilities
There also exists a utility class that allows to obtain different
`javax.security.auth.login.Configuration` for the most common setup [11]:

- `ConfigurationUtil#getDefaultConfiguration`: default OAK configuration supporting uid/pw login configures `LoginModuleImpl` only

- `ConfigurationUtil#getJackrabbit2Configuration`: backwards compatible configuration that provides the functionality covered by jackrabbit-core DefaultLoginModule, namely:

    - `GuestLoginModule`: null login falls back to anonymous
    - `TokenLoginModule`: covers token base authentication
    - `LoginModuleImpl`: covering regular uid/pw login


#### 7. References

[0] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/GuestLoginModule.java

[1] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/LoginContextProvider.java

[2] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/LoginContext.java

[3] https://java.net/jira/browse/JSR_333-27

[4] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/ImpersonationCredentials.java

[5] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/token/

[6] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authentication/token/TokenLoginModule.java

[7] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/external/

[8] http://dev.day.com/docs/en/crx/current/administering/ldap_authentication.html

[9] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/AuthenticationConfiguration.java

[10] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenConfiguration.java

[11] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/authentication/ConfigurationUtil.java
