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

#### Guest Login vs Null Login

##### Null Login

As of Oak 1.0 `Repository#login()` and `Repository#login(null, wspName)` is no
longer treated as guest login but as _pre-authenticed_ as defined by JSR 283. 
In order to get backwards compatible behavior, Oak comes with a dedicated 
[GuestLoginModule] that can be added to the JAAS (or the corresponding OSGi) 
configuration.

##### Guest Login

With respect to guest login (aka anonymous login) the Oak content repository
out of the box contains the following modifications:

- null login != guest login
- special password handling of the anonymous user has been dropped
- the anonymous user by default doesn't have a password set

##### Pre-Authentication

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation. See the corresponding section [Pre-Authentication](preauthentication.html)
for details and examples.

- Pre-Authentication combined with Login Module Chain
- Pre-Authentication without Repository Involvement: the `Subject` must be available
  with the current `java.security.AccessControlContext`.

#### Impersonation

##### Self-Impersonation (aka Cloning a Session)

As of OAK 1.0 the latest changes made to JSR 333 with respect to `Session#impersonate`
have been adopted ([JSR_333-27]): Any attempt to impersonate the same session (self-impersonation)
will succeed as long as the user is still valid.

##### Impersonation Credentials

The OAK implementation of `Session#impersonate` no longer uses `SimpleCredentials`
to transport the original `Subject` but rather performs the login with dedicated
[ImpersonationCredentials].

This modification will not affect applications that used JCR API to impersonate
a given session. However the following example which 'manually' builds impersonation
credentials the way jackrabbit core was handling it will **no longer work** to
impersonate an existing session:

     org.apache.jackrabbit.core.SessionImpl sImpl = (SessionImpl) mySession;
     SimpleCredentials jrImpCreds = new SimpleCredentials("someUserId, new char[0]);
     creds.setAttribute(SecurityConstants.IMPERSONATOR_ATTRIBUTE, sImpl.getSubject());
     Session impersonated = sImpl.getRepository().login(jrImpCreds, sImpl.getWorkspace().getName());

Upon migration to Oak such implementation specific code should be refactored
to use regular JCR API for impersonation:

     // Note: build credentials depends on the auth setup !
     Credentials impersonationCredentials = new SimpleCredentials("someUserId, new char[0]);
     Session impersonated = session.impersonate(impersonationCredentials);

In order to achieve impersonation on the Oak API directly:

     ContentRepository contentRepo = ...
     ContentSession editingSession = ...

     AuthInfo impersonatorInfo = editingSession.getAuthInfo();
     Credentials credentials = new SimpleCredentials("someUserId, new char[0]);
     ImpersonationCredentials impersonationCredentials = new ImpersonationCredentials(credentials, impersonatorInfo);
     ContentSession impersonated = contentRepo.login(impersonationCredentials, editingSession.getWorkspaceName());

#### Token based Authentication

The token based authentication has been completely refactor in Oak as described
in section [Token Management](tokenmanagement.html). The default implementation
differs from Jackrabbit as follows

- token node is created with dedicated node type (rep:Token)
- expiration and key properties are mandatory and protected properties
- expiration time is obtained from `PARAM_TOKEN_EXPIRATION` is defined by
  the token management API.

As far as the token based authentication itself is concerned the Oak implementation
contains the following changes compared to Jackrabbit 2.x:

- token based authentication is completely separated from regular uid/pw authentication.
- the dedicated [TokenLoginModule] is both responsible for creating new login tokens
  performing the the authentication for [TokenCredentials] passed to the repository
  login. Other login modules should not attempt to do so.
- token characteristics such as expiration time only need to be configured with the
  token management API; other `LoginModule` implementations no longer need to have
  the same config options set.

<!-- references -->

[TokenLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/token/TokenLoginModule.html
[TokenCredentials]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authentication/token/TokenCredentials.java
[com.day.crx.security.ldap.LDAPLoginModule]: http://dev.day.com/docs/en/crx/current/administering/ldap_authentication.html
[GuestLoginModule]:/oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/GuestLoginModule.html
[JSR_333-27]: https://java.net/jira/browse/JSR_333-27
[ImpersonationCredentials]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/ImpersonationCredentials.html
