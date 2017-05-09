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

Pre-Authenticated Login
--------------------------------------------------------------------------------

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation.

- [Pre-Authentication combined with Login Module Chain](#withloginchain)
- [Pre-Authentication without Repository Involvement](#withoutrepository)

<a name="withloginchain"/>
### Pre-Authentication combined with Login Module Chain

This first variant allows to support 3rd party login modules that wish to provide
the login context with pre authenticated login names, but still want to rely on
the rest of the Oak's login module chain. For example an external SSO login module
can extract the userid from a servlet request and use it to authenticate against
the repository. But instead of re-implementing the user lookup and subject
population (and possible external user synchronization) it just informs any
subsequent login modules that the credential validation was already successful.

The key to understand this mechanism is the [PreAuthenticatedLogin] marker class,
which is pushed to the shared state of the login context and which indicates
to any subsequent LoginModule that the credentials present in the state already
have been verified and thus can be trusted.

This setup is particularly recommended in a OSGi setup that includes Apache Sling
on top of the Oak repository but still requires user information to be synchronized
into the repository.

#### How it works

The basic steps of the pre-authentication in combination with regular JAAS login
module chain are outlined as follows:

1. verify the identity in the layer on top of the JCR repository (e.g. in a custom Sling Authentication Handler)
2. pass a custom, non-public Credentials implementation to the repository login
3. create a custom login module that only supports these dedicated credentials and
   pushes both a new instance of `PreAuthenticatedLogin` and other information
   required and processed by subsequent login modules (e.g. credentials and
   user name).
4. make sure the subsequent login modules in the JAAS configuration are capable
   to deal with the `PreAuthenticatedLogin` and the additional information and
   will properly populate the subject and optionally synchronize user information
   or create login tokens.

##### Example

Example implementation of `LoginModule#login` that pushes the `PreAuthenticatedLogin`
marker to the shared state:

    public class PreAuthLoginModule extends AbstractLoginModule {

    [...]

        @Overwrite
        public boolean login() throws LoginException {
            Credentials credentials = getCredentials();
            if (credentials instanceof MyPreAuthCredentials) {
                userId = ((MyPreAuthCredentials) credentials).getUserId();
                if (userId == null) {
                    log.debug("Could not extract userId/credentials");
                } else {
                    sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(userId));
                    sharedState.put(SHARED_KEY_CREDENTIALS, new SimpleCredentials(userId, new char[0]));
                    sharedState.put(SHARED_KEY_LOGIN_NAME, userId);
                    log.debug("login succeeded with trusted user: {}", userId);
                }
            }

            [...]
            
            // subsequent login modules need to succeed and process the 'PreAuthenticatedLogin'
            return false;
        }
        
        @Overwrite
        public boolean commit() {
            // this module leaves subject population to the subsequent modules 
            // that already handled the login with 'PreAuthenticatedLogin' marker.
            return false;
        }
    }

<a name="withoutrepository"/>
### Pre-Authentication without Repository Involvement

Like in Jackrabbit-core the repository internal authentication verification can
be skipped by calling `Repository#login()` or `Repository#login(null, wspName)`.
In this case the repository implementation expects the verification to be performed
prior to the login call.

This behavior is provided by the default implementation of the `LoginContextProvider` [1]
which expects a `Subject` to be available with the current `java.security.AccessControlContext`.
However, in contrast to Jackrabbit-core the current implementation does not try
to extend the pre-authenticated subject but skips the internal verification step altogether.

#### Options to modify the default behavior

Since the `LoginContextProvider` is a configurable with the authentication setup
OAK users also have the following options by providing a custom `LoginContextProvider`:

- Disable pre-authentication by not trying to retrieve a pre-authenticated `Subject`.
- Add support for extending the pre-authenticated subject by always passing writable subjects to the `JaasLoginContext`
- Dropping JAAS altogether by providing a custom implementation of the
  `org.apache.jackrabbit.oak.spi.security.authentication.LoginContext` [2] interface.

##### Example

Example how to use this type of pre-authentication:

    String userId = "test";
    /**
     * Retrive valid principals e.g. by using Jackrabbit or Oak API:
     * - PrincipalManager#getPrincipal and/or #getGroupMembership
     * - PrincipalProvider#getPrincipals(String userId)
     */
    Set<? extends Principal> principals = getPrincipals(userId);
    AuthInfo authInfo = new AuthInfoImpl(userId, Collections.<String, Object>emptyMap(), principals);
    Subject subject = new Subject(true, principals, Collections.singleton(authInfo), Collections.<Object>emptySet());
    Session session;
    try {
        session = Subject.doAsPrivileged(subject, new PrivilegedExceptionAction<Session>() {
            @Override
            public Session run() throws Exception {
                return login(null, null);
            }
        }, null);
    } catch (PrivilegedActionException e) {
        throw new RepositoryException("failed to retrieve session.", e);
    }

<!-- references -->
[PreAuthenticatedLogin]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/PreAuthenticatedLogin.html
