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

__A. Login Phase__

1. In the first phase, the `LoginModule`'s `login` method gets invoked by the `LoginContext`'s `login` method.
2. The `login` method for the `LoginModule` then performs the actual authentication (prompt for and verify a
   password for example) and saves its authentication status as private state information.
3. Once finished, the `LoginModule`'s login method either returns `true` (if it succeeded) or `false` (if it should
   be ignored), or throws a `LoginException` to specify a failure. In the failure case, the `LoginModule` must not
   retry the authentication or introduce delays. The responsibility of such tasks belongs to the application.
   If the application attempts to retry the authentication, the `LoginModule`'s `login` method will be called again.

__B. Commit Phase__

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

**Required**
:  The LoginModule is required to succeed.  
   If it succeeds or fails, authentication still continues to proceed down the LoginModule list.

**Requisite**
:  The LoginModule is required to succeed.  
   If it succeeds, authentication continues down the LoginModule list.
   If it fails, control immediately returns to the application (authentication does not proceed down the LoginModule 
   list).

**Sufficient**
:  The LoginModule is not required to succeed.    
   If it does succeed, control immediately returns to the application (authentication does not proceed down the 
   LoginModule list).
   If it fails, authentication continues down the LoginModule list.

**Optional**
:  The LoginModule is not required to succeed.  
   If it succeeds or fails, authentication still continues to proceed down the LoginModule list.
 
The overall authentication succeeds **only** if **all** Required and Requisite LoginModules succeed. If a Sufficient 
LoginModule is configured and succeeds, then only the Required and Requisite LoginModules prior to that Sufficient 
LoginModule need to have succeeded for the overall authentication to succeed. If no Required or Requisite LoginModules 
are configured for an application, then at least one Sufficient or Optional LoginModule must succeed.

### JCR and Oak Authentication

Within the scope of JCR `Repository.login` is used to authenticate a given user.
This method either takes a `Credentials` argument if the validation is performed
by the repository itself or `null` in case the user has be pre-authenticated by
an external system.

#### Differences wrt Jackrabbit 2.x

see the corresponding [documentation](authentication/differences.html).

#### Guest Login

_todo_

#### Logins with Credentials

_todo_

#### Impersonation

_todo_

#### Pre Authenticated Logins

Oak provides two different mechanisms to create pre-authentication that doesn't
involve the repositories internal authentication mechanism for credentials
validation.

- Pre-Authentication combined with Login Module Chain
- Pre-Authentication without Repository Involvement

See section [Pre-Authentication Login](authentication/preauthentication.html) for
further details and examples.


### Oak Login Module Implementations

#### Abstract Login Module

_todo_

#### Default Login Module

The behavior of the default login module is relatively simple, so it is explained first:

upon login():

* if a user does not exist in the repository (i.e. cannot be provided by the user manager) it **returns `false`**.
* if an authorizable with the respective userId exists but is a group or a disabled users, it **throws `LoginException`**
* if a user exists in the repository and the credentials don't match, it **throws `LoginException`**
* if a user exists in the repository and the credentials match, it **returns `true`** 
    * also, it adds the credentials to the shared state
    * also, it adds the login name to the shared state 
    * also, it calculates the principals and adds them to the private state
    * also, it adds the credentials to the private state

upon commit():

* if the private state contains the credentials and principals, it adds them (both) to the subject and **returns `true`**
* if the private state does not contain credentials and principals, it clears the state and **returns `false`**

#### Token Login Module

_todo_

### Guest Login Module

_todo_

#### External Login Module

The external login module is a base implementation that allows easy integration
of 3rd party authentication and identity systems, such as [LDAP](ldap.html). The
general mode of the external login module is to use the external system as authentication
source and as a provider for users and groups that may also be synchronized into
the repository.

This login module implementation requires an valid `SyncHandler` and `IdentityProvider`
to be present.

Further reading:
- [External LoginModule and User Synchronization](authentication/externalloginmodule.html): Details regarding the login module and the associated interfaces as well as configuration of the default implementations present with Oak.
- [LDAP Integration](authentication/ldap.html): How to make use of the `ExternalLoginModule` to authenticate against LDAP.


### Authentication related Interfaces and Extension Points in Oak

#### LoginContextProvider

_todo_

#### Token Management

See section [token management](authentication/tokenmanagement.html) for details.

#### External Identity Management

See section [identity management](authentication/identitymanagement.html) for details.

<!-- references -->
[javax.security.auth.spi.LoginModule]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/spi/LoginModule.html
[javax.security.auth.login.Configuration]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/login/Configuration.html