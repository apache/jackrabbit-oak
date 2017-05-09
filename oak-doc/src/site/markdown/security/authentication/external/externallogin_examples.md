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

Authentication with External Login Module : Examples
----------------------------------------------------

- [Integration with Standard Oak Authentication](#standard)
- [Integration with Pre-Authentication and Login Module Chain](#preauth)

<a name="standard"/>
### Integration with Standard Oak Authentication

#### Example JAAS Configuration

      Example {
         org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
         org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl sufficient;
         org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule required
                         sync.handlerName="your-synchandler_name"
                         idp.name="your_idp_name";
       };

#### Understanding the Configuration

##### The LoginModule Sequence

- The `TokenLoginModule` is in charge of handling repository authentication 
  request with `TokenCredentials`: 
    - _Login Success_: If token-login succeeds the _sufficient_ flag makes sure
    authentication does not proceed down the `LoginModule` list. This means
    that it will not hit the `ExternalIdentityProvider` and will not re-sync
    an external user as long as the login token is valid.
    - _Login Failure_: If it fails (e.g. other type of `Credentials`) the authentication
    will proceed down the `LoginModule` list.
    - _Commit_: If the login failed the login module will test if the
    `Credentials` passed to the login ask for generation of a new login token.
    If this login succeeded it will populate the `Subject` with `Principal`s,
    `Credentials` and `AuthInfo`.
    
    NOTE: In this setup the `TokenLoginModule` is expected to only handle
    subsequent authentication request after having issued a login token.
    The latter is achieved by providing `Credentials` attributes that force
    the `TokenLoginModule` to generate a new login token in the _commit_ phase.
    The application should then use that login toke for subsequent requests.
  
    See [Token Authentication and Token Management](../tokenmanagement.html) for
    details and for a description of the default implementation.
  
- The `LoginModuleImpl` is in charge of handling authentication request for
  users managed and created through the repository's user management API;
  i.e. users that are not defined by an `ExternalIdentityProvider`. This
  includes built-in system users like the administrator, the guest-user
  (aka anonymous) or `SystemUsers`. It also handles impersonation logins.
    - _Login Success_: If regular user authentication (or impersonation) succeeds
      the _sufficient_ flag makes sure authentication does not proceed 
      down the `LoginModule` list i.e. omits unnecessarily trying to 
      authenticate a local user against the external IDP.
    - _Login Failure_: If the authentication fails (e.g. no local user that
      could have uid/pw matching the passed `Credentials`), it will
      continue down the `LoginModule` list. 
    - _Commit_: If the login succeeded the login module will populate the 
      `Subject` with `Principal`s, `Credentials` and `AuthInfo`.
      
      NOTE: if no login token is generated upon first login, any subsequent
      login for _local_ users will end up being handled by this module or fail.
      
- The `ExternalLoginModule` is in charge of handling authentication request for
  users managed by an `ExternalIdentityProvider`.
    - _Login Success_: If user authentication against the IDP succeeds
      the module synchronizes the external user into the repository according
      to the logic defined in the configure `SyncHandler`. If the user
      has been synced before it might be updated. If and how often a user
      gets re-synced is an implementation detail of the `SyncHandler`.
    - _Login Failure_: If the authentication fails (e.g. wrong IDP or invalid
      `Credentials`), the whole login will fail because the `ExternalLoginModule`
      is configured to be _required_ and the last module in the chain.
    - _Commit_: If the login succeeded the login module will populate the 
      `Subject` with `Principal`s, `Credentials` and `AuthInfo`.
   
      NOTE: if no login token is generated upon first login, any subsequent
      login for _external_ users will end up being handled by this module 
      (including connection to the IDP) or fail.

##### Login with Different Credentials

###### GuestCredentials
- `TokenLoginModule` will ignore
- `LoginModuleImpl` by default supports `GuestCredentials`; success depends 
  on the existence of a valid guest user in the repository. If it succeeds
  authentication doesn't move down to `ExternalLoginModule`.
- `ExternalLoginModule` by default doesn't support `GuestCredentials`
  but may do if a suitable `CredentialsSupport` is configured.

###### SimpleCredentials
- `TokenLoginModule` will ignore
- `LoginModuleImpl` by default supports `SimpleCredentials` and it 
  will succeed if the credentials are successfully validated against a 
  local repository user. It is not expected to succeed for synced 
  external users,which should not have their password synced. If it succeeds 
  authentication doesn't move down to `ExternalLoginModule`.
- `ExternalLoginModule` by default support `SimpleCredentials` and will 
  succeed if authenticating an external against the external IDP including 
  sync is successful. If none of the other modules succeeded the
  `ExternalLoginModule` is required to succeed.

###### TokenCredentials
- `TokenLoginModule` supports `TokenCredentials` and will succeed if the
  credentials are valid. If it succeeds authentication doesn't move down
  the module list. If it fails overall authentication is expected to fail 
  as the subsequent modules are not expected to support `TokenCredentials`. 
- `LoginModuleImpl` does not support  `TokenCredentials` and will fail.
- `ExternalLoginModule` is not expected to support `TokenCredentials` and
  thus overall authentication is expected to fail if `TokenLoginModule`
  failed.

###### ImpersonationCredentials
- `TokenLoginModule` will ignore
- `LoginModuleImpl` by default supports `ImpersonationCredentials` and it 
  will succeed if impersonation for the target user is allowed. If it succeeds 
  authentication doesn't move down to `ExternalLoginModule`.
- `ExternalLoginModule` by default doesn't support `ImpersonationCredentials`
  but may do if a suitable `CredentialsSupport` is configured.

###### Other Credentials
- Overall login success only if the `ExternalLoginModule` supports these credentials
- `TokenLoginModule` will ignore
- `LoginModuleImpl` will ignore
- `ExternalLoginModule` will only succeed if configured with a suitable 
  `CredentialsSupport` that ensures that authentication against the external 
  IDP is successful.

<a name="preauth"/>
### Integration with Pre-Authentication and Login Module Chain

#### Example JAAS Configuration

      Example {
         your.org.PreAuthenticationLoginModule optional;
         org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl optional;
         org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule sufficient
                         sync.handlerName="your-synchandler_name"
                         idp.name="your_idp_name";
       };
       
See [Pre-Authenticated Login](../preauthentication.html#withloginchain) for an example 
`LoginModule` that illustrates how the pre-authentication is being pushed 
to the shared stated.

_Note:_ This configuration has been slightly adjusted from the example in 
[OAK-3508] marking the pre-auth login to be _optional_. This highlights 
the fact that subsequent `LoginModule`s are in charge of respecting the 
`PreAuthenticatedLogin` marker and properly populating the `Subject` in 
the second _commit_ phase.

Also, in the example implementation the login never succeeds (in which case
_sufficient_ would actually work as well). However, if it ever succeeded the 
`PreAuthenticatedLogin` marker would be meaningless and the pre-auth module
in fact would have to populate the  `Subject` i.e. relying on details 
defined and handled by other `LoginModule`s. 

#### Understanding the Configuration

##### The LoginModule Sequence

- The custom pre-auth module is in charge of handling custom pre-auth `Credentials`
  shared between the code performing the authentication outside of the 
  scope of the repository and this module.
  It's only task is to create the `PreAuthenticatedLogin` marker and push
  it to the shared stated to inform subsequent modules, which will always 
  be consulted due to the _optional_ flag.
      - _Login Success_: not desired as we want subsequent modules to verify if
      there is a matching identity for the `PreAuthenticatedLogin` and later on
      populate the subject.
      - _Login Failure_: the default passing over the responsibility the
      other modules in the chain.
      - _Commit_: Nothing to do.
   
- The `LoginModuleImpl` will try to resolve the repository user associated
  with the  `PreAuthenticatedLogin` or perform regular login with the login
  `Credentials` if no `PreAuthenticatedLogin` is present. 
      - _Login Success_: If there exists a valid user for the given `PreAuthenticatedLogin`
      or `Credentials` login will always succeed in case of a pre-auth login.
      Otherwise credentials are regularly evaluated (e.g. password validation). 
      The authentication will continue down the chain due to the _optional_ flag.
      - _Login Failure_: If no matching user exists or if the user is not valid 
      (e.g. disabled). In case of regular authentication it will fail if the
      `Credentials` cannot be validated. Then authentication it will again 
      continue down the `LoginModule` list. 
      - _Commit_: If the login succeeded the login module will populate the 
        `Subject` with `Principal`s, `Credentials` and `AuthInfo`.
  
- The `ExternalLoginModule` will try to resolve the `PreAuthenticatedLogin` or 
  alternatively the `Credentials` to a `SyncedIdentity`.
      - If no `SyncedIdentity` exists the user is retrieved from external IDP
      and eventually synced into the repository. In case no `PreAuthenticatedLogin` 
      is present retrieving identity additionally includes credentials validation.
      - If there exists a `SyncedIdentity` the module will validate it.
      In case of `PreAuthenticatedLogin` it checks if the identity needs to
      be synced again.
      - _Login Success_: If there exists a valid external identity on the
      IDP and it has be synced with the repository.
      - _Login Failure_: If no matching/valid identity exists on the IDP or 
      if there exists a `SyncedIdentity` that doesn't belong to the IDP or
      we have a `PreAuthenticatedLogin` marker and the `SyncedIdentity` doesn't
      need a re-sync. 
      - _Commit_: If the login succeeded the login module will populate the 
      `Subject` with `Principal`s, `Credentials` and `AuthInfo`.

##### Login with Different Credentials

###### Custom Pre-Auth Credentials
- Custom pre-auth module will push `PreAuthenticatedLogin` on the shared state
- Overall login suceeds if any of the subsequent modules is able to deal 
  with the `PreAuthenticatedLogin`.

###### GuestCredentials
- Custom pre-auth module will ignore
- Overall login success if the subsequent modules allow for login with `GuestCredentials`
- `LoginModuleImpl` by default supports `GuestCredentials`; success depends 
  on the existence of a valid guest user in the repository.
- `ExternalLoginModule` by default doesn't support `GuestCredentials`
  but may do if a suitable `CredentialsSupport` is configured.

###### SimpleCredentials
- Custom pre-auth module will ignore
- Overall login success if the subsequent modules allow for login with `SimpleCredentials`
- `LoginModuleImpl` by default supports `SimpleCredentials` and it 
  will succeed if the credentials are successfully validated against a 
  local repository user.
- `ExternalLoginModule` by default support `SimpleCredentials` and will 
  succeed if authentication against the external IDP including sync is successful.

###### ImpersonationCredentials
- Custom pre-auth module will ignore
- Overall login success if the subsequent modules allow for login with `ImpersonationCredentials`
- `LoginModuleImpl` by default supports `ImpersonationCredentials` and it 
  will succeed if impersonation for the target user is allowed.
- `ExternalLoginModule` by default doesn't support `ImpersonationCredentials`
  but may do if a suitable `CredentialsSupport` is configured.

###### Other Credentials
- Overall login success only if the `ExternalLoginModule` supports these credentials
- Custom pre-auth module will ignore
- `LoginModuleImpl` will ignore
- `ExternalLoginModule` will only succeed if configured with a suitable 
  `CredentialsSupport` that ensures that authentication against the external 
  IDP is successful.

##### FAQ

###### Why are the custom 'PreAuthCredentials' not public?

The custom `Credentials` shared between the code performing the authentication
(outside of the repository) and the custom  _PreAuthenticationLoginModule_ 
implementation must neither be public nor shared with other implementations 
in order to prevent un-authenticated login.

###### Why is the 'LoginModuleImpl' not flagged SUFFICIENT?

If `LoginModuleImpl` was defined to be _sufficient_ external identities 
would never be synced again if the `PreAuthenticatedLogin` marker is 
present in the shared state.

###### Why is the 'ExternalLoginModule' not flagged REQUIRED?

If `ExternalLoginModule` was required to succeed, login for _local_ users
was no longer possible. It also would mean that pre-authenticated login
for a `SyncedIdentity` that doesn't needs a re-sync would not longer
be possible and would ultimately fail the repository authentication.

<!-- references -->
[OAK-3508]: https://issues.apache.org/jira/browse/OAK-3508
