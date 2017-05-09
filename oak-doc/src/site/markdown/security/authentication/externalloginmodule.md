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

Authentication with the External Login Module
--------------------------------------------------------------------------------

### Overview
The purpose of the external login module is to provide a base implementation that
allows easy integration of 3rd party authentication and identity systems, such as
[LDAP](ldap.html). The general mode of the external login module is to use the
external system as authentication source and as a provider for users and groups
that may also be synchronized into the repository.

what it does:

* facilitate the use of a 3rd party system for authentication
* simplify populating the oak user manager with identities from a 3rd party system

what it does not:

* provide a transparent oak user manager
* provide a transparent oak principal provider.
* offer services for background synchronization of users and groups

<a name="details"/>
### Implementation Details
The external identity and login handling is split into 3 parts:

- **External Login Module**: LoginModule implementation that represents the connection between JAAS login mechanism, the external identity provider and the synchronization handler.
- **External Identity Provider** (IDP): This is a service implementing the `ExternalIdentityProvider` interface and is responsible to retrieve and authenticate identities towards an external system (e.g. LDAP).
- **User and Group Synchronization**: This is a service implementing the `SyncHandler` interface and is responsible to actually managing the external identities within the Oak user management. A very trivial implementation might just create users and groups for external ones on demand.

This modularization allows to reuse the same external login module for different
combinations of IDPs and synchronization handlers. Although in practice, systems
usually have 1 of each.

An example where multiple such entities come into play would be the case to use
several LDAP servers for authentication. Here we would configure 2 LDAP IDPs,
1 Sync handler and 2 ExtLMs.

#### External Login Module

##### General
The external login module has 2 main tasks. One is to authenticate credentials
against a 3rd party system, the other is to coordinate syncing of the respective
users and groups with the JCR repository (via the UserManager).

If a user needs re-authentication (for example, if the cache validity expired or
if the user is not yet present in the local system at all), the login module must
check the credentials with the external system during the `login()` method.

The details of the default user/group synchronization mechanism are described in section
[User and Group Synchronization : The Default Implementation](external/defaultusersync.html)

##### Supported Credentials

As of Oak 1.5.1 the `ExternalLoginModule` can deal for any kind of `Credentials`
implementations. By default (i.e. unless configured otherwise) the module supports
`SimpleCredentials` and thus behaves backwards compatible to previous versions.

Additional/other credentials can be supported by providing an `ExternalIdentityProvider` 
that additionally implements the [CredentialsSupport] interface.
See section [Pluggability](#pluggability) for instructions and an example.

##### Authentication in Detail 

The details of the external authentication are as follows:

_Phase 1: Login_

* if the user exists in the repository and any of the following conditions is met **return `false`**
    * user is not an externally synced _or_
    * user belongs to a different IDP than configured for the `ExternalLoginModule` _or_
    * [`PreAuthenticatedLogin`](preauthentication.html) is present on the shared state _and_ the external user doesn't require an updating sync ([OAK-3508])
* if the user exists in the 3rd party system but the credentials don't match it **throws `LoginException`**
* if the user exists in the 3rd party system and the credentials match
    * put the credentials in the shared and private state
    * possibly sync the user
    * and **returns `true`**
* if the user does not exist in the 3rd party system, checks if it needs to remove the user and then it **returns `false`**

_Phase 2: Commit_

* if there is no credentials in the private state, it **returns `false`**
* if there are credentials in the private state propagate the subject and **return `true`**

See section [Example Configurations](external/externallogin_examples.html) for 
some common setup scenarios.

#### External Identity Provider

The `ExternalLoginModule` is designed to work with a pluggable [ExternalIdentityProvider]
implementation that is responsible for validating the authentication request and
provide information about the user that is associated with the specified credentials.

See [External Identity Management](identitymanagement.html) for further information
regarding the identity management API defined by Oak. Section [LDAP](ldap.html)
further describes the `LDAPIdentityProvider` implementation shipped with Oak.

#### User and Group Synchronization

The synchronization of users and groups is triggered by the external login module,
after a user is successfully authenticated against the IDP or if it's no longer
present on the IDP.

See section [User Synchronization](usersync.html) for further details and a
description of the default implementation.

<a name="configuration"/>
### Configuration

#### Configuration Parameters

The external authentication module comes with the following configuration parameters
for the [ExternalLoginModuleFactory]/[ExternalLoginModule].

| Parameter                 | Type     | Default    | Description |
|---------------------------|----------|------------|-------------|
| `PARAM_IDP_NAME`          | String   | \-         | Name of the external IDP to be retrieved from the `ExternalIdentityProviderManager` |
| `PARAM_SYNC_HANDLER_NAME` | String   | \-         | Name of the sync handler to be retrieved from the `SyncManager` |
|                           |          |            |                          |
| _Optional (OSGi-setup)_   |          |            |                          |
| `JAAS_RANKING`            | int      | 50         | Ranking of the `ExternalLoginModule` in the JAAS configuration, see [LoginModuleFactory] |
| `JAAS_CONTROL_FLAG`       | String   | SUFFICIENT | See [LoginModuleControlFlag] for supported values. |
| `JAAS_REALM_NAME`         | String   | \-         | See [LoginModuleFactory] |

##### Examples

###### Example JAAS Configuration

The following JAAS configuration shows how the `ExternalLoginModule` could be
used in a setup that not solely uses third party login (Note: JAAS configuration 
equivalents of the parameters defined by `org.apache.felix.jaas.LoginModuleFactory` 
are omitted):

    jackrabbit.oak {
         org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
         org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl sufficient;
         org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule required
            sync.handlerName="default"
            idp.name="ldap";
     };

<a name="pluggability"/>
### Pluggability

The design of the `ExternalLoginModule` allows for customization of the key features
associated with third party authentication. In an OSGi-based setup these are 
covered by references within the `ExternalLoginModuleFactory`:

 - [ExternalIdentityProviderManager]: Mandatory, unary reference for the `ExternalIdentityProvider` lookup; see [External Identity Management](identitymanagement.html) for details. 
 - [SyncManager]: Mandatory, unary reference for the `SyncHandler` lookup; see [User/Group Synchronization](usersync.html) for details.

The default implementations ([ExternalIDPManagerImpl] and [SyncManagerImpl]) 
extend `AbstractServiceTracker` and will automatically keep track of 
new [ExternalIdentityProvider] and [SyncHandler] services, respectively.

Since Oak 1.5.1 support for different or multiple types of `Credentials` can easily
be plugged by providing an [ExternalIdentityProvider] that additionally implements 
[CredentialsSupport]. This is an optional extension point for each IDP; if 
missing the `ExternalLoginModule` will fall back to a default implementation and 
assume the IDP only supports `SimpleCredentials`. See details below.
 
#### Supported Credentials
 
The following steps are required in order to change or extend the set credential 
classes supported by the `ExternalLoginModule`:

- Extend your `ExternalIdentityProvider` to additionally implement the [CredentialsSupport] interface.

Don't forget to make sure that `ExternalIdentityProvider.authenticate(Credentials)` 
handles the same set of supported credentials!

##### Examples
 
###### Example CredentialsSupport

      @Component()
      @Service(ExternalIdentityProvider.class, CredentialsSupport.class)
      public class MyIdentityProvider implements ExternalIdentityProvider, CredentialsSupport {
    
          public MyCredentialsSupport() {}
    
          //-----------------------------------------< CredentialsSupport >---
          @Nonnull
          @Override
          public Set<Class> getCredentialClasses() {
              return ImmutableSet.<Class>of(MyCredentials.class);
          }
  
          @CheckForNull
          @Override
          public String getUserId(@Nonnull Credentials credentials) {
              if (credentials instanceof MyCredentials) {
                  return ((MyCredentials) credentials).getID();
              } else {
                  return null;
              }
          }
  
          @Nonnull
          @Override
          public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
              // our credentials never contain additional attributes
              return ImmutableMap.of();
          }
          
          //-------------------------------------< ExternalIdentityProvider >---
          
          @CheckForNull
          @Override
          public ExternalUser authenticate(@Nonnull Credentials credentials) {
              if (credentials instanceof MyCredentials) {
                  MyCredentials mc = (MyCredentials) credentials;
                  if (internalAuthenticate(mc)) {
                      return new MyExternalUser(mc.getID());
                  } else {
                      throw new LoginException();
                  }
              } else {
                  return null;
              }
          }
    
          [...]
          
          //----------------------------------------------< SCR Integration >---
          @Activate
          private void activate() {
              // TODO
          }
      }

<!-- references -->
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncConfig.html
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[ExternalIdentityProviderManager]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProviderManager.html
[ExternalIDPManagerImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/ExternalIDPManagerImpl.html
[ExternalLoginModuleFactory]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/ExternalLoginModuleFactory.html
[LoginModuleFactory]: http://svn.apache.org/repos/asf/felix/trunk/jaas/src/main/java/org/apache/felix/jaas/LoginModuleFactory.java
[LoginModuleControlFlag]: https://docs.oracle.com/javase/7/docs/api/javax/security/auth/login/AppConfigurationEntry.LoginModuleControlFlag.html
[SyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncHandler.html
[SyncManager]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncManager.html
[SyncManagerImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/SyncManagerImpl.html
[CredentialsSupport]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/credentials/CredentialsSupport.html
[OAK-3508]: https://issues.apache.org/jira/browse/OAK-3508
