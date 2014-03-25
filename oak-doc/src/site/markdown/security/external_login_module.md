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
The Oak Security Layer
======================

Authentication / Login Modules
------------------------------------------------------------------------------------------------------------------------

### Types of login modules
In order to understand how login modules work and how Oak can help providing extension points we need to look at how
JAAS authentication works in general and discuss where the actual credential-verification is performed.

#### Brief recap of the JAAS authentication
The following section is copied and adapted from the javadoc of [javax.security.auth.spi.LoginModule]:

The authentication process within the `LoginModule` proceeds in two distinct phases. 

1.

   1. In the first phase, the `LoginModule`'s `login` method gets invoked by the `LoginContext`'s `login` method.
   2. The `login` method for the `LoginModule` then performs the actual authentication (prompt for and verify a 
      password for example) and saves its authentication status as private state information. 
   3. Once finished, the `LoginModule`'s login method either returns `true` (if it succeeded) or `false` (if it should 
      be ignored), or throws a `LoginException` to specify a failure. In the failure case, the `LoginModule` must not 
      retry the authentication or introduce delays. The responsibility of such tasks belongs to the application. 
      If the application attempts to retry the authentication, the `LoginModule`'s `login` method will be called again.

2.

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
      
#### Login module execution order
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

### Pre Authenticated Logins
Pre authenticated logins allows to support 3rd party login modules that wish to provide the login context with pre authenticated login names, but still want to rely on the rest of the oak's login module chain. For example an external SSO login module can extract the userid from a servlet request and use it to authenticate against the repository. But instead of re-implementing the user lookup and subject population (and possible external user synchronization) it just sets a respective [org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin] on the shared state.

Default Login Module
------------------------------------------------------------------------------------------------------------------------
### Behavior of the Default Login Module
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


External Login Module
------------------------------------------------------------------------------------------------------------------------

### Overview
The purpose of the external login module is to provide a base implementation that allows easy integration of 3rd party 
authentication and identity systems, such as LDAP. The general mode of the external login module is to use the external
system as authentication source and as a provider for users and groups.

what it does:

* facilitate the use of a 3rd party system for authentication
* simplify populating the oak user manager with identities from a 3rd party system

what it does not:

* provide a transparent oak user manager
* provide a transparent oak principal provider.
* offer services for background synchronization of users and groups

### Structure
The external identity and login handling is split into 3 parts:

1. An external identity provider (IDP). This is a service implementing the `ExternalIdentityProvider` interface and is responsible to retrieve and authenticate identities towards an external system (e.g. LDAP).
2. An synchronization handler. This is a service implementing the `SyncHandler` interface and is responsible to actually managing the external identities within the Oak user management. A very trivial implementation might just create users and groups for external ones on demand.
3. The external login module (ExtLM). This is the connection between JAAS login mechanism, the external identity provider and the synchronization handler.

This modularization allows to reuse the same external login module for different combinations of IDPs and synchronization handlers. Although in practice, systems usually have 1 of each. 

An example where multiple such entities come into play would be the case to use several LDAP servers for authentication. Here we would configure 2 LDAP IDPs, 1 Sync handler and 2 ExtLMs.


#### Authentication and subject population
The goal of the external login module is to provide a very simple way of using 
_"the users stored in an external system for authentication and authorization in the Oak content repository"_. So the
easiest way of doing this is to import the users on-demand when they log in. 

### Behavior of the External Login Module

#### General
The external login module has 2 main tasks. one is to authenticate credentials against a 3rd party system, the other is
to coordinate syncing of the respective users and groups with the JCR repository (via the UserManager).

If a user needs re-authentication (for example, if the cache validity expired or if the user is not yet present in the
local system at all), the login module must check the credentials with the external system during the `login()` method. 

**ExternalLoginModule**

Note:

* users (and groups) that are synced from the 3rd party system contain a `rep:externalId` property. This allows to identify the external users and distinguish them from others.
* to reduce expensive syncing, the synced users and groups have sync timestamp `rep:lastSynced` and are considered valid for a configurable time. if they expire, they need to be validated against the 3rd party system again.

upon login():

* if the user exists in the repository and is not an externally synced, **return `false`**
* if the user exists in the 3rd party system but the credentials don't match it **throws `LoginException`**
* if the user exists in the 3rd party system and the credentials match
    * put the credentials in the shared and private state 
    * possibly sync the user 
    * and **returns `true`**
* if the user does not exist in the 3rd party system, checks if it needs to remove the user and then it **returns `false`**

upon commit():

* if there is no credentials in the private state, it **returns `false`**
* if there are credentials in the private state propagate the subject and **return `true`**

User and Group Synchronization
------------------------------------------------------------------------------------------------------------------------
The synchronization of users and groups is triggered by the external login module, after a user is successfully 
authenticated against the IDP or if it's no longer present on the IDP.

### Configuration of the DefaultSyncHandler
Oak provides a default synchronization handler that is configured via [org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfig]. The handler is configured either via OSGi or during manual [Repository Construction](../construct.html).

| Name                          | Property                      | Description                              |
|-------------------------------|-------------------------------|------------------------------------------|
| Sync Handler Name             | `handler.name`                | Name of this sync configuration. This is used to reference this handler by the login modules. |
| User auto membership          | `user.autoMembership`         | List of groups that a synced user is added to automatically |
| User Expiration Time          | `user.expirationTime`         | Duration until a synced user gets expired (eg. '1h 30m' or '1d'). |
| User Membership Expiration    | `user.membershipExpTime`      | Time after which membership expires (eg. '1h 30m' or '1d'). |
| User membership nesting depth | `user.membershipNestingDepth` | Returns the maximum depth of group nesting when membership relations are synced. A value of 0 effectively disables group membership lookup. A value of 1 only adds the direct groups of a user. This value has no effect when syncing individual groups only when syncing a users membership ancestry. |
| User Path Prefix              | `user.pathPrefix`             | The path prefix used when creating new users. |
| User property mapping         | `user.propertyMapping`        | List mapping definition of local properties from external ones. eg: 'profile/email=mail'.Use double quotes for fixed values. eg: 'profile/nt:primaryType="nt:unstructured" |
| Group auto membership         | `group.autoMembership`        | List of groups that a synced group is added to automatically |
| Group Expiration Time         | `group.expirationTime`        | Duration until a synced group expires (eg. '1h 30m' or '1d'). |
| Group Path Prefix             | `group.pathPrefix`            | The path prefix used when creating new groups. |
| Group property mapping        | `group.propertyMapping`       | List mapping definition of local properties from external ones. |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | | |

LDAP Identity Provider
------------------------------------------------------------------------------------------------------------------------
Oak comes with a default implementation of an LDAP identity provider.

### Configuration
The LDAP IPDs are configured through the [org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig] 
which is populated either via OSGi or during manual [Repository Construction](../construct.html).

| Name                         | Property                | Description                              |
|------------------------------|-------------------------|------------------------------------------|
| LDAP Provider Name           | `provider.name`         | Name of this LDAP provider configuration. This is used to reference this provider by the login modules. |
| Bind DN                      | `bind.dn`               | DN of the user for authentication. Leave empty for anonymous bind. |
| Bind Password                | `bind.password`         | Password of the user for authentication. |
| LDAP Server Hostname         | `host.name`             | Hostname of the LDAP server              |
| Disable certificate checking | `host.noCertCheck`      | Indicates if server certificate validation should be disabled. |
| LDAP Server Port             | `host.port`             | Port of the LDAP server                  |
| Use SSL                      | `host.ssl`              | Indicates if an SSL (LDAPs) connection should be used. |
| Use TLS                      | `host.tls`              | Indicates if TLS should be started on connections. |
| Search Timeout               | `searchTimeout`         | Time in until a search times out (eg: '1s' or '1m 30s'). |
| User base DN                 | `user.baseDN`           | The base DN for user searches.           |
| User extra filter            | `user.extraFilter`      | Extra LDAP filter to use when searching for users. The final filter is formatted like: `(&(<idAttr>=<userId>)(objectclass=<objectclass>)<extraFilter>)` |
| User id attribute            | `user.idAttribute`      | Name of the attribute that contains the user id. |
| User DN paths                | `user.makeDnPath`       | Controls if the DN should be used for calculating a portion of the intermediate path. |
| User object classes          | `user.objectclass`      | The list of object classes an user entry must contain. |
| Group base DN                | `group.baseDN`          | The base DN for group searches.          |
| Group extra filter           | `group.extraFilter`     | Extra LDAP filter to use when searching for groups. The final filter is formatted like: `(&(<nameAttr>=<groupName>)(objectclass=<objectclass>)<extraFilter>)` |
| Group DN paths               | `group.makeDnPath`      | Controls if the DN should be used for calculating a portion of the intermediate path. |
| Group member attribute       | `group.memberAttribute` | Group attribute that contains the member(s) of a group. |
| Group name attribute         | `group.nameAttribute`   | Name of the attribute that contains the group name. |
| Group object classes         | `group.objectclass`     | The list of object classes a group entry must contain. |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | | |


<!-- references -->
[javax.security.auth.spi.LoginModule]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/spi/LoginModule.html
[javax.security.auth.login.Configuration]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/login/Configuration.html
[org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/PreAuthenticatedLogin.html
[org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncConfig.html
[org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/ldap/impl/LdapProviderConfig.html


