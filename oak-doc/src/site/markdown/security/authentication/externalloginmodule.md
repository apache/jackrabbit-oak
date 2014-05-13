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

### Structure
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

Note:

* users (and groups) that are synced from the 3rd party system contain a `rep:externalId` property. This allows to identify the external users and distinguish them from others.
* to reduce expensive syncing, the synced users and groups have sync timestamp `rep:lastSynced` and are considered valid for a configurable time. if they expire, they need to be validated against the 3rd party system again.

_Phase 1: Login_

* if the user exists in the repository and is not an externally synced, **return `false`**
* if the user exists in the 3rd party system but the credentials don't match it **throws `LoginException`**
* if the user exists in the 3rd party system and the credentials match
    * put the credentials in the shared and private state
    * possibly sync the user
    * and **returns `true`**
* if the user does not exist in the 3rd party system, checks if it needs to remove the user and then it **returns `false`**

_Phase 2: Commit_

* if there is no credentials in the private state, it **returns `false`**
* if there are credentials in the private state propagate the subject and **return `true`**


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

### Configuration

#### Examples

##### Example JAAS Configuration

The following JAAS configuration shows how the `ExternalLoginModule` could be
used in a setup that not solely uses third party login:

    jackrabbit.oak {
         org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
         org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl sufficient;
         org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule required
            sync.handlerName="default"
            idp.name="ldap";
     };

<!-- references -->

[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncConfig.html
