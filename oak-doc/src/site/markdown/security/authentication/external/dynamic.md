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

User and Group Synchronization : Dynamic Membership
---------------------------------------------------

As of Oak 1.5.3 the default sync handler comes with an additional configuration 
option (see section [Configuration](defaultusersync.html#configuration) 
that allows to enable dynamic group membership resolution for external users. 

Enabling dynamic membership in the [DefaultSyncConfig] will change the way external
groups are synchronized (see [OAK-4101]) and how automatic group membership 
is being handled (see [OAK-4087])

The key benefits of dynamic membership resolution are:

- avoiding duplicate user management effort wrt to membership handling both in the external IDP and the repository
- avoid storing/updating auto-membership which is assigned to all external users
- ease principal resolution upon repository login

#### SyncContext with Dynamic Membership

With the default `SyncHandler` this configuration option will show the following 
effects:

##### External Groups

- If enabled the handler will use an alternative [SyncContext] to synchronize external groups (`DynamicSyncContext`).
- Instead of synchronizing groups into the user management, this `DynamicSyncContext`
  will additionally set the property `rep:externalPrincipalNames` on the synchronized external user
- `rep:externalPrincipalNames` is a system maintained multivalued property of type 
  'STRING' storing the names of the `java.security.acl.Group`-principals a given 
  external user is member of (both declared and inherited according to the configured
  membership nesting depth)
- External groups will no longer be synchronised into the repository's user management 
  but will only be available as `Principal`s (see section _User Management_ below).
  
Note: as a further improvement the [PrincipalNameResolver] interface was introduced 
in Oak 1.6.1 to allow for optimized resolution of a principal names from a given 
`ExternalIdentityRef`. In order to benefit from that shortcut a given implementation 
of `ExternalIdentityProvider` needs to also implement `PrincipalNameResolver`.
See also [OAK-5210].

##### Automatic Membership

- If enabled automatic membership assignment for existing, local groups will not longer be written to the repository
- Instead the `ExternalPrincipalConfiguration` _("Apache Jackrabbit Oak External PrincipalConfiguration")_ will keep 
  track of the mapping between registered [SyncHandler]s (i.e. auto-membership configuration) and [ExternalIdentityProvider]s.
  This allows to determine auto-membership based on the `rep:externalId` stored with the user accounts.
- The `PrincipalProvider` associated with this dedicated principal configuration 
  will expand the collection of `Principal`s generated for the following calls 
  with the automatically assigned principals:
    - `PrincipalProvider.getGroupMembership(Principal)`
    - `PrincipalProvider.getPrincipals(String)`
- Configured auto-membership groupIds that cannot be resolved to an existing
  `o.a.j.api.security.user.Group` will be ignored in accordance to the default behavior.
- Consequently, the `PrincipalProvider` relies on other `PrincipalProvider` 
  implementations to _own_ these group principals and will not expose them
  upon other calls (e.g.  `PrincipalProvider.getPrincipal(String)`.
- Any changes to the auto-membership configuration will be immediately reflected 
  to new instances of the `PrincipalProvider`.
- Note, that in the initial version (Oak 1.6) only the `user.autoMembership` 
  configuration is respected (see also [OAK-5194] and [OAK-5195])
  
#### Effect of Dynamic Membership on other Security Modules
  
##### Principal Management

The dynamic (principal) membership features comes with a dedicated `PrincipalConfiguration` 
implementation (i.e. [ExternalPrincipalConfiguration]) that is in charge of securing  
the `rep:externalPrincipalNames` properties (see also section [Validation](defaultusersync.html#validation) 
and [Configuration](defaultusersync.html#configuration)). 

Additionally the [ExternalPrincipalConfiguration] provides a `PrincipalProvider` 
implementation which makes external (group) principals available to the repository's 
authentication and authorization using the `rep:externalPrincipalNames` as a 
persistent cache to avoid expensive lookup on the IDP.
This also makes external `Principal`s retrievable and searchable through the 
Jackrabbit principal management API (see section [Principal Management](../../principal.html)
for a comprehensive description).

Please note the following implementation detail wrt accessibility of group principals:
A given external principal will be accessible though the principal management API 
if it can be read from any of the `rep:externalPrincipalNames` properties 
present using a dedicated query.

##### User Management

As described above the dynamic membership option will effectively disable the
synchronization of the complete external group account information into the repository's
user management feature but limit the synchronized information to the principal 
names and the membership relation between a given `java.security.acl.Group` principal 
and external user accounts.

The user management API will consequently no longer be knowledgeable of external 
group identities (exception: groups that have been synchronized before enabling 
the feature will remain untouched and will be synchronized according to the 
sync configuration).

While this behavior does not affect default authentication and authorization modules 
(see below) it will have an impact on applications that rely on full synchronization 
of external identities. Those application won't be able to benefit from the dynamic 
membership feature until dynamic groups can be created with the 
Jackrabbit [User Management API](../../user.html) (see [OAK-2687]).

##### Authentication

The authentication setup provided by Oak is not affected by the dynamic membership 
handling as long as the configured `LoginModule` implementations rely on the 
`PrincipalProvider` for principal resolution and the `ExternalPrincipalConfiguration` 
_("Apache Jackrabbit Oak External PrincipalConfiguration")_ is properly registered 
with the `SecurityProvider` (see section [Configuration](defaultusersync.html#configuration)).

##### Authorization

The authorization modules shipped with Oak only depend on `Principal`s (and not on
user management functionality) and are therefore not affected by the dynamic 
membership configuration.

<!-- references -->
[SyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncHandler.html
[SyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncContext.html
[DefaultSyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncContext.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncConfig.html
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[PrincipalNameResolver]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/PrincipalNameResolver.html
[OAK-4101]: https://issues.apache.org/jira/browse/OAK-4101
[OAK-2687]: https://issues.apache.org/jira/browse/OAK-2687
[OAK-4087]: https://issues.apache.org/jira/browse/OAK-4087
[OAK-5194]: https://issues.apache.org/jira/browse/OAK-5194
[OAK-5195]: https://issues.apache.org/jira/browse/OAK-5195
[OAK-5210]: https://issues.apache.org/jira/browse/OAK-5210