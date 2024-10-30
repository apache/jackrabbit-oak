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

# User and Group Synchronization : Dynamic Membership and Dynamic Groups
----------------------------------------------------------------------

<!-- MACRO{toc} -->

As of Oak 1.5.3 the default sync handler comes with an additional configuration 
option (see section [Configuration](defaultusersync.html#configuration) 
that allows enabling dynamic group membership resolution for external users. 

Enabling dynamic sync options in the [DefaultSyncConfig] will change the way external
groups are synchronized (see [OAK-4101]) and how automatic group membership 
is being handled (see [OAK-4087]).

The key benefits of dynamic membership resolution are:

- avoiding duplicate user management effort wrt to membership handling both in the external IDP and the repository
- avoid storing/updating auto-membership which is assigned to all external users
- ease principal resolution upon repository login

See also [FAQ](faq.html#Dynamic_Sync) for frequently asked questions about the dynamic sync.

## SyncContext with Dynamic Membership

With the default `SyncHandler` this configuration option will show the following 
effects:

### External Groups

- If enabled the handler will use an alternative [SyncContext] to synchronize external groups (`DynamicSyncContext`).
- Instead of synchronizing membership information alongside the group accounts, this `DynamicSyncContext`
  will set the property `rep:externalPrincipalNames` on the synchronized external user
- `rep:externalPrincipalNames` is a system maintained multivalued property of type 
  'STRING' storing the names of the `java.security.acl.Group`-principals a given 
  external user is member of (both declared and inherited according to the configured
  membership nesting depth)
- By default, external groups will no longer be synchronized into the repository's user management 
  but will only be available as `Principal`s (see section _User Management_ below).
- If the `Dynamic Groups` option is enabled together with the `Dynamic Membership`, external groups will be 
  synchronized into the user management but marked as _dynamic_. User-Group relationship for these dynamic external 
  groups will be determined by a dedicated `DynamicMembershipService` that is registered if both options are enabled
  for a given `SyncHandler` mapping.
  
Note: as a further improvement the [PrincipalNameResolver] interface was introduced 
in Oak 1.6.1 to allow for optimized resolution of a principal names from a given 
`ExternalIdentityRef`. In order to benefit from that shortcut a given implementation 
of `ExternalIdentityProvider` needs to also implement `PrincipalNameResolver`.
See also [OAK-5210].

### Automatic Membership

- If enabled automatic membership assignment for existing, local groups will not longer be written to the repository
- Instead, the `ExternalPrincipalConfiguration` _("Apache Jackrabbit Oak External PrincipalConfiguration")_ will keep 
  track of the mapping between registered [SyncHandler]s (i.e. auto-membership configuration) and [ExternalIdentityProvider]s 
  and determine auto-membership based on the `rep:externalId` stored with the user accounts.
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
- With [OAK-9462] an implementation of `DynamicMembershipProvider` will be registered 
  and reflect autoMembership for synchronized external users in the User Management API (see below).
  The same applies for the conditional auto-membership as introduced with [OAK-9463].
  
## Effect of Dynamic Membership on other Security Modules
  
### Principal Management

The dynamic (principal) membership features comes with a dedicated `PrincipalConfiguration` 
implementation (i.e. [ExternalPrincipalConfiguration]) that is in charge of securing  
the `rep:externalPrincipalNames` properties (see also section [Validation](defaultusersync.html#validation) 
and [Configuration](defaultusersync.html#configuration)). 

Additionally, the [ExternalPrincipalConfiguration] provides a `PrincipalProvider` 
implementation which makes external (group) principals available to the repository's 
authentication and authorization using the `rep:externalPrincipalNames` as a 
persistent cache to avoid an expensive lookup on the IDP.
This also makes external `Principal`s retrievable and searchable through the 
Jackrabbit principal management API (see section [Principal Management](../../principal.html)
for a comprehensive description).

Please note the following implementation detail wrt accessibility of group principals:
A given external principal will be accessible though the principal management API 
if it can be read from any of the `rep:externalPrincipalNames` properties 
present using a dedicated query.

#### API Overview

- `extUserName`       : the principal name of an external user
- `extGroupName`      : the principal name of an external group
- `extUserPrincipal`  : the principal associated with a synchronized external user
- `extGroupPrincipal` : the principal associated with a synchronized external group

| API Call                                                 | Default Sync | Dynamic Membership | Dynamic Membership + Dynamic Groups | Comment |
-----------------------------------------------------------|--------------|--------------------|------------------------|---------|
| `PrincipalManager.getPrincipal(extUserName)`             | ok           | ok                 | ok                     | |
| `PrincipalManager.getPrincipal(extGroupName)`            | ok           | (ok) <sup>1</sup>  | ok                     | <sup>1</sup> If the editing session can read any `rep:externalPrincipalNames` property containing the group principal name |
| `PrincipalManager.getGroupMembership(extUserPrincipal)`  | ok           | ok                 | ok                     | Dynamic group principals include both declared external groups and configured auto-membership principals (including inherited principals).|
| `PrincipalManager.getGroupMembership(extGroupPrincipal)` | ok           | - <sup>2</sup>     | - <sup>2,3</sup>       | <sup>2</sup> Group membership gets flattened and stored with the external user. Group-group relationship is not preserved.<br/><sup>3</sup> For dynamic groups synced into the repository the configured auto-membership principals are resolved, see also user management API below.  |

### User Management
#### User Management without Dynamic Groups Option

Unless the 'Dynamic Groups' option is set additionally, the dynamic membership option will effectively disable the
synchronization of the external group account information into the repository's user management feature.
It will instead limit the synchronized information to the group principal names and the membership relation between a 
given `java.security.acl.Group` principal and external user accounts.

The user management API will consequently no longer be knowledgeable of **external group identities**.

For groups that have been synchronized before dynamic membership got enabled, the following rules will 
apply:
- if option `user.enforceDynamicMembership` is disabled (default), previously synced groups, and their member information will continue to be synchronized according to the sync configuration.
- if option `user.enforceDynamicMembership` is enabled, previously synced membership will be migrated to become dynamic upon user synchronization. The synchronized group will be removed once it not longer has any declared members.

While this behavior does not affect default authentication and authorization modules 
(see below) it will have an impact on applications that rely on full synchronization 
of external identities. Those application won't be able to benefit from the dynamic 
membership feature until dynamic groups can be created with the 
Jackrabbit [User Management API](../../user.html) (see [OAK-2687]).

Note however, that with [OAK-9462] groups listed in the **autoMembership** configuration 
parameters as well as the optional [AutoMembershipConfig] will have dynamic group membership 
of external user identities reflected in the corresponding API calls, most notably `Group.isMember`, 
`Group.isDeclaredMember`, `Group.getMembers`, `Group.getDeclaredMembers` as well as `Authorizable.memberOf`
and `Authorizable.declaredMemberOf()`.

#### User Management with Dynamic Groups Option enabled

If the 'Dynamic Groups' flag is turned on in addition, external group accounts will continue to be synchronized into the 
repository's user management. However, membership information will not be stored together with the groups but instead will 
be dynamically calculated from the `rep:externalPrincipalNames` property caching the membership information with the user 
accounts. This is achieved by means of a dedicated implementation of the `DynamicMembershipProvider` interface.

For groups that have been synchronized prior to enabling dynamic membership, the following rules will 
apply:
- if option `user.enforceDynamicMembership` is disabled (default), previously synced groups, and their member information will continue to be synchronized according to the sync configuration.
- if option `user.enforceDynamicMembership` is enabled, previously synced membership will be migrated to become dynamic upon user synchronization. The synchronized group will _not_ be removed once it not longer has any declared members.
 
Note, that manually adding members to these dynamic external groups using `Group.addMember`, `Group.addMembers` or 
equivalent Oak API operations will be prevented by a dedicated validator that is enabled as soon as the _Dynamic Groups_
option is present together with _Dynamic Membership_.

#### API Overview

- `extUserId`  : the ID of a synchronized external user
- `extGroupId` : the ID of a synchronized external group
- `extUser`    : a synchronized external user as `org.apache.jackrabbit.api.security.user.User`
- `extGroup`   : a synchronized external group as `org.apache.jackrabbit.api.security.user.Group`
- `autoGroup`  : a local group configured in the auto-membership option of the `DefaultSyncConfig`

| API Call                                                 | Default Sync | Dynamic Membership | Dynamic Membership + Dynamic Groups | Comment |
-----------------------------------------------------------|--------------|--------------------|------------------------|---------|
| `UserManager.getAuthorizable(extUserId)`                 | ok           | ok                 | ok                     | Same applies for<br/>`UserManager.getAuthorizable(extUserId, User.class)`,<br/>`UserManager.getAuthorizable(extUserPrincipal)`,<br/>`UserManager.getAuthorizableByPath(extUserPath)` |
| `UserManager.getAuthorizable(extGroupId)`                | ok           | -                  | ok                     | Same applies for<br/>`UserManager.getAuthorizable(extGroupId, Group.class)`,<br/>`UserManager.getAuthorizable(extGroupPrincipal)`,<br/>`UserManager.getAuthorizableByPath(extGroupPath)`        |
| `extUser.declaredMemberOf()`                             | ok           | - <sup>3</sup>     | (ok) <sup>4</sup>      | <sup>3</sup> Only auto-membership to local groups, external groups not synced.<br/><sup>4</sup> Same as `User.memberOf()` as nested group membership gets flattened upon dynamic sync. Configured auto-membership is reflected through dynamic `AutoMembershipProvider`. |
| `extUser.memberOf()`                                     | ok           | - <sup>3</sup>     | ok                     | |
| `extGroup.declaredMemberOf()`                            | ok           | - <sup>5</sup>     | - <sup>6</sup>         | <sup>5</sup> External groups not synced!<br/><sup>6</sup> Only (conditional) automembership as upon dynamic sync nested group membership gets flattened |
| `extGroup.memberOf()`                                    | ok           | - <sup>5</sup>     | - <sup>6</sup>         | |
| `extGroup.getDeclaredMembers()`                          | ok           | - <sup>5</sup>     | (ok) <sup>7</sup>      | <sup>7</sup> Same as `Group.getMembers()` | 
| `extGroup.getMembers()`                                  | ok           | - <sup>5</sup>     | (ok) <sup>8</sup>      | <sup>8</sup> Only includes external users as nested membership gets flattened upon dynamic sync. | 
| `extGroup.isDeclaredMember(extUser)`                     | ok           | - <sup>5</sup>     | (ok) <sup>9</sup>      | <sup>9</sup> Same as `Group.isMember(extUser)` |
| `extGroup.isMember(extUser)`                             | ok           | - <sup>5</sup>     | ok                     |  | 
| `extGroup.isDeclaredMember(extGroup)`                    | ok           | - <sup>5</sup>     | - <sup>10</sup>        | <sup>10</sup> No group-group relations as nested membership gets flattened  | 
| `extGroup.isMember(extGroup)`                            | ok           | - <sup>5</sup>     | - <sup>10</sup>        |  | 
| `extGroup.addMember(Authorizable)`                       | ok           | - <sup>5</sup>     | - <sup>11</sup>        | <sup>11</sup> Adding members to dynamic groups will fail upon commit. | 
| `extGroup.addMembers(String...)`                         | ok           | - <sup>5</sup>     | - <sup>11</sup>        |  | 
| `extGroup.removeMember(Authorizable)`                    | ok           | - <sup>5</sup>     | ok                     |  | 
| `extGroup.removeMembers(String...)`                      | ok           | - <sup>5</sup>     | ok                     |  | 
| `autoGroup.isDeclaredMember(extUser)`                    | ok           | ok <sup>12</sup>   | ok <sup>12</sup>       | <sup>12</sup> Through `AutoMembershipProvider` but not stored with local group node that is listed in 'auto-membership' config. |
| `autoGroup.isMember(extUser)`                            | ok           | ok <sup>12</sup>   | ok <sup>12</sup>       |  |
| `autoGroup.isDeclaredMember(extGroup)`                   | ok           | - <sup>5</sup>     | ok <sup>12</sup>       |  | 
| `autoGroup.isMember(extGroup)`                           | ok           | - <sup>5</sup>     | ok <sup>12</sup>       |  |
| `autoGroup.getDeclaredMembers()`                         | ok           | (ok) <sup>5,12</sup>| ok <sup>12</sup>      |  |
| `autoGroup.getMembers()`                                 | ok           | (ok) <sup>5,12</sup>| ok <sup>12</sup>      |  |

### Authentication

The authentication setup provided by Oak is not affected by the dynamic membership 
handling as long as the configured `LoginModule` implementations rely on the 
`PrincipalProvider` for principal resolution and the `ExternalPrincipalConfiguration` 
_("Apache Jackrabbit Oak External PrincipalConfiguration")_ is properly registered 
with the `SecurityProvider` (see section [Configuration](defaultusersync.html#configuration)).

### Authorization

The authorization modules shipped with Oak only depend on `Principal`s (and not on
user management functionality) and are therefore not affected by the dynamic 
membership configuration.

### Principal Resolution Cache

Since Oak 1.72 `ExternalGroupPrincipalProvider`, see [OAK-11026], will cache the principal resolution for 
those dynamic groups that have been added to local groups. Caching is disabled by default and can be enabled 
as described in the section [Caching Results of Principal Resolution](../../principal/cache.html#configuration).

Cache enablement has a direct impact on the performance of the principal resolution for dynamic groups included in 
local groups. Benchmarks have shown that the cache can significantly reduce the time required for principal resolution
is subsequent login attempts, see [OAK-11171] for benchmark results.

<!-- references -->
[SyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncHandler.html
[SyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncContext.html
[DefaultSyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncContext.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncConfig.html
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[PrincipalNameResolver]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/PrincipalNameResolver.html
[AutoMembershipConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/AutoMembershipConfig.html
[OAK-4101]: https://issues.apache.org/jira/browse/OAK-4101
[OAK-2687]: https://issues.apache.org/jira/browse/OAK-2687
[OAK-4087]: https://issues.apache.org/jira/browse/OAK-4087
[OAK-5194]: https://issues.apache.org/jira/browse/OAK-5194
[OAK-5195]: https://issues.apache.org/jira/browse/OAK-5195
[OAK-5210]: https://issues.apache.org/jira/browse/OAK-5210
[OAK-9462]: https://issues.apache.org/jira/browse/OAK-9462
[OAK-9463]: https://issues.apache.org/jira/browse/OAK-9463
[OAK-11026]: https://issues.apache.org/jira/browse/OAK-11026
[OAK-11171] https://issues.apache.org/jira/browse/OAK-11171