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

User and Group Synchronization : The Default Implementation
--------------------------------------------------------------------------------

### Default Implementation of Sync API

#### SyncManager

The default implementation (`SyncManagerImpl`) is intended for use in an OSGi-base
repository setup: it tracks all `SyncHandler` registered via OSGi.

It can be used in non-OSGi environments by passing a `org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard`
to the constructor.

#### SyncHandler

The [DefaultSyncHandler] comes with a set of configuration options that
allow to specify the synchronization behavior (see below). Depending on the 
configuration it chooses between two different `SyncContext` implementations.

#### SyncContext

Oak provides the following implementations of the [SyncContext] interface:

- [DefaultSyncContext]: base implementation that synchronizes external user and group accounts into the repository
- [DynamicSyncContext]: derived implementation that provides special handling for external groups.
 
##### DefaultSyncContext

All users/groups synchronized by this context will get the following properties set.
These properties allow to run separate task for periodical update and make sure
the authorizables can later on be identified as external users.

- `rep:externalId` : This allows to identify the external users, know the associated IDP and distinguish them from others.
- `rep:lastSynced` : Sync timestamp to mark the external user/group valid for the configurable time (to reduce expensive syncing). Once expired, they will be validated against the 3rd party system again.

NOTE: Since Oak 1.5.8 the system-maintained property `rep:externalId` is 
protected and can not be altered using regular JCR and Jackrabbit 
API, irrespective of the permission setup of the editing session. For 
backwards compatibility this protection can be turned off. See [OAK-4301] 
for further details.

The [DefaultSyncContext] is exported as part of the 'basic' package space and
may be used to provide custom implementations.

##### DynamicSyncContext

Extending from the [DefaultSyncContext] this implementation that provides special 
handling  for external groups in case the [Dynamic Group Membership](#dynamic_membership) 
option is enabled in the [Configuration](#configuration).

In addition to the properties mentioned above this implementation will additionally create 
a multivalued STRING property that caches the group principal names of the external 
user accounts:

- `rep:externalPrincipalNames` : Optional system-maintained property related to [Dynamic Group Membership](#dynamic_membership)

#### SyncResult

The [DefaultSyncResultImpl] is exported as part of the 'basic' package space 
providing a simple `SyncResult` implementation based on a status and a [DefaultSyncedIdentity].


#### SyncedIdentity

The [DefaultSyncedIdentity] is exported as part of the 'basic' package space. It 
maps the ID of a synchronized user/group account to the external identity references
represented by [ExternalIdentityRef].


<a name="dynamic_membership"/>
### Dynamic Group Membership

As of Oak 1.5.3 the default sync handler comes with an addition configuration 
option that allows to enable dynamic group membership resolution for external users. 
Enabling dynamic membership in the [DefaultSyncConfig] will change the way external
groups are synchronized (see also [OAK-4101]).
 
The details and effects on other security related modules are described in 
section [Dynamic Membership](dynamic.html). 

<a name="xml_import"/>
#### XML Import

The protected nature of the `rep:externalPrincipalNames` is also reflected during
XML import of user accounts:

External users with a `rep:externalPrincipalNames` property will get regularly imported.
However, any non-system driven import will omit the `rep:externalPrincipalNames` 
and additional remove the `rep:lastSynced` property in order to force a re-sync
of the external user by the system upon the next login or when triggered through
the JMX console. Depending on the _User Dynamic Membership_ configuration value on
the target system the sync will then result in a full sync of group membership or 
will re-create the `rep:externalPrincipalNames` property.

<a name="validation"/>
#### Validation

##### rep:externalPrincipalNames

As of Oak 1.5.3 a dedicated `Validator` implementation asserts that the protected,
system-maintained property `rep:externalPrincipalNames` is only written by the 
internal system session. 

This prevents users to unintentionally or maliciously manipulating the information
linking to the external identity provider in particular their external identity
and the set of external group principals associated with their account.

Additionally the validator asserts the consistency of the properties defined
with external user/group accounts.

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0070              | Attempt to create, modify or remove the system property 'rep:externalPrincipalNames' |
| 0071              | Attempt to write 'rep:externalPrincipalNames' with a type other than Type.STRINGS |
| 0072              | Property 'rep:externalPrincipalNames' requires 'rep:externalId' to be present on the Node. |
| 0073              | Property 'rep:externalId' cannot be removed if 'rep:externalPrincipalNames' is present. |

##### rep:externalId

If protection of the `rep:externalId` property is enabled (since Oak 1.5.8) the
validator performs the following checks:
 
| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0074              | Attempt to add, modify or remove the system maintained property 'rep:externalId'. |
| 0075              | Property 'rep:externalId' may only have a single value of type STRING. |
 

<a name="configuration"/>
### Configuration

#### Configuration of the DefaultSyncHandler

The default `SyncHandler` implementations are configured via [DefaultSyncConfig]:

| Name                          | Property                      | Description                              |
|-------------------------------|-------------------------------|------------------------------------------|
| Sync Handler Name             | `handler.name`                | Name of this sync configuration. This is used to reference this handler by the login modules. |
| User auto membership          | `user.autoMembership`         | List of groups that a synced user is added to automatically |
| User Expiration Time          | `user.expirationTime`         | Duration until a synced user gets expired (eg. '1h 30m' or '1d'). |
| User Membership Expiration    | `user.membershipExpTime`      | Time after which membership expires (eg. '1h 30m' or '1d'). |
| User membership nesting depth | `user.membershipNestingDepth` | Returns the maximum depth of group nesting when membership relations are synced. A value of 0 effectively disables group membership lookup. A value of 1 only adds the direct groups of a user. This value has no effect when syncing individual groups only when syncing a users membership ancestry. |
| User Dynamic Membership       | `user.dynamicMembership`      | Enabling dynamic membership for external users. |
| User Path Prefix              | `user.pathPrefix`             | The path prefix used when creating new users. |
| User property mapping         | `user.propertyMapping`        | List mapping definition of local properties from external ones. eg: 'profile/email=mail'.Use double quotes for fixed values. eg: 'profile/nt:primaryType="nt:unstructured" |
| Disable missing users         | `user.disableMissing`         | By default, users that no longer exist on the external provider will be locally removed. Set this property to `true` to [disable](https://jackrabbit.apache.org/api/2.8/org/apache/jackrabbit/api/security/user/User.html#disable(java.lang.String)) them instead and have them re-enabled if they become available again. |
| Group auto membership         | `group.autoMembership`        | List of groups that a synced group is added to automatically |
| Group Expiration Time         | `group.expirationTime`        | Duration until a synced group expires (eg. '1h 30m' or '1d'). |
| Group Path Prefix             | `group.pathPrefix`            | The path prefix used when creating new groups. |
| Group property mapping        | `group.propertyMapping`       | List mapping definition of local properties from external ones. |
| | | |

#### Configuration of the 'Apache Jackrabbit Oak External PrincipalConfiguration'

Please note that the `ExternalPrincipalConfiguration` _("Apache Jackrabbit Oak External PrincipalConfiguration")_ 
comes with a dedicated `RepositoryInitializer`, which requires the repository to be (re)initialized
once the module `oak-auth-external` is installed.

The recommended way to assert a proper init, is to add 
'org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalPrincipalConfiguration'
as additional value to the `requiredServicePids` configuration option of the 
`SecurityProviderRegistration` _("Apache Jackrabbit Oak SecurityProvider")_.

See section [Introduction to Oak Security](../../introduction.html) for further details on the `SecurityProviderRegistration`.

The `ExternalPrincipalConfiguration` defines the following configuration options:
     
| Name                         | Property                      | Description                              |
|------------------------------|-------------------------------|------------------------------------------|
| External Identity Protection | `protectExternalId`           | Enables protection of the system maintained `rep:externalId` properties |
| | | |

<!-- references -->
[SyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncContext.html
[DefaultSyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncContext.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncConfig.html
[DefaultSyncResultImpl]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncResultImpl.html
[DefaultSyncedIdentity]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/basic/DefaultSyncedIdentity.html
[DefaultSyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncHandler.html
[ExternalIdentityRef]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityRef.html
[DynamicSyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/principal/DynamicSyncContext.html
[OAK-4101]: https://issues.apache.org/jira/browse/OAK-4101
[OAK-2687]: https://issues.apache.org/jira/browse/OAK-2687
[OAK-4301]: https://issues.apache.org/jira/browse/OAK-4301