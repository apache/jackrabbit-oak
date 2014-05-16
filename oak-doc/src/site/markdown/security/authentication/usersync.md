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

User and Group Synchronization
--------------------------------------------------------------------------------

### General

The synchronization of users and groups is triggered by the [ExternalLoginModule](externalloginmodule.html),
after a user is successfully authenticated against the IDP or if it's no longer
present on the IDP.

Oak comes with a default implementation of the `SyncHandler` interface:
[org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler].

### Synchronization API

- [SyncManager]: factory for all configured `SyncHandler` implementations.
- [SyncHandler]: responsible for synchronizing users/groups from an `ExternalIdentityProvider` into the repository.
- [SyncContext]: executes the synchronization
- [SyncedIdentity]: represents a synchronized identity
- [SyncResult]: the result of a sync operation
- [SyncException]

### Default Implementation

Oak 1.0 provides a default implementation of the user synchronization API that allow
to plug additional `SyncHandler` implementations.

The [DefaultSyncHandler] itself comes with a set of configuration options that
allow to specify the synchronization behavior (see below). All users/groups
synchronized by this handler will get the following properties set:

- `rep:externalId`
- `rep:lastSynced`

These properties allow to run separat task for periodical update and make sure
the authorizables can later on be identitied as external users.


### Configuration

#### Configuration of the DefaultSyncHandler

The default sync handler implementation is configured via [DefaultSyncConfig]:

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
| | | |


### Pluggability

There are two ways to replace/change the user synchronization behavior

1. Write custom `SyncManager`
2. Write custom `SyncHandler`

The following steps are required in order to replace the default `SyncManager` implementation
or plug a new implementation of the `SyncHandler`:

- write your custom implementation of the interface
- make the manager/handler available to the authentication setup or sync manager
    - OSGi setup: making the implementation an OSGi service
    - non-OSGi setup: configure the manager/handler during manual [Repository Construction](../../construct.html).


<!-- references -->
[SyncManager]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncManager.html
[SyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncHandler.html
[SyncContext]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncContext.html
[SyncedIdentity]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncedIdentity.html
[SyncResult]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncResult.html
[SyncException]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/SyncException.html
[DefaultSyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncHandler.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncConfig.html
