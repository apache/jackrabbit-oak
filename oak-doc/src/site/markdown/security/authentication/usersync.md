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

### Synchronization API

- [SyncManager]: factory for all configured `SyncHandler` implementations.
- [SyncHandler]: responsible for synchronizing users/groups from an `ExternalIdentityProvider` into the repository.
- [SyncContext]: executes the synchronization
- [SyncedIdentity]: represents a synchronized identity
- [SyncResult]: the result of a sync operation
- [SyncException]: marker for sync related errors

#### JMX Synchronization Tool

In addition to the synchronization API Oak 1.0 defines utilities to manage
synchronized external identities within JMX (`SynchronizationMBean`) which allows
for the following tasks:

- `syncUsers(String[] userIds, boolean purge)`
- `syncAllUsers(boolean purge)`
- `syncExternalUsers(String[] externalIds)`
- `syncAllExternalUsers()`
- `listOrphanedUsers()`
- `purgeOrphanedUsers()`

### Default Implementation

Oak 1.0 provides a default implementation of the user synchronization API that allow
to plug additional `SyncHandler` implementations. 

Default implementation is described in section [User and Group Synchronization : The Default Implementation](external/defaultusersync.html).

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
