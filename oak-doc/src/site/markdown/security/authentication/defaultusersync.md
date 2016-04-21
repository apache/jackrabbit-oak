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

### DefaultSyncHandler

The [DefaultSyncHandler] comes with a set of configuration options that
allow to specify the synchronization behavior (see below). All users/groups
synchronized by this handler will get the following properties set.

These properties allow to run separate task for periodical update and make sure
the authorizables can later on be identified as external users.

- `rep:externalId` : This allows to identify the external users, know the associated IDP and distinguish them from others.
- `rep:lastSynced` : Sync timestamp to mark the external user/group valid for the configurable time (to reduce expensive syncing). Once expired, they will be validated against the 3rd party system again.


<a name="configuration"/>
### Configuration

#### Configuration of the DefaultSyncHandler

The default `SyncHandler` implementation is configured via [DefaultSyncConfig]:

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


<!-- references -->
[DefaultSyncHandler]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncHandler.html
[DefaultSyncConfig]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/DefaultSyncConfig.html
