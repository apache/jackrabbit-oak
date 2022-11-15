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

# External Authentication : FAQ

<!-- MACRO{toc} -->

## General

| Question          | Answer                        | References        |
|-------------------|-------------------------------|-------------------|
| Why am I no longer able to change the `rep:externalId`?               | Since Oak 1.5.8 the default sync mechanism properly protects the system maintained property `rep:externalId` which is used to link a given synced user/group account to the corresponding entry on the external IDP. | See [documentation](defaultusersync.html) and [OAK-4301] |
| Why does a user or group created with a content package not get synced with the IDP? | Only users/groups with a `rep:externalId` linking them to the external IDP will be respected during the default sync mechanism. | See also [OAK-4397] and [OAK-5304] |
| Synchronized user/group is not updated | The default sync configuration defines an expiration time before identities get re-synced | See section [Configuration](defaultusersync.html#configuration) | 
| Membership information is not store | The default sync configuration needs to define a `user.membershipNestingDepth` > 0 in order to have external membership information synchronized  | See section [Configuration](defaultusersync.html#configuration) | 
| Membership information is not updated | The default sync configuration defines `user.membershipExpTime` before membership get re-synced  | See section [Configuration](defaultusersync.html#configuration) | 
| Can I synchronize identities outside of the repository login? | Yes, there is a `SynchronizationMBean` in the JMX console with additional synchronization options | |

## Dynamic Sync
See [User and Group Synchronization : Dynamic Membership and Dynamic Groups](dynamic.html) for further details.

| Question          | Answer                        | References        |
|-------------------|-------------------------------|-------------------|
| The external group doesn't get created | The dynamic membership option will only synchronize the membership information but not the group accounts. Additionally enabling 'Dynamic Groups' option will make sure groups are synchronized while keeping the dynamic nature of the membership information. | See section [Dynamic Groups](defaultusersync.html#dynamic_groups)|
| I cannot add members to a synchronized group | The dynamic groups option comes with a dedicated validator that makes external groups read-only | See section [Enforcing dynamic groups](defaultusersync.html#validation) |
| Auto-membership cannot be altered through user management API | The configured auto-membership with local groups is calculated dynamically from the configuration and cannot be changed through user management API | See section [Automatic Membership](dynamic.html) | 
| External groups have no rep:members property | The membership information is computed using an implementation of `DynamicMembershipProvider` computed dynamically from the `rep:externalPrincipalNames` properties stored with external users | See [OAK-9803] | 
| Group nesting is not reflected in the repository | The dynamic sync flattens the nested membership and the `DynamicMembershipProvider` will mark members and membership as declared | See description of [`DynamicSyncContext`](dynamic.html) |

[OAK-4301]: https://issues.apache.org/jira/browse/OAK-4301
[OAK-4397]: https://issues.apache.org/jira/browse/OAK-4397
[OAK-5304]: https://issues.apache.org/jira/browse/OAK-5304
[OAK-9803]: https://issues.apache.org/jira/browse/OAK-9803


