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

External Authentication : FAQ
-----------------------------

| Question          | Answer                        | References        |
|-------------------|-------------------------------|-------------------|
| Why am I no longer able to change the `rep:externalId`?               | Since Oak 1.5.8 the default sync mechanism properly protects the system maintained property `rep:externalId` which is used to link a given synced user/group account to the corresponding entry on the external IDP. | See [documentation](defaultusersync.html) and [OAK-4301] |
| Why does a User or Group created with a content package not get synced with the IDP? | Only users/groups with a `rep:externalId` linking them to the external IDP will be respected during the default sync mechanism. | See also [OAK-4397] and [OAK-5304] |




[OAK-4301]: https://issues.apache.org/jira/browse/OAK-4301
[OAK-4397]: https://issues.apache.org/jira/browse/OAK-4397
[OAK-5304]: https://issues.apache.org/jira/browse/OAK-5304


