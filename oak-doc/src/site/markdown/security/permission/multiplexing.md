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

Multiplexing support in the PermissionStore
--------------------------------------------------------------------------------

### General Notes

Multiplexing support is implemented as a composite `PermissionProvider` made of
the default workspace provider and the existing mounts.
This is available since Oak 1.7.3 [OAK-3777](https://issues.apache.org/jira/browse/OAK-3777).

### PermissionStore Evaluation (reading)

Given the following mount setup

    private
        - /libs
        - /apps
    default
        - /

In above setup nodes under /apps and /libs (include apps and libs) are part of "private" mount (mount name is "private") and all other paths are part of default mount.
A dedicated PermissionStore will be created under `oak:mount-private-default` that contains information relevant to this specific mount.

    /jcr:system/rep:permissionStore
        + oak:mount-private-default
            + editor //principal name
                + 1345610890 (rep:PermissionStore) //path hash
                    - rep:accessControlledPath = /libs
                        + 0
                          - rep:isAllow = false
                          - rep:privileges = [1279]
        + default  //workspace name
            + editor //principal name
                + 1227964008 (rep:PermissionStore) //path hash
                    - rep:accessControlledPath = /content
                        + 0
                          - rep:isAllow = true
                          - rep:privileges = [1279]

### PermissionStore updates (writing)

The `PermissionHook` is now mount-aware and will delegate changes to specific path to their designated components based on path.

