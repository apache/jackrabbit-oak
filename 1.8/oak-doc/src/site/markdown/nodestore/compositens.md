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
# Oak Composite NodeStore

**The documentation and the Composite NodeStore implementation are a work-in-progress. Please ask on oak-dev for things that are missing or unclear.**

## Checking for read-only access

The Composite NodeStore mounts various other node stores in read-only mode. Since the read-only mode
is not enfored via permissions, it may not be queried via `Session.hasPermission`. Instead, the
read-only status is surfaced via `Session.hasCapability`. See [OAK-6563][OAK-6563] for details.

[OAK-6563]: https://issues.apache.org/jira/browse/OAK-6563