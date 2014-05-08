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

Searching Users and Groups
--------------------------------------------------------------------------------

_todo_

### Lookup a Single Authorizable

#### Lookup by Id

_todo_

#### Lookup by Path

_todo_


#### Lookup by Principal

_todo_

### Searching for Authorizables : The Query API

_todo_

- simple search by property
- query api


### Characteristics of the Default Implementation

#### Changes wrt Jackrabbit 2.x

The user query is expected to work as in Jackrabbit 2.x with the following notable
bug fixes:

* `QueryBuilder#setScope(String groupID, boolean declaredOnly)` now also works properly
  for the everyone group (see [OAK-949])
* `QueryBuilder#impersonates(String principalName)` works properly for the admin
  principal which are specially treated in the implementation of the `Impersonation`
  interface (see [OAK-1183]).



<!-- hidden references -->
[OAK-949]: https://issues.apache.org/jira/browse/OAK-949
[OAK-1183]: https://issues.apache.org/jira/browse/OAK-1183