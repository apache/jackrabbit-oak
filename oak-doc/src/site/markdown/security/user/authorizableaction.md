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

Authorizable Actions
--------------------------------------------------------------------------------

### Overview

_todo_

The former internal Jackrabbit interface `AuthorizableAction` has been slightly
adjusted to match OAK requirements and is now part of the public OAK SPI interfaces.
In contrast to Jackrabbit-core the AuthorizableAction(s) now operate directly on
the Oak API, which eases the handling of implementation specific tasks such as
writing protected items.


### Default Actions

The default implementations of the `AuthorizableAction` interface present with
OAK match the implementations available with Jackrabbit 2.x:

* `AccessControlAction`: set up permission for new authorizables
* `PasswordAction`: simplistic password verification upon user creation and password modification
* `PasswordChangeAction`: verifies that the new password is different from the old one
* `ClearMembershipAction`: clear group membership upon removal of an authorizable.

As in jackrabbit core the actions are executed with the editing session and the target operation will fail if any of the configured actions fails (e.g. due to insufficient permissions by the editing OAK ContentSession).

In order to match the OAK repository configuration setup and additional interface AuthorizableActionProvider has been introduced. See section Configuration below.


### Pluggability

_todo_


#### Examples

##### Custom Action Provider

_todo_

##### Custom Action

_todo_

