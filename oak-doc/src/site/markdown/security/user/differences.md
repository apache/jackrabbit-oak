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
### User Management : Differences to Jackrabbit 2.x

The default user management implementation present has the following characteristics
that differ from the default behavior in Jackrabbit 2.x

#### General

* changes made to the user management API are always transient and require `Session#save()` to be persisted.
* In case of a failure `Session#refresh` is no longer called in order to prevent
reverting other changes unrelated to the user management operation. Consequently
it's the responsibility of the API consumer to specifically revert pending or
invalid transient modifications.

#### Differences by Interface

##### UserManager

- stores user/group information in the workspace associated with the editing Session
- the autosave feature is no longer supported by default; configuration option
  `PARAM_SUPPORT_AUTOSAVE` can be used to obtain backwards compatible behavior
- calling `getAuthorizable` with empty id or `null` id/principal will not throw
  a runtime exception but silently returns `null`

##### Authorizable

* Equality and HashCode : the implementation of `Object#equals()` and `Object#hashCode()` for
  authorizables differs from Jackrabbit 2.x. It no longer relies on the _sameness_ of
  the underlaying JCR node but only compares IDs and the user manager instance.
* Authorizable ID: the ID of authorizables is stored separately in a `rep:authorizableId` property.
  This value is returned upon `Authorizable#getID`. For backwards compatibility
  it falls back on the node name in case the ID property is missing.
* Node Name: The name of the authorizable node is generated based on a configurable implementation
  of the `AuthorizableNodeName` interface. Default: ID as name hint.
  See section [Authorizable Node Name Generation](authorizablenodename.html) for details.

##### User

* Creation: The password is no longer mandatory upon user creation.

##### Group

* Creation: `createGroup(Principal)` will no longer generate a groupID in case the principal
  name collides with an existing user or group ID. This has been considered redundant
  as the Jackrabbit API in the mean time added `UserManager#createGroup(String groupID)`.
* Group Members: The way many group members are stored with a given Group has been redesigned
  in Oak 1.0. See section [Group Membership](membership.html) for a detailed description.

<a name="query"/>
##### QueryBuilder

The user query is expected to work as in Jackrabbit 2.x with the following notable
bug fixes:

* `QueryBuilder#setScope(String groupID, boolean declaredOnly)` now also works properly
  for the everyone group (see [OAK-949])
* `QueryBuilder#impersonates(String principalName)` works properly for the admin
  principal which are specially treated in the implementation of the `Impersonation`
  interface (see [OAK-1183]).

#### Additional Functionality

##### XML Import

* Importing an authorizable to another tree than the configured user/group node 
  will only failed upon save (-> see `UserValidator` during the `Root#commit`). 
  With Jackrabbit 2.x core it used to fail immediately.
* The `BestEffort` behavior is now also implemented for the import of impersonators (was missing in Jackrabbit /2.x).
* Oak also supports workspace import for authorizables

##### Built-in Users

- admin user can be initialized without password (`PARAM_OMIT_ADMIN_PW` config option)
- anonymous user is optional (missing `PARAM_ANONYMOUS_ID` config option)
- anonymous user is always initialized without password.

##### Group representing the Everyone Principal

- the implementation of the optional special group representing the [everyone] principal is consistent throughout all group membership related methods.

##### Authorizable Actions
The former internal interface `AuthorizableAction` has been slightly adjusted to
match OAK requirements and is now part of the public OAK SPI interfaces.

See the corresponding [section](authorizableaction.html) for details.

#### Node Type Definitions

The built-in node types related to user management tasks have been modified as follows.

- *rep:Authorizable*
    - new protected property `rep:authorizableId`
- *rep:Group*
    - extends from `rep:MemberReferences` which provides the multivalued property `rep:members`
    - the child node definition `rep:members` has been deprecated and is no longer used
    - new child node definition `rep:membersList`

The following node type definitions have been added:

- *rep:MemberReferences* : provides the multivalued `rep:members` property.
- *rep:MemberReferencesList*

The following node type definition has been deprecated and will no longer be used:

- *rep:Members*

#### Configuration

The following configuration parameters present with the default implementation
in Jackrabbit 2.x are no longer supported and will be ignored:

* "compatibleJR16"
* "autoExpandTree"
* "autoExpandSize"
* "groupMembershipSplitSize"

<!-- hidden references -->

[OAK-949]: https://issues.apache.org/jira/browse/OAK-949
[OAK-1183]: https://issues.apache.org/jira/browse/OAK-1183
[everyone]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.html
