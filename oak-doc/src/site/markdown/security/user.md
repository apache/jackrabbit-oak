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

User Management
--------------------------------------------------------------------------------

### JCR User Management

JCR itself doesn't come with a dedicated user management API. The only method
related and ultemately used for user management tasks is `Session.getUserID()`.
Therefore an API for user and group management has been defined as part of the
extensions present with Jackrabbit API.

### Jackrabbit User Management API

_todo_

### Oak User Management Implementation

The default user management implementation stores user/group information in the
content repository. In contrast to Jackrabbit 2.x, which by default used a single,
dedicated workspace for user/group data, this data will as of Oak 1.0 be stored
separately for each JCR workspace.

Consequently the `UserManager` associated with the editing sessions, performs
all actions with this editing session. This corresponds to the behavior as defined
the alternative implementation present with Jackrabbit 2.x ((see Jackrabbit 2.x `UserPerWorkspaceUserManager`).

#### General
* The Oak implementation is build on the Oak API. This allows for double usage as
  extension to the JCR API as well as within the Oak layer (aka SPI).
* The `UserManager` is always associated with the same JCR workspace as the editing
  `Session` from which the class has been obtained.
* Changes made to the user management API are always transient and require `Session#save()` to be persisted.
* In case of any failure during user management related write operations the API
  consumer is in charge of specifically revert pending or invalid transient modifications
  or calling `Session#refresh(false)`.

#### Differences wrt Jackrabbit 2.x

A summary of all changes with respect to the former implementation present with
Jackrabbit 2.x is present in the corresponding [section](user/differences.html).

#### Built-in Users and Special Groups

The setup of builtin user and group accounts is triggered by the configured `WorkspaceInitializer`
associated with the user management configuration (see Configuration section below).

The default user management implementation in OAK comes with an initializer that
creates the following builtin user accounts:

##### Administrator
The admin user is always being created. The ID of this user is retrieved from the
user configuration parameter `PARAM_ADMIN_ID`, which defaults to `admin`.

As of OAK 1.0 however the administrator user might be created without initial
password forcing the application to set the password upon start (see `PARAM_OMIT_ADMIN_PW`
configuration parameter).

##### Anonymous User
In contrast to Jackrabbit 2.x the anonymous (or guest) user is optional. Creation
will be skipped if the value of the `PARAM_ANONYMOUS_ID` configuration parameter
is `null` or empty.

Note, that the anonymous user will always be created without specifying a password
in order to prevent regular login with `SimpleCredentials`.
The proper way to obtain a guest session is:

    Repository#login(new GuestCredentials(), wspName);

See section [Authentication](authentication.html) for further information about
guest login.

##### Everyone Group

The default user management implementation in Oak contains special handling for
the optional group that represents _everyone_, which is marked by the reserved
name [everyone] and corresponds to the `EveryonePrincipal`.

This special group always contains all Authorizable as member and cannot be edited
with user management API. As of OAK this fact is consistently reflected in all
group membership related methods. See also [Principal Management](principal.html).

#### Reading Authorizables

##### Handling of the Authorizable ID
* As of Oak the node type definition of `rep:Authorizable` defines a new property `rep:authorizableId` which is intended to store the ID of a user or group.
* The default implementation comes with a dedicated property index for `rep:authorizableId` which asserts the uniqueness of that ID.
* `Authorizable#getID` returns the string value contained in `rep:authorizableID` and for backwards compatibility falls back on the node name in case the ID property is missing.
* The name of the authorizable node is generated based on a configurable implementation of the `AuthorizableNodeName` interface (see configuration section below). By default it uses the ID as name hint and includes a conversion to a valid JCR node name.

##### equals() and hashCode()
The implementation of `Object#equals()` and `Object#hashCode()` for user and
groups slightly differs from Jackrabbit 2.x. It no longer relies on the _sameness_
of the underlaying JCR node but only compares IDs and the user manager instance.

#### Creating Authorizables
* The `rep:password` property is no longer defined to be mandatory. Therefore a new user might be created without specifying a password. Note however, that `User#changePassword` does not allow to remove the password property.
* `UserManager#createGroup(Principal)` will no longer generate a groupID in case the principal name collides with an existing user or group ID. This has been considered redundant as the Jackrabbit API in the mean time added `UserManager#createGroup(String groupID)`.
* Since OAK is designed to scale with flat hierarchies the former configuration options `autoExpandTree` and `autoExpandSize` are no longer supported.

#### Query

See section [Searching Users and Groups](user/query.html) for details.

#### Group Membership

See section [Group Membership](user/membership.html) for details.

#### Autosave Behavior
Due to the nature of the UserManager (see above) we decided to drop the auto-save
behavior in the default implementation present with OAK. Consequently,

* `UserManager#autoSave(boolean)` throws `UnsupportedRepositoryOperationException`
* `UserManager#isAutoSave()` always returns `false`

See also `PARAM_SUPPORT_AUTOSAVE` below; while this should not be needed if
application code has been written against the Jackrabbit API (and thus testing if
auto-save mode is enabled or not) this configuration option can be used as last resort.


### User/Group Representation in the Repository

The following block lists the built-in node types related to user management tasks:

    [rep:Authorizable] > mix:referenceable, nt:hierarchyNode
      abstract
      + * (nt:base) = nt:unstructured VERSION
      - rep:principalName  (STRING) protected mandatory
      - rep:authorizableId (STRING) protected /* @since oak 1.0 */
      - * (UNDEFINED)
      - * (UNDEFINED) multiple

    [rep:Group] > rep:Authorizable, rep:MemberReferences
      + rep:members (rep:Members) = rep:Members multiple protected VERSION /* @deprecated */
      + rep:membersList (rep:MemberReferencesList) = rep:MemberReferencesList protected COPY

    /** @since oak 1.0 */
    [rep:MemberReferences]
      - rep:members (WEAKREFERENCE) protected multiple < 'rep:Authorizable'

    /** @since oak 1.0 */
    [rep:MemberReferencesList]
      + * (rep:MemberReferences) = rep:MemberReferences protected COPY

    /** @deprecated since oak 1.0 */
    [rep:Members]
      orderable
      + * (rep:Members) = rep:Members protected multiple
      - * (WEAKREFERENCE) protected < 'rep:Authorizable'


### XML Import
As of Oak 1.0 user and group nodes can be imported both with Session and Workspace
import. Other differences compared to Jackrabbit 2.x:

* Importing an authorizable to another tree than the configured user/group node will only failed upon save (-> see `UserValidator` during the `Root#commit`). With Jackrabbit 2.x core it used to fail immediately.
* The `BestEffort` behavior is now also implemented for the import of impersonators (was missing in Jackrabbit /2.x).

### API Extensions
The Oak project introduces the following user management related public
interfaces and classes:

#### Authorizable Actions

The former internal Jackrabbit interface `AuthorizableAction` has been slightly
adjusted to match Oak requirements and is now part of the public Oak SPI interfaces.
In contrast to Jackrabbit-core the AuthorizableAction(s) now operate directly on
the Oak API, which eases the handling of implementation specific tasks such as
writing protected items.

See section [Authorizable Actions](user/authorizableaction.html) for further
details and examples.

#### Node Name Generation

_todo_

`org.apache.jackrabbit.oak.spi.security.user.*`

- `AuthorizableNodeName` : Defines the generation of the authorizable node names
   in case the user management implementation stores user information in the repository.

#### Utilities

`org.apache.jackrabbit.oak.spi.security.user.*`

- `AuthorizableType` : Ease handling with the different authorizable types.
- `UserConstants` : Constants (NOTE: OAK names/paths)

`org.apache.jackrabbit.oak.spi.security.user.util.*`

- `PasswordUtil` : Utilities for password generation. This utility corresponds
  to the internal jackrabbit utility.
  As of OAK it also supports Password-Based Key Derivation Function 2 (PBKDF2)
  function for password generation.
- `UserUtil` : Utilities related to general user management tasks.


### Configuration

The following configuration options are present with the `UserConfiguration` as of OAK 1.0:

* getUserManager: Obtain a new user manager instance
* getAuthorizableActionProvider: Obtain a new instance of the AuthorizableActionProvider (see above)

#### Configuration Parameters supported by the default implementation

| Parameter                           | Type    | Default                                      |
|-------------------------------------|---------|----------------------------------------------|
| `PARAM_ADMIN_ID`                    | String  | "admin"                                      |
| `PARAM_OMIT_ADMIN_PW`               | boolean | false                                        |
| `PARAM_ANONYMOUS_ID`                | String  | "anonymous" (nullable)                       |
| `PARAM_USER_PATH`                   | String  | "/rep:security/rep:authorizables/rep:users"  |
| `PARAM_GROUP_PATH`                  | String  | "/rep:security/rep:authorizables/rep:groups" |
| `PARAM_DEFAULT_DEPTH`               | int     | 2                                            |
| `PARAM_PASSWORD_HASH_ALGORITHM`     | String  | "SHA-256"                                    |
| `PARAM_PASSWORD_HASH_ITERATIONS`    | int     | 1000                                         |
| `PARAM_PASSWORD_SALT_SIZE`          | int     | 8                                            |
| `PARAM_AUTHORIZABLE_NODE_NAME`      | AuthorizableNodeName | AuthorizableNodeName#DEFAULT    |
| `PARAM_SUPPORT_AUTOSAVE`            | boolean | false                                        |

The following configuration parameters present with the default implementation in Jackrabbit 2.x are no longer supported and will be ignored:

* "compatibleJR16"
* "autoExpandTree"
* "autoExpandSize"
* "groupMembershipSplitSize"

### Further Reading

- [Differences wrt Jackrabbit 2.x](user/differences.html)
- [Group Membership](user/membership.html)
- [Authorizable Actions](user/authorizableaction.html)
- [Searching Users and Groups](user/query.html)

<!-- hidden references -->
[everyone]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.html#NAME
[OAK-118]: https://issues.apache.org/jira/browse/OAK-118
[OAK-482]: https://issues.apache.org/jira/browse/OAK-482
[OAK-793]: https://issues.apache.org/jira/browse/OAK-793
[OAK-949]: https://issues.apache.org/jira/browse/OAK-949
[OAK-1183]: https://issues.apache.org/jira/browse/OAK-1183

