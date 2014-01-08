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
#### 1. Characteristics of the Default Implementation
The default user management implementation present with OAK always stores
user/group information in the workspace associated with the editing Session
(see Jackrabbit 2.x `UserPerWorkspaceUserManager`). The implementation of a user
management variant corresponding to Jackrabbit's default `UserManagerImpl` is
blocked by missing workspace handling (see [OAK-118]). The current user manager
has the following characteristics that differ from the corresponding Jackrabbit implementation:

##### General
* Changes made to the user management API are always transient and require `Session#save()` to be persisted.
* In case of a failure `Session#refresh` is no longer called in order to prevent
reverting other changes unrelated to the user management operation. Consequently
it's the responsibility of the API consumer to specifically revert pending or
invalid transient modifications.
* The implementation is no longer built on top of the JCR API but instead directly
acts on `Tree` and `PropertyState` defined by the OAK API. This move allows to
make use of the user management API within the OAK layer (aka SPI).

##### User/Group Creation
* The `rep:password` property is no longer defined to be mandatory. Therefore a new user might be created without specifying a password. Note however, that `User#changePassword` does not allow to remove the password property.
* `UserManager#createGroup(Principal)` will no longer generate a groupID in case the principal name collides with an existing user or group ID. This has been considered redundant as the Jackrabbit API in the mean time added `UserManager#createGroup(String groupID)`.
* Since OAK is designed to scale with flat hierarchies the former configuration options `autoExpandTree` and `autoExpandSize` are no longer supported.

##### Handling of the Authorizable ID
* As of OAK the node type definition of `rep:Authorizable` defines a new property `rep:authorizableId` which is intended to store the ID of a user or group.
* The default implementation comes with a dedicated property index for `rep:authorizableId` which asserts the uniqueness of that ID.
* `Authorizable#getID` returns the string value contained in `rep:authorizableID` and for backwards compatibility falls back on the node name in case the ID property is missing.
* The name of the authorizable node is generated based on a configurable implementation of the `AuthorizableNodeName` interface (see configuration section below). By default it uses the ID as name hint and includes a conversion to a valid JCR node name.

##### equals() and hashCode() for Authorizables
The implementation of `Object#equals()` and `Object#hashCode()` for user and groups slightly differs from Jackrabbit 2.x. It no longer relies on the _sameness_ of the underlaying JCR node but only compares IDs and the user manager instance.

##### The _everyone_ Group
As in Jackrabbit 2.x the OAK implementation contains special handling for the
optional group corresponding to the `EveryonePrincipal` which always contains
all Authorizable as member. As of OAK this fact is consistently reflected in all
group membership related methods.

##### Query

The user query is expected to work as in Jackrabbit 2.x with the following notable
differences:

* `QueryBuilder#setScope(String groupID, boolean declaredOnly)` now also works properly
  for the everyone group (see [OAK-949])
* `QueryBuilder#impersonates(String principalName)` works properly for the admin
  principal which are specially treated in the implementation of the `Impersonation`
  interface (see [OAK-1183]).

##### Autosave Behavior
Due to the nature of the UserManager (see above) we decided to drop the auto-save
behavior in the default implementation present with OAK. Consequently,

* `UserManager#autoSave(boolean)` throws `UnsupportedRepositoryOperationException`
* `UserManager#isAutoSave()` always returns `false`

See also `PARAM_SUPPORT_AUTOSAVE` below; while this should not be needed if
application code has been written against the Jackrabbit API (and thus testing if
auto-save mode is enabled or not) this configuration option can be used as last resort.

##### XML Import
As of OAK 1.0 user and group nodes can be imported both with Session and Workspace
import. The difference compare to Jackrabbit 2.x are listed below:

* Importing an authorizable to another tree than the configured user/group node will only failed upon save (-> see `UserValidator` during the `Root#commit`). With Jackrabbit 2.x core it used to fail immediately.
* NEW: The `BestEffort` behavior is now also implemented for the import of impersonators (was missing in Jackrabbit /2.x).
* NEW: Workspace Import

##### Group Membership

###### Behavior in Jackrabbit 2.x
With the default configuration Jackrabbit 2.x stores the group members as
_weak references_ in a `rep:members` multi value property in the group node.
If the `groupMembershipSplitSize` configuration parameter is set and valid,
the group memberships are collected in a node structure below `rep:members` instead
of the default multi valued property. Its value determines the maximum number of
member properties until additional intermediate nodes are inserted. Valid parameter
values are integers > 4. The node structure is a balanced b-tree where only the
leave nodes carry the actual values in residual properties which name is the
principal name of the member.

###### Behavior as of OAK 1.0
As of Oak the user manager automatically chooses an appropriate storage structure
depending on the number of group members. If the number of members is low they
are stored as _weak references_ in a `rep:members` multi value property. This is
similar to Jackrabbit 2.x. If the number of members is high the user manager
will limit the size of the multi value properties and create overflow 
`rep:MemberReferences` nodes below a `rep:membersList` node to hold the extra members.

###### Relevant new and modified node types
    [rep:Group] > rep:Authorizable, rep:MemberReferences
      + rep:members (rep:Members) = rep:Members multiple protected VERSION /* @deprecated */
      + rep:membersList (rep:MemberReferencesList) = rep:MemberReferencesList protected COPY
    
    [rep:MemberReferences]
      - rep:members (WEAKREFERENCE) protected multiple < 'rep:Authorizable'

    [rep:MemberReferencesList]
      + * (rep:MemberReferences) = rep:MemberReferences protected COPY

###### Example Group with few members
*(irrelevant properties excluded)*

    {
        "jcr:primaryType": "rep:Group",
        "rep:principalName": "contributor",
        "rep:members": [
            "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
            "ca58c408-fe06-357e-953c-2d23ffe1e096",
            "3ebb1c04-76dd-317e-a9ee-5164182bc390",
            "d3c827d3-4db2-30cc-9c41-0ed8117dbaff",
            "f5777a0b-a933-3b4d-9405-613d8bc39cc7",
            "fdd1547a-b19a-3154-90da-1eae8c2c3504",
            "65c3084e-abfc-3719-8223-72c6cb9a3d6f"
        ]
    }

###### Example Group with many members
*(irrelevant properties excluded)*

    {
        "jcr:primaryType": "rep:Group",
        "rep:principalName": "employees",
        "rep:membersList": {
            "jcr:primaryType": "rep:MemberReferencesList",
            "0": {
                "jcr:primaryType": "rep:MemberReferences",
                "rep:members": [
                    "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
                    "ca58c408-fe06-357e-953c-2d23ffe1e096",
                    ...
                ]
            },
            ...
            "341": {
                "jcr:primaryType": "rep:MemberReferences",
                "rep:members": [
                    "fdd1547a-b19a-3154-90da-1eae8c2c3504",
                    "65c3084e-abfc-3719-8223-72c6cb9a3d6f",
                    ...
                ]
            }
        }
    }

*Note*: The exact threshold value that determines the storage strategy is an implementation detail and might even vary depending on the underlying persistence layer.

###### Upgrading Groups from Jackrabbit 2.x to OAK content structure

Upon upgrade from a Jackrabbit 2.x repository to OAK the group member lists that
adjusted to reflect the new content structure as created by the OAK user management
implementation; this means that the group member node structure will be changed
by the migration process. Applications that rely on these implementation
details of Jackrabbit 2.x user management instead of use the corresponding
API calls will need to be modified accordingly.

###### Importing Group Members
Importing group members through the import methods in `javax.jcr.Session` or
`javax.jcr.Workspace` is storage agnostic and supports both, property based and
node based, strategies and is backward compatible to content exported from
Jackrabbit 2.x. The group member lists that are modified during an import are
internally processed using the normal user manager APIs. This implies that the
node structure after the import might not be the same as the one represented in the input.

#### 2. Builtin Users
The setup of builtin user and group accounts is triggered by the configured `WorkspaceInitializer` associated with the user management configuration (see Configuration section below). 

The default user management implementation in OAK comes with an initializer that creates the following builtin user accounts (as in Jackrabbit 2.x):

##### Administrator User
The admin user is always being created. The ID of this user is retrieved from the user configuration parameter `PARAM_ADMIN_ID`, which defaults to `admin`.
 
As of OAK 1.0 however the administrator user might be created without initial password forcing the application to set the password upon start (see `PARAM_OMIT_ADMIN_PW` configuration parameter).

##### Anonymous User
In contrast to Jackrabbit 2.x the anonymous (or guest) user is optional. Creation will be skipped if the value of the `PARAM_ANONYMOUS_ID` configuration parameter is `null` or empty.
 
Note, that the anonymous user will always be created without specifying a password in order to prevent login with SimpleCredentials. 

The proper way to obtain a guest session is (see also [OAK-793]):

    Repository#login(new GuestCredentials(), wspName);

#### 3. Authorizable Actions
The former internal interface `AuthorizableAction` has been slightly adjusted to match OAK requirements and is now part of the public OAK SPI interfaces. In contrast to Jackrabbit-core the AuthorizableAction(s) now operate directly on the OAK API which eases the handling of implementation specific tasks such as writing protected items.

The example implementations of the `AuthorizableAction` interface present with OAK match the implementations available with Jackrabbit 2.x:

* `AccessControlAction`: set up permission for new authorizables
* `PasswordAction`: simplistic password verification upon user creation and password modification
* `PasswordChangeAction`: verifies that the new password is different from the old one
* `ClearMembershipAction`: clear group membership upon removal of an authorizable.

As in jackrabbit core the actions are executed with the editing session and the target operation will fail if any of the configured actions fails (e.g. due to insufficient permissions by the editing OAK ContentSession).

In order to match the OAK repository configuration setup and additional interface AuthorizableActionProvider has been introduced. See section Configuration below.

#### 4. Node Type Definitions
The built-in node types related to user management tasks have been modified as follows:

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

#### 5. API Extensions
The OAK project introduces the following user management related public
interfaces and classes:

`org.apache.jackrabbit.oak.spi.security.user.*`

- `AuthorizableNodeName` : Defines the generation of the authorizable node names
   in case the user management implementation stores user information in the repository.
- `AuthorizableType` : Ease handling with the different authorizable types.
- `UserConstants` : Constants (NOTE: OAK names/paths)

`org.apache.jackrabbit.oak.spi.security.user.action.*`

- `AuthorizableAction` : (see above)
- `AuthorizableActionProvider`  : (see above)

`org.apache.jackrabbit.oak.spi.security.user.util.*`

- `PasswordUtil` : Utilities for password generation. This utility corresponds
  to the internal jackrabbit utility.
  As of OAK it also supports Password-Based Key Derivation Function 2 (PBKDF2)
  function for password generation.
- `UserUtil` : Utilities related to general user management tasks.

#### 6. Configuration

The following configuration options are present with the `UserConfiguration` as of OAK 1.0:

* getUserManager: Obtain a new user manager instance
* getAuthorizableActionProvider: Obtain a new instance of the AuthorizableActionProvider (see above)

##### Configuration Parameters supported by the default implementation

| Parameter                           | Type    | Default                                      |
|-------------------------------------|---------|----------------------------------------------|
| `PARAM_ADMIN_ID`                    | String  | "admin"                                      |
| `PARAM_OMIT_ADMIN_PW`               | boolean | false                                        |
| `PARAM_ANONYMOUS_ID`                | String  | "anonymous" (nullable)                       |
| `PARAM_USER_PATH`                   | String  | "/rep:security/rep:authorizables/rep:users"  |
| `PARAM_GROUP_PATH`                  | String  | "/rep:security/rep:authorizables/rep:groups" |
| `PARAM_DEFAULT_DEPTH`               | int     | 2                                            |
| `PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE` | int     |                                              |
| `PARAM_PASSWORD_HASH_ALGORITHM`     | String  | "SHA-256"                                    |
| `PARAM_PASSWORD_HASH_ITERATIONS`    | int     | 1000                                         |
| `PARAM_PASSWORD_SALT_SIZE`          | int     | 8                                            |
| `PARAM_AUTHORIZABLE_NODE_NAME`      | AuthorizableNodeName | AuthorizableNodeName#DEFAULT    |
| `PARAM_SUPPORT_AUTOSAVE`            | boolean | false                                        |

The following configuration parameters present with the default implementation in Jackrabbit 2.x are no longer supported and will be ignored:

* "compatibleJR16"
* "autoExpandTree"
* "autoExpandSize"

<!-- hidden references -->
[OAK-118]: https://issues.apache.org/jira/browse/OAK-118
[OAK-482]: https://issues.apache.org/jira/browse/OAK-482
[OAK-793]: https://issues.apache.org/jira/browse/OAK-793
[OAK-949]: https://issues.apache.org/jira/browse/OAK-949
[OAK-1183]: https://issues.apache.org/jira/browse/OAK-1183