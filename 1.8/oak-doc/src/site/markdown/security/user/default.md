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

User Management : The Default Implementation
--------------------------------------------------------------------------------

### General Notes

The default user management implementation stores user/group information in the
content repository. In contrast to Jackrabbit 2.x, which by default used a single,
dedicated workspace for user/group data, this data will as of Oak 1.0 be stored
separately for each JCR workspace.

Consequently the `UserManager` associated with the editing sessions, performs
all actions with this editing session. This corresponds to the behavior as defined
the alternative implementation present with Jackrabbit 2.x ((see Jackrabbit 2.x `UserPerWorkspaceUserManager`).

* The Oak implementation is build on the Oak API. This allows for double usage as
  extension to the JCR API as well as within the Oak layer (aka SPI).
* The `UserManager` is always associated with the same JCR workspace as the editing
  `Session` from which the class has been obtained.
* Changes made to the user management API are always transient and require `Session#save()` to be persisted.
* In case of any failure during user management related write operations the API
  consumer is in charge of specifically revert pending or invalid transient modifications
  or calling `Session#refresh(false)`.

### Differences wrt Jackrabbit 2.x

A summary of all changes with respect to the former implementation present with
Jackrabbit 2.x is present in the corresponding [section](differences.html).

### Built-in Users and Special Groups

The setup of builtin user and group accounts is triggered by the configured `WorkspaceInitializer`
associated with the user management configuration (see Configuration section below).

The default user management implementation in OAK comes with an initializer that
creates the following builtin user accounts:

#### Administrator
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

See section [Authentication](../authentication.html) for further information about
guest login.

#### Everyone Group

The default user management implementation in Oak contains special handling for
the optional group that represents the [everyone] principal, which is marked by 
the reserved principal name and by definition has all other principals as members.

This special group always contains all users/groups as member and cannot be edited
with user management API. As of OAK this fact is consistently reflected in all
group membership related methods. See also [Principal Management](../principal.html).

### User Management Operations

#### Reading Authorizables

##### Handling of the Authorizable ID
* As of Oak 1.0 the node type definition of `rep:Authorizable` defines a new property `rep:authorizableId` which is intended to store the ID of a user or group.
* This property is protected and system maintained and cannot be changed after creation through user management API calls.
* The default implementation comes with a dedicated property index for `rep:authorizableId` which asserts the uniqueness of that ID.
* For backwards compatibility with Jackrabbit 2.x the ID specified during creation is also reflected in the `jcr:uuid` (protected and mandatory), which is used for the lookup. 
* `Authorizable#getID` returns the string value contained in `rep:authorizableID` and for backwards compatibility falls back on the node name in case the `rep:authorizableId` property is missing.
* The name of the authorizable node is generated based on a configurable implementation of the `AuthorizableNodeName` interface (see configuration section below). By default it uses the ID as name hint and includes a conversion to a valid JCR node name.

##### equals() and hashCode()
The implementation of `Object#equals()` and `Object#hashCode()` for user and
groups slightly differs from Jackrabbit 2.x. It no longer relies on the _sameness_
of the underlaying JCR node but only compares IDs and the user manager instance.

#### Creating Authorizables
* The `rep:password` property is no longer defined to be mandatory. Therefore a new user might be created without specifying a password. Note however, that `User#changePassword` does not allow to remove the password property.
* Since version 1.1.0 Oak supports the new API to create dedicated system users [JCR-3802](https://issues.apache.org/jira/browse/JCR-3802).

<a name="query"/>
#### Searching

#### XPathQueryBuilder

Oak 1.0 comes with a default XPATH based implementation of the `QueryBuilder`
interface which is passed to the query upon calling `UserManager#findAuthorizables(Query)`.

#### Group Membership

See section [Group Membership](membership.html) for details.

#### Autosave Behavior
Due to the nature of the UserManager (see above) we decided to drop the auto-save
behavior in the default implementation present with OAK. Consequently,

* `UserManager#autoSave(boolean)` throws `UnsupportedRepositoryOperationException`
* `UserManager#isAutoSave()` always returns `false`

See also `PARAM_SUPPORT_AUTOSAVE` below; while this should not be needed if
application code has been written against the Jackrabbit API (and thus testing if
auto-save mode is enabled or not) this configuration option can be used as last resort.

#### XML Import
As of Oak 1.0 user and group nodes can be imported both with Session and Workspace
import. Other differences compared to Jackrabbit 2.x:

* Importing an authorizable to another tree than the configured user/group node will only failed upon save (-> see `UserValidator` during the `Root#commit`). With Jackrabbit 2.x core it used to fail immediately.
* The `BestEffort` behavior is now also implemented for the import of impersonators (was missing in Jackrabbit /2.x).

#### Password Expiry and Force Initial Password Change

Since Oak 1.1.0 the default user management and authentication implementation
provides password expiry and initial password change.

By default these features are disabled. See section [Password Expiry and Force Initial Password Change](expiry.html)
for details.

#### Password History

Since Oak 1.3.3 the default user management implementation provides password
history support. By default this feature is disabled.

See section [Password History](history.html) for details.

<a name="representation"/>
### Representation in the Repository

The following block lists the built-in node types related to user management tasks:
  
    [rep:Authorizable] > mix:referenceable, nt:hierarchyNode
      abstract
      + * (nt:base) = nt:unstructured VERSION
      - rep:principalName  (STRING) protected mandatory
      - rep:authorizableId (STRING) protected /* @since oak 1.0 */
      - * (UNDEFINED)
      - * (UNDEFINED) multiple
    
    [rep:User] > rep:Authorizable, rep:Impersonatable
      + rep:pwd (rep:Password) = rep:Password protected /* @since oak 1.1.0 */
      - rep:password (STRING) protected
      - rep:disabled (STRING) protected
    
    [rep:SystemUser] > rep:User /* @since oak 1.1.0 */
    
    [rep:Impersonatable]
      mixin
      - rep:impersonators (STRING) protected multiple
        
    /* @since oak 1.1.0 */
    [rep:Password]
      - * (UNDEFINED) protected
      - * (UNDEFINED) protected multiple
    
    [rep:Group] > rep:Authorizable, rep:MemberReferences
      + rep:members (rep:Members) = rep:Members multiple protected VERSION /* @deprecated since oak 1.0 */
      + rep:membersList (rep:MemberReferencesList) = rep:MemberReferencesList protected COPY /* @since oak 1.0 */
    
    
    [rep:AuthorizableFolder] > nt:hierarchyNode
      + * (rep:Authorizable) = rep:User VERSION
      + * (rep:AuthorizableFolder) = rep:AuthorizableFolder VERSION    
    
    /* @since oak 1.0 */
    [rep:MemberReferences]
      - rep:members (WEAKREFERENCE) protected multiple < 'rep:Authorizable'
    
    /* @since oak 1.0 */
    [rep:MemberReferencesList]
      + * (rep:MemberReferences) = rep:MemberReferences protected COPY
      
    /* @deprecated since oak 1.0 */
    [rep:Members]
      orderable
      + * (rep:Members) = rep:Members protected multiple
      - * (WEAKREFERENCE) protected < 'rep:Authorizable'

<a name="validation"/>
### Validation

The consistency of this content structure is asserted by a dedicated `UserValidator`.
The corresponding errors are all of type `Constraint` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0020              | Admin user cannot be disabled                            |
| 0021              | Invalid jcr:uuid for authorizable (creation)             |
| 0022              | Changing Id, principal name after creation               |
| 0023              | Invalid jcr:uuid for authorizable (mod)                  |
| 0024              | Password may not be plain text                           |
| 0025              | Attempt to remove id, principalname or pw                |
| 0026              | Mandatory property rep:principalName missing             |
| 0027              | The admin user cannot be removed                         |
| 0028              | Attempt to create outside of configured scope            |
| 0029              | Intermediate folders not rep:AuthorizableFolder          |
| 0030              | Missing uuid for group (check for cyclic membership)     |
| <s>0031</s>        | <s>Cyclic group membership</s> (see [OAK-6072])         |
| 0032              | Attempt to set password with system user                 |
| 0033              | Attempt to add rep:pwd node to a system user             |

<a name="configuration"/>
### Configuration

The following user management specific methods are present with the [UserConfiguration]
as of OAK 1.0:

* getUserManager: Obtain a new user manager instance

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
| `PARAM_AUTHORIZABLE_ACTION_PROVIDER`| AuthorizableActionProvider | DefaultAuthorizableActionProvider |
| `PARAM_SUPPORT_AUTOSAVE`            | boolean | false                                        |
| `PARAM_IMPORT_BEHAVIOR`             | String ("abort", "ignore", "besteffort") | "ignore"    |
| `PARAM_PASSWORD_MAX_AGE`            | int     | 0                                            |
| `PARAM_PASSWORD_INITIAL_CHANGE`     | boolean | false                                        |
| `PARAM_PASSWORD_HISTORY_SIZE`       | int (upper limit: 1000) | 0                            |
| `PARAM_CACHE_EXPIRATION`            | long    | 0                                            |
| `PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE`| boolean | false                              |
| | | |

The following configuration parameters present with the default implementation in Jackrabbit 2.x are no longer supported and will be ignored:

* `compatibleJR16`
* `autoExpandTree`
* `autoExpandSize`
* `groupMembershipSplitSize`

The optional `cacheExpiration` configuration option listed above is discussed in
detail in section [Caching Results of Principal Resolution](../principal/cache.html).
It is not related to user management s.str. but affects the implementation
specific `PrincipalProvider` implementation exposed by `UserConfiguration.getUserPrincipalProvider`.

<a name="pluggability"/>
### Pluggability

Within the default user management implementation the following parts can be
modified or extended at runtime by providing corresponding OSGi services or passing
appropriate configuration parameters exposing the custom implementations:

- `AuthorizableActionProvider`: Defines the authorizable actions, see [Authorizable Actions](authorizableaction.html).
- `AuthorizableNodeName`: Defines the generation of the authorizable node names
  in case the user management implementation stores user information in the repository.
  See [Authorizable Node Name Generation](authorizablenodename.html).
- `UserAuthenticationFactory`: see below
   
#### UserAuthenticationFactory : Authenticating Users

Since Oak 1.1.5 the default user management implementation allows to configure
and thus replace the default [UserAuthenticationFactory], which links the user
management implementation with the authentication (specifically the [uid/pw-login](../authentication/default.html#user_authentication))
as it exposes the [Authentication] implementation to be used for verification
of the specified credentials according to details provided by a given user management
implementation.  
   
##### Examples

###### Example UserAuthenticationFactory

    @Component()
    @Service(UserAuthenticationFactory.class)
    public class MyUserAuthenticationFactory implements UserAuthenticationFactory {

        private Set<String> ids = Collections.emptySet();

        public MyUserAuthenticationFactory() {}

        //--------------------------------------< UserAuthenticationFactory >---
        @Override
        Authentication getAuthentication(@Nonnull UserConfiguration configuration, @Nonnull Root root, @Nullable String userId) {
            final boolean canAuthenticate = (userId != null && ids.contains(userId));
            return new Authentication() {
                @Override
                public boolean authenticate(@Nullable Credentials credentials) {
                    return canAuthenticate;
                }
            };
        }

        //------------------------------------------------< SCR Integration >---
        @Activate
        private void activate(Map<String, Object> properties) {
             ids = ImmutableSet.copyOf(PropertiesUtil.toStringArray(properties.get("ids"), new String[0]));
        }
    }


<!-- hidden references -->
[everyone]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.html#NAME
[UserConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserConfiguration.html
[UserAuthenticationFactory]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserAuthenticationFactory.html
[Authentication]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/Authentication.html
[OAK-6072]: https://issues.apache.org/jira/browse/OAK-6072
