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

Oak 1.0 comes with a extension to the Jackrabbit user management API that allows
to perform additional actions or validations upon common user management tasks
such as

- create authorizables
- remove authorizables
- change a user's password

Similar functionality has been present in Jackrabbit 2.x as internal interface.
Compared to the Jackrabbit interface the new `AuthorizableAction` has been slightly
adjusted to match Oak requirements operate directly on the Oak API, which eases
the handling of implementation specific tasks such as writing protected items.

### AuthorizableAction API

The following public interfaces are provided by Oak in the package `org.apache.jackrabbit.oak.spi.security.user.action`:

- [AuthorizableAction]
- [AuthorizableActionProvider]

The `AuthorizableAction` interface itself allows to perform validations or write
additional application specific content while executing user management related
write operations. Therefore these actions are executed as part of the transient 
user management modifications. This contrasts to `org.apache.jackrabbit.oak.spi.commit.CommitHook`s
which in turn are only triggered once modifications are persisted.

Consequently, implementations of the `AuthorizableAction` interface are expected 
to adhere to this rule and perform transient repository operation or validation.
They must not force changes to be persisted by calling `org.apache.jackrabbit.oak.api.Root.commit()`.

See section [Group Actions](groupaction.html) for a related extension to
monitor group specific operations.

### Default Implementations

Oak 1.0 provides the following base implementations:

- `AbstractAuthorizableAction`: abstract base implementation that doesn't perform any action.
- `DefaultAuthorizableActionProvider`: default action provider service that allows to enable the built-in actions provided with oak.
- `CompositeActionProvider`: Allows to aggregate multiple provider implementations.

#### Changes wrt Jackrabbit 2.x

- actions no longer operate on JCR API but rather on the Oak API direct.
- provider interface simplifies pluggability

#### Built-in AuthorizableAction Implementations

The following implementations of the `AuthorizableAction` interface are provided:

* `AccessControlAction`: set up permission for new authorizables
* `PasswordValidationAction`: simplistic password verification upon user creation and password modification
* `PasswordChangeAction`: verifies that the new password is different from the old one
* `ClearMembershipAction`: clear group membership upon removal of an authorizable.

As in Jackrabbit 2.x the actions are executed with the editing session and the
target operation will fail if any of the configured actions fails (e.g. due to
insufficient permissions by the editing Oak ContentSession).

### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`AuthorizableActionProvider` implementations and will automatically combine the
different implementations using the `CompositeActionProvider`.

In an OSGi setup the following steps are required in order to add an action provider
implementation:

- implement `AuthorizableActionProvider` interface exposing your custom action(s).
- make the provider implementation an OSGi service and make it available to the Oak repository.

##### Examples

###### Example Action Provider

    @Component()
    @Service(AuthorizableActionProvider.class)
    public class MyAuthorizableActionProvider implements AuthorizableActionProvider {

        private static final String PUBLIC_PROFILE_NAME = "publicProfileName";
        private static final String PRIVATE_PROFILE_NAME = "privateProfileName";
        private static final String FRIENDS_PROFILE_NAME = "friendsProfileName";

        @Property(name = PUBLIC_PROFILE_NAME, value = "publicProfile")
        private String publicName;

        @Property(name = PRIVATE_PROFILE_NAME, value = "privateProfile")
        private String privateName;

        @Property(name = FRIENDS_PROFILE_NAME, value = "friendsProfile")
        private String friendsName;

        private ConfigurationParameters config = ConfigurationParameters.EMPTY;

        public MyAuthorizableActionProvider() {}

        public MyAuthorizableActionProvider(ConfigurationParameters config) {
            this.config = config;
        }

        //-----------------------------------------< AuthorizableActionProvider >---
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(SecurityProvider securityProvider) {
            AuthorizableAction action = new ProfileAction(publicName, privateName, friendsName);
            action.init(securityProvider, config);
            return Collections.singletonList(action);
        }

        //----------------------------------------------------< SCR Integration >---
        @Activate
        private void activate(Map<String, Object> properties) {
            config = ConfigurationParameters.of(properties);
        }
    }

###### Example Action

This example action generates additional child nodes upon user/group creation
that will later be used to store various target-specific profile information:

    class ProfileAction extends AbstractAuthorizableAction {

        private final String publicName;
        private final String privateName;
        private final String friendsName;

        ProfileAction(@Nullable String publicName, @Nullable String privateName, @Nullable String friendsName) {
            this.publicName = publicName;
            this.privateName = privateName;
            this.friendsName = friendsName;
        }

        @Override
        public void onCreate(Group group, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            createProfileNodes(group.getPath(), root);
        }

        @Override
        public void onCreate(User user, String password, Root root, NamePathMapper namePathMapper) throws RepositoryException {
            createProfileNodes(user.getPath(), root);
        }

        private void createProfileNodes(@Nonnull String authorizablePath, @Nonnull Root root) throws AccessDeniedException {
            Tree tree = root.getTree(authorizablePath);
            if (tree.exists()) {
                NodeUtil authorizableNode = new NodeUtil(tree);
                if (publicName != null) {
                    authorizableNode.addChild(publicName, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                }
                if (privateName != null) {
                    authorizableNode.addChild(privateName, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                }
                if (friendsName != null) {
                    authorizableNode.addChild(friendsName, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                }
            }
        }

###### Example Non-OSGI Setup

    Map<String, Object> userParams = new HashMap<String, Object>();
    userParams.put(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, new MyAuthorizableActionProvider());
    ConfigurationParameters config =  ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams)));
    SecurityProvider securityProvider = new SecurityProviderImpl(config));
    Repository repo = new Jcr(new Oak()).with(securityProvider).createRepository();


<!-- hidden references -->
[AuthorizableAction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/action/AuthorizableAction.html
[AuthorizableActionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/action/AuthorizableActionProvider.html
