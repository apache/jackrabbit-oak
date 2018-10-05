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

Group Actions
-------------

### Overview

Oak 1.6 comes with an extension to the Jackrabbit user management API that allows
to perform additional actions or validations upon group member management tasks
such as

- add an authorizable to a group
- remove an authorizable from a group
- add a set of member ids as members of a group
- remove a set of member ids from a group

### GroupAction API

The following public interface is provided by Oak in the package `org.apache.jackrabbit.oak.spi.security.user.action`:

- [GroupAction]

The `GroupAction` interface extends from `AuthorizableAction` and itself allows to perform validations or write
additional application specific content while executing group member management related
write operations. Therefore these actions are executed as part of the transient 
user management modifications. This contrasts to `org.apache.jackrabbit.oak.spi.commit.CommitHook`s
which in turn are only triggered once modifications are persisted.

Consequently, implementations of the `GroupAction` interface are expected 
to adhere to this rule and perform transient repository operations or validation.
They must not force changes to be persisted by calling `org.apache.jackrabbit.oak.api.Root.commit()`.

Any group actions are executed with the editing session and the
target operation will fail if any of the configured actions fails (e.g. due to
insufficient permissions by the editing Oak ContentSession).

### Default Implementations

Oak 1.5 provides the following base implementation for `GroupAction` implementations to build upon:

- `AbstractGroupAction`: abstract base implementation that doesn't perform any action.

### Pluggability

Refer to [Authorizable Actions | Pluggability ](authorizableaction.html#Pluggability) for details on how to plug
a new group action into the system.

### XML Import

During import the group actions are called in the same fashion as for regular groups as long as the member reference
can be resolved to an existing authorizable. Member IDs of authorizables that do not exist at group import time  or
failed member IDs are passed to the group actions if `ImportBehavior.BESTEFFORT` is set for the import.

##### Examples

###### Example Action

This example action creates or removes asset home directories for members
added to or removed from a specific group:

    public class CreateHomeForMemberGroupAction extends AbstractGroupAction {
    
        private static final String GROUP_ID = "asset-editors";
        private static final String ASSET_ROOT = "/content/assets";
        private SecurityProvider securityProvider;
    
        @Override
        public void init(SecurityProvider securityProvider, ConfigurationParameters config) {
            this.securityProvider = securityProvider;
        }
    
        @Override
        public void onMemberAdded(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            createHome(group, root, member.getID(), namePathMapper);
        }
    
        @Override
        public void onMembersAdded(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            createHome(group, root, memberIds, failedIds, namePathMapper);
        }
    
        @Override
        public void onMemberRemoved(@Nonnull Group group, @Nonnull Authorizable member, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            removeHome(group, root, member.getID(), namePathMapper);
        }
    
        @Override
        public void onMembersRemoved(@Nonnull Group group, @Nonnull Iterable<String> memberIds, @Nonnull Iterable<String> failedIds, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            removeHome(group, root, memberIds, failedIds, namePathMapper);
        }
    
        private void createHome(Group group, Root root, String memberId, NamePathMapper namePathMapper) throws RepositoryException {
            createHome(group, root, Lists.newArrayList(memberId), Lists.<String>newArrayList(), namePathMapper);
        }
    
        private void createHome(Group group, Root root, Iterable<String> memberIds, Iterable<String> failedIds, NamePathMapper namePathMapper) throws RepositoryException {
            if (GROUP_ID.equals(group.getID())) {
                UserManager userManager = securityProvider.getConfiguration(UserConfiguration.class).getUserManager(root, namePathMapper);
                for (String memberId : memberIds) {
                    Authorizable authorizable = userManager.getAuthorizable(memberId);
                    if (authorizable != null && !authorizable.isGroup()) {
                        // Note: this is done with the editing session of the group modification and may not
                        // be the desired session / privilege level with which to perform these actions.
                        NodeUtil assetRoot = new NodeUtil(root.getTree(ASSET_ROOT));
                        NodeUtil home = assetRoot.addChild(memberId, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                        // ...
                    }
                }
            }
        }
    
        private void removeHome(Group group, Root root, String memberId, NamePathMapper namePathMapper) {
            removeHome(group, root, Lists.newArrayList(memberId), Lists.<String>newArrayList(), namePathMapper);
        }
    
        private void removeHome(Group group, Root root, Iterable<String> memberIds, Iterable<String> failedIds, NamePathMapper namePathMapper) {
    
        }
    }

<!-- hidden references -->
[GroupAction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/action/GroupAction.html
