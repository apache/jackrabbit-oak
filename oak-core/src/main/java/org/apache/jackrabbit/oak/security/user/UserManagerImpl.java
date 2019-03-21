/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.user;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.security.user.query.UserQueryManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.apache.jackrabbit.oak.spi.security.user.action.UserAction;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final Root root;
    private final PartialValueFactory valueFactory;
    private final NamePathMapper namePathMapper;
    private final SecurityProvider securityProvider;

    private final UserProvider userProvider;
    private final MembershipProvider membershipProvider;
    private final ConfigurationParameters config;
    private final AuthorizableActionProvider actionProvider;

    private UserQueryManager queryManager;
    private ReadOnlyNodeTypeManager ntMgr;

    public UserManagerImpl(@NotNull Root root,
                           @NotNull PartialValueFactory valueFactory,
                           @NotNull SecurityProvider securityProvider) {
        this.root = root;
        this.valueFactory = valueFactory;
        this.namePathMapper = valueFactory.getNamePathMapper();
        this.securityProvider = securityProvider;

        UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class);
        this.config = uc.getParameters();
        this.userProvider = new UserProvider(root, config);
        this.membershipProvider = new MembershipProvider(root, config);
        this.actionProvider = getActionProvider(config);
    }

    @NotNull
    private static AuthorizableActionProvider getActionProvider(@NotNull ConfigurationParameters config) {
        AuthorizableActionProvider actionProvider = config.getConfigValue(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, null, AuthorizableActionProvider.class);
        if (actionProvider == null) {
            actionProvider = new DefaultAuthorizableActionProvider(config);
        }
        return actionProvider;
    }

    //--------------------------------------------------------< UserManager >---
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        Authorizable authorizable = null;
        Tree tree = (Strings.isNullOrEmpty(id)) ? null : userProvider.getAuthorizable(id);
        if (tree != null) {
            authorizable = getAuthorizable(UserUtil.getAuthorizableId(tree), tree);
        }
        return authorizable;
    }

    @Override
    public <T extends Authorizable> T getAuthorizable(String id, Class<T> authorizableClass) throws RepositoryException {
        return UserUtil.castAuthorizable(getAuthorizable(id), authorizableClass);
    }

    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        return (principal == null) ? null : getAuthorizable(userProvider.getAuthorizableByPrincipal(principal));
    }

    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        String oakPath = namePathMapper.getOakPath(path);
        if (oakPath == null) {
            throw new RepositoryException("Invalid path " + path);
        }
        return getAuthorizableByOakPath(oakPath);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        return findAuthorizables(relPath, value, SEARCH_TYPE_AUTHORIZABLE);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        return getQueryManager().findAuthorizables(relPath, value, AuthorizableType.getType(searchType));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        return getQueryManager().findAuthorizables(query);
    }

    @Override
    public User createUser(final String userID, String password) throws RepositoryException {
        Principal principal = new PrincipalImpl(userID);
        return createUser(userID, password, principal, null);
    }

    @Override
    public User createUser(String userID, String password, Principal principal,
                           @Nullable String intermediatePath) throws RepositoryException {
        checkValidId(userID);
        checkValidPrincipal(principal, false);

        if (intermediatePath != null) {
            intermediatePath = namePathMapper.getOakPath(intermediatePath);
        }
        Tree userTree = userProvider.createUser(userID, intermediatePath);
        setPrincipal(userTree, principal);
        if (password != null) {
            setPassword(userTree, userID, password, true);
        }

        User user = new UserImpl(userID, userTree, this);
        onCreate(user, password);

        log.debug("User created: " + userID);
        return user;
    }

    @Override
    public User createSystemUser(String userID, String intermediatePath) throws RepositoryException {
        checkValidId(userID);
        Principal principal = new PrincipalImpl(userID);
        checkValidPrincipal(principal, false);

        Tree userTree = userProvider.createSystemUser(userID, intermediatePath);
        setPrincipal(userTree, principal);

        User user = new SystemUserImpl(userID, userTree, this);

        log.debug("System user created: " + userID);
        return user;
    }

    @Override
    public Group createGroup(String groupId) throws RepositoryException {
        Principal principal = new PrincipalImpl(groupId);
        return createGroup(groupId, principal, null);
    }

    @Override
    public Group createGroup(Principal principal) throws RepositoryException {
        return createGroup(principal, null);
    }

    @Override
    public Group createGroup(Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        return createGroup(principal.getName(), principal, intermediatePath);
    }

    @Override
    public Group createGroup(String groupID, Principal principal, @Nullable String intermediatePath) throws RepositoryException {
        checkValidId(groupID);
        checkValidPrincipal(principal, true);

        if (intermediatePath != null) {
            intermediatePath = namePathMapper.getOakPath(intermediatePath);
        }
        Tree groupTree = userProvider.createGroup(groupID, intermediatePath);
        setPrincipal(groupTree, principal);

        Group group = new GroupImpl(groupID, groupTree, this);
        onCreate(group);

        log.debug("Group created: " + groupID);
        return group;
    }

    /**
     * Always returns {@code false}. Any modifications made to this user
     * manager instance require a subsequent call to {@link javax.jcr.Session#save()}
     * in order to have the changes persisted.
     */
    @Override
    public boolean isAutoSave() {
        return false;
    }

    /**
     * Changing the auto-save behavior is not supported by this implementation
     * and this method always throws {@code UnsupportedRepositoryOperationException}
     */
    @Override
    public void autoSave(boolean enable) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Session#save() is always required.");
    }


    //--------------------------------------------------------------------------

    /**
     * Let the configured {@code AuthorizableAction}s perform additional
     * tasks associated with the creation of the new user before the
     * corresponding new node is persisted.
     *
     * @param user     The new user.
     * @param password The password.
     * @throws RepositoryException If an exception occurs.
     */
    void onCreate(@NotNull User user, @Nullable String password) throws RepositoryException {
        if (!user.isSystemUser()) {
            for (AuthorizableAction action : actionProvider.getAuthorizableActions(securityProvider)) {
                action.onCreate(user, password, root, namePathMapper);
            }
        } else {
            log.debug("Omit onCreate action for system users.");
        }
    }

    /**
     * Let the configured {@code AuthorizableAction}s perform additional
     * tasks associated with the creation of the new group before the
     * corresponding new node is persisted.
     *
     * @param group The new group.
     * @throws RepositoryException If an exception occurs.
     */
    void onCreate(@NotNull Group group) throws RepositoryException {
        for (AuthorizableAction action : actionProvider.getAuthorizableActions(securityProvider)) {
            action.onCreate(group, root, namePathMapper);
        }
    }

    /**
     * Let the configured {@code AuthorizableAction}s perform any clean
     * up tasks related to the authorizable removal (before the corresponding
     * node gets removed).
     *
     * @param authorizable The authorizable to be removed.
     * @throws RepositoryException If an exception occurs.
     */
    void onRemove(@NotNull Authorizable authorizable) throws RepositoryException {
        for (AuthorizableAction action : actionProvider.getAuthorizableActions(securityProvider)) {
            action.onRemove(authorizable, root, namePathMapper);
        }
    }

    /**
     * Let the configured {@code AuthorizableAction}s perform additional
     * tasks associated with password changing of a given user before the
     * corresponding property is being changed.
     *
     * @param user     The target user.
     * @param password The new password.
     * @throws RepositoryException If an exception occurs.
     */
    void onPasswordChange(@NotNull User user, @NotNull String password) throws RepositoryException {
        for (AuthorizableAction action : actionProvider.getAuthorizableActions(securityProvider)) {
            action.onPasswordChange(user, password, root, namePathMapper);
        }
    }

    void onDisable(@NotNull User user, @Nullable String disableReason) throws RepositoryException {
        for (UserAction action : filterUserActions()) {
            action.onDisable(user, disableReason, root, namePathMapper);
        }
    }

    void onImpersonation(@NotNull User user, @NotNull Principal principal, boolean granting) throws RepositoryException {
        for (UserAction action : filterUserActions()) {
            if (granting) {
                action.onGrantImpersonation(user, principal, root, namePathMapper);
            } else {
                action.onRevokeImpersonation(user, principal, root, namePathMapper);
            }
        }
    }

    /**
     * Upon a group being updated (single {@code Authorizable} successfully added or removed),
     * call available {@code GroupAction}s and execute the method specific to removal or addition.
     * {@code GroupAction}s may then validate or modify the changes.
     *
     * @param group    The target group.
     * @param isRemove Indicates whether the member is removed or added.
     * @param member   The member successfully removed or added.
     * @throws RepositoryException If an error occurs.
     */
    void onGroupUpdate(@NotNull Group group, boolean isRemove, @NotNull Authorizable member) throws RepositoryException {
        for (GroupAction action : filterGroupActions()) {
            if (isRemove) {
                action.onMemberRemoved(group, member, root, namePathMapper);
            } else {
                action.onMemberAdded(group, member, root, namePathMapper);
            }
        }
    }

    /**
     * Upon a group being updated (multiple {@code memberIds} added or removed),
     * call available {@code GroupAction}s and execute the method specific to removal or addition.
     * {@code GroupAction}s may then validate or modify the changes.
     *
     * @param group       The target group.
     * @param isRemove    Indicates whether the member is removed or added.
     * @param isContentId Indicates whether member ids are expressed as content-ids (UUID) or member-ids.
     * @param memberIds   The IDs of all members successfully removed or added.
     * @param failedIds   The IDs of all members whose addition or removal failed.
     * @throws RepositoryException If an error occurs.
     */
    void onGroupUpdate(@NotNull Group group, boolean isRemove, boolean isContentId, @NotNull Set<String> memberIds, @NotNull Set<String> failedIds) throws RepositoryException {
        for (GroupAction action : filterGroupActions()) {
            if (isRemove) {
                action.onMembersRemoved(group, memberIds, failedIds, root, namePathMapper);
            } else {
                if (isContentId) {
                    action.onMembersAddedContentId(group, memberIds, failedIds, root, namePathMapper);
                } else {
                    action.onMembersAdded(group, memberIds, failedIds, root, namePathMapper);
                }
            }
        }
    }

    //--------------------------------------------------------------------------
    @Nullable
    public Authorizable getAuthorizable(@Nullable Tree tree) throws RepositoryException {
        if (tree == null || !tree.exists()) {
            return null;
        }
        return getAuthorizable(UserUtil.getAuthorizableId(tree), tree);
    }

    @Nullable
    Authorizable getAuthorizableByOakPath(@NotNull String oakPath) throws RepositoryException {
        return getAuthorizable(userProvider.getAuthorizableByPath(oakPath));
    }

    @NotNull
    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @NotNull
    PartialValueFactory getPartialValueFactory() {
        return valueFactory;
    }

    @NotNull
    ReadOnlyNodeTypeManager getNodeTypeManager() {
        if (ntMgr == null) {
            ntMgr = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
        }
        return ntMgr;
    }

    @NotNull
    MembershipProvider getMembershipProvider() {
        return membershipProvider;
    }

    @NotNull
    PrincipalManager getPrincipalManager() {
        return securityProvider.getConfiguration(PrincipalConfiguration.class).getPrincipalManager(root, namePathMapper);
    }

    @NotNull
    ConfigurationParameters getConfig() {
        return config;
    }

    @Nullable
    private Authorizable getAuthorizable(@Nullable String id, @Nullable Tree tree) throws RepositoryException {
        if (id == null || tree == null) {
            return null;
        }
        if (UserUtil.isType(tree, AuthorizableType.USER)) {
            if (UserUtil.isSystemUser(tree)) {
                return new SystemUserImpl(id, tree, this);
            } else {
                return new UserImpl(id, tree, this);
            }
        } else if (UserUtil.isType(tree, AuthorizableType.GROUP)) {
            return new GroupImpl(id, tree, this);
        } else {
            throw new RepositoryException("Not a user or group tree " + tree.getPath() + '.');
        }
    }

    private void checkValidId(@Nullable String id) throws RepositoryException {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Invalid ID " + id);
        }
        if (getAuthorizable(id) != null) {
            throw new AuthorizableExistsException("Authorizable with ID " + id + " already exists");
        }
    }

    void checkValidPrincipal(@Nullable Principal principal, boolean isGroup) throws RepositoryException {
        if (principal == null || Strings.isNullOrEmpty(principal.getName())) {
            throw new IllegalArgumentException("Principal may not be null and must have a valid name.");
        }
        if (!isGroup && EveryonePrincipal.NAME.equals(principal.getName())) {
            throw new IllegalArgumentException("'everyone' is a reserved group principal name.");
        }
        if (getAuthorizable(principal) != null) {
            throw new AuthorizableExistsException("Authorizable with principal " + principal.getName() + " already exists.");
        }
    }

    void setPrincipal(@NotNull Tree authorizableTree, @NotNull Principal principal) {
        authorizableTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, principal.getName());
    }

    void setPassword(@NotNull Tree userTree, @NotNull String userId, @NotNull String password, boolean forceHash) throws RepositoryException {
        String pwHash;
        if (forceHash || PasswordUtil.isPlainTextPassword(password)) {
            try {
                pwHash = PasswordUtil.buildPasswordHash(password, config);
            } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
                throw new RepositoryException(e);
            }
        } else {
            pwHash = password;
        }
        userTree.setProperty(UserConstants.REP_PASSWORD, pwHash);

        // set last-modified property if pw-expiry is enabled and the user is not
        // admin. if initial-pw-change is enabled, we don't set the last modified
        // for new users, in order to force a pw change upon the next login
        boolean expiryEnabled = passwordExpiryEnabled();
        boolean forceInitialPwChange = forceInitialPasswordChangeEnabled();
        boolean isNewUser = userTree.getStatus() == Tree.Status.NEW;

        if (Utils.canHavePasswordExpired(userId, config)
                // only expiry is enabled, set in all cases
                && ((expiryEnabled && !forceInitialPwChange)
                // as soon as force initial pw is enabled, we set in all cases except new users,
                // irrespective of password expiry being enabled or not
                || (forceInitialPwChange && !isNewUser))) {

            Tree pwdTree = TreeUtil.getOrAddChild(userTree, UserConstants.REP_PWD, UserConstants.NT_REP_PASSWORD);
            // System.currentTimeMillis() may be inaccurate on windows. This is accepted for this feature.
            pwdTree.setProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED, System.currentTimeMillis(), Type.LONG);
        }
    }

    private boolean passwordExpiryEnabled() {
        return config.getConfigValue(UserConstants.PARAM_PASSWORD_MAX_AGE, UserConstants.DEFAULT_PASSWORD_MAX_AGE) > 0;
    }

    private boolean forceInitialPasswordChangeEnabled() {
        return config.getConfigValue(UserConstants.PARAM_PASSWORD_INITIAL_CHANGE, UserConstants.DEFAULT_PASSWORD_INITIAL_CHANGE);
    }

    @NotNull
    private UserQueryManager getQueryManager() {
        if (queryManager == null) {
            queryManager = new UserQueryManager(this, namePathMapper, config, root);
        }
        return queryManager;
    }

    /**
     * Select only {@code GroupAction}s from the available {@code AuthorizableAction}s.
     *
     * @return A {@code List} of {@code GroupAction}s. List may be empty.
     */
    @NotNull
    private Iterable<GroupAction> filterGroupActions() {
        return Iterables.filter(actionProvider.getAuthorizableActions(securityProvider), GroupAction.class);
    }

    @NotNull
    private Iterable<UserAction> filterUserActions() {
        return Iterables.filter(actionProvider.getAuthorizableActions(securityProvider), UserAction.class);
    }
}
