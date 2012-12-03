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
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final Root root;
    private final NamePathMapper namePathMapper;
    private final SecurityProvider securityProvider;

    private final UserProvider userProvider;
    private final MembershipProvider membershipProvider;
    private final ConfigurationParameters config;
    private final List<AuthorizableAction> authorizableActions;

    private UserQueryManager queryManager;

    public UserManagerImpl(Root root, NamePathMapper namePathMapper, SecurityProvider securityProvider) {
        this.root = root;
        this.namePathMapper = namePathMapper;
        this.securityProvider = securityProvider;

        UserConfiguration uc = securityProvider.getUserConfiguration();
        this.config = uc.getConfigurationParameters();
        this.userProvider = new UserProvider(root, config);
        this.membershipProvider = new MembershipProvider(root, config);
        this.authorizableActions = uc.getAuthorizableActionProvider().getAuthorizableActions();
    }

    //--------------------------------------------------------< UserManager >---
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        checkIsLive();
        Authorizable authorizable = null;
        Tree tree = userProvider.getAuthorizable(id);
        if (tree != null) {
            authorizable = getAuthorizable(id, tree);
        }
        return authorizable;
    }

    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        checkIsLive();
        return getAuthorizable(userProvider.getAuthorizableByPrincipal(principal));
    }

    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        checkIsLive();
        String oakPath = namePathMapper.getOakPath(path);
        if (oakPath == null) {
            throw new RepositoryException("Invalid path " + path);
        }
        return getAuthorizable(userProvider.getAuthorizableByPath(oakPath));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        return findAuthorizables(relPath, value, SEARCH_TYPE_AUTHORIZABLE);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        checkIsLive();
        return getQueryManager().findAuthorizables(relPath, value, AuthorizableType.getType(searchType));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        checkIsLive();
        return getQueryManager().find(query);
    }

    @Override
    public User createUser(final String userId, String password) throws RepositoryException {
        Principal principal = new PrincipalImpl(userId);
        return createUser(userId, password, principal, null);
    }

    @Override
    public User createUser(String userID, String password, Principal principal, String intermediatePath) throws RepositoryException {
        checkIsLive();
        checkValidID(userID);
        checkValidPrincipal(principal, false);

        if (intermediatePath != null) {
            intermediatePath = namePathMapper.getOakPath(intermediatePath);
        }
        Tree userTree = userProvider.createUser(userID, intermediatePath);
        setPrincipal(userTree, principal);
        if (password != null) {
            setPassword(userTree, password, true);
        }

        User user = new UserImpl(userID, userTree, this);
        onCreate(user, password);

        log.debug("User created: " + userID);
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
    public Group createGroup(Principal principal, String intermediatePath) throws RepositoryException {
        return createGroup(principal.getName(), principal, intermediatePath);
    }

    @Override
    public Group createGroup(String groupID, Principal principal, String intermediatePath) throws RepositoryException {
        checkIsLive();
        checkValidID(groupID);
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
     *
     * @see org.apache.jackrabbit.api.security.user.UserManager#isAutoSave()
     */
    @Override
    public boolean isAutoSave() {
        return false;
    }

    /**
     * Changing the auto-save behavior is not supported by this implementation
     * and this method always throws {@code UnsupportedRepositoryOperationException}
     *
     * @see UserManager#autoSave(boolean)
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
     * @param user The new user.
     * @param password The password.
     * @throws RepositoryException If an exception occurs.
     */
    void onCreate(User user, String password) throws RepositoryException {
        for (AuthorizableAction action : authorizableActions) {
            action.onCreate(user, password, root, namePathMapper);
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
    void onCreate(Group group) throws RepositoryException {
        for (AuthorizableAction action : authorizableActions) {
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
    void onRemove(Authorizable authorizable) throws RepositoryException {
        for (AuthorizableAction action : authorizableActions) {
            action.onRemove(authorizable, root, namePathMapper);
        }
    }

    /**
     * Let the configured {@code AuthorizableAction}s perform additional
     * tasks associated with password changing of a given user before the
     * corresponding property is being changed.
     *
     * @param user The target user.
     * @param password The new password.
     * @throws RepositoryException If an exception occurs.
     */
    void onPasswordChange(User user, String password) throws RepositoryException {
        for (AuthorizableAction action : authorizableActions) {
            action.onPasswordChange(user, password, root, namePathMapper);
        }
    }

    //--------------------------------------------------------------------------
    @CheckForNull
    Tree getAuthorizableTree(String id) {
        Tree tree = userProvider.getAuthorizable(id);
        if (tree == null) {
            throw new IllegalStateException("Authorizable not associated with an existing tree (WAITING FOR OAK-343 TO BE FIXED)");
        }
        return tree;
    }

    @CheckForNull
    Authorizable getAuthorizable(Tree tree) throws RepositoryException {
        if (tree == null) {
            return null;
        }
        return getAuthorizable(userProvider.getAuthorizableId(tree), tree);
    }

    @Nonnull
    AuthorizableProperties getAuthorizableProperties(String id) throws RepositoryException {
        return new OakAuthorizableProperties(root, userProvider, id, namePathMapper);
    }

    @Nonnull
    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @Nonnull
    MembershipProvider getMembershipProvider() {
        return membershipProvider;
    }

    @Nonnull
    PrincipalProvider getPrincipalProvider() throws RepositoryException {
        return securityProvider.getPrincipalConfiguration().getPrincipalProvider(root, namePathMapper);
    }

    @Nonnull
    SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    @Nonnull
    ConfigurationParameters getConfig() {
        return config;
    }

    @CheckForNull
    private Authorizable getAuthorizable(String id, Tree tree) throws RepositoryException {
        if (id == null || tree == null) {
            return null;
        }
        if (UserUtility.isType(tree, AuthorizableType.USER)) {
            return new UserImpl(userProvider.getAuthorizableId(tree), tree, this);
        } else if (UserUtility.isType(tree, AuthorizableType.GROUP)) {
            return new GroupImpl(userProvider.getAuthorizableId(tree), tree, this);
        } else {
            throw new RepositoryException("Not a user or group tree " + tree.getPath() + '.');
        }
    }

    private void checkValidID(String ID) throws RepositoryException {
        if (ID == null || ID.length() == 0) {
            throw new IllegalArgumentException("Invalid ID " + ID);
        } else if (getAuthorizable(ID) != null) {
            throw new AuthorizableExistsException("Authorizable with ID " + ID + " already exists");
        }
    }

    private void checkValidPrincipal(Principal principal, boolean isGroup) {
        if (principal == null || principal.getName() == null || "".equals(principal.getName())) {
            throw new IllegalArgumentException("Principal may not be null and must have a valid name.");
        }
        if (!isGroup && EveryonePrincipal.NAME.equals(principal.getName())) {
            throw new IllegalArgumentException("'everyone' is a reserved group principal name.");
        }
    }

    void setPrincipal(Tree authorizableTree, Principal principal) {
        checkNotNull(principal);
        authorizableTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, principal.getName());
    }

    void setPassword(Tree userTree, String password, boolean forceHash) throws RepositoryException {
        String pwHash;
        if (forceHash || PasswordUtility.isPlainTextPassword(password)) {
            try {
                pwHash = PasswordUtility.buildPasswordHash(password, config);
            } catch (NoSuchAlgorithmException e) {
                throw new RepositoryException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RepositoryException(e);
            }
        } else {
            pwHash = password;
        }
        userTree.setProperty(UserConstants.REP_PASSWORD, pwHash);
    }

    private void checkIsLive() throws RepositoryException {
        try {
            // FIXME: checkIsLive is not part of the public root interface... execute the check using another method.
            root.getBlobFactory();
        } catch (IllegalStateException e) {
            throw new RepositoryException("User manager is no longer alive.");
        }
    }

    private UserQueryManager getQueryManager() {
        if (queryManager == null) {
            queryManager = new UserQueryManager(this, root);
        }
        return queryManager;
    }
}