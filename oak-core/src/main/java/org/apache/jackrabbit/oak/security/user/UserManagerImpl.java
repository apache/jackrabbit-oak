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

import java.security.Principal;
import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.security.user.query.XPathQueryBuilder;
import org.apache.jackrabbit.oak.security.user.query.XPathQueryEvaluator;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final Session session;
    private final NamePathMapper namePathMapper;

    private final UserProvider userProvider;
    private final MembershipProvider membershipProvider;
    private final UserConfig config;

    public UserManagerImpl(Session session, NamePathMapper namePathMapper,
                           UserProvider userProvider, MembershipProvider membershipProvider,
                           UserConfig config) {
        this.session = session;
        this.namePathMapper = namePathMapper;
        this.userProvider = userProvider;
        this.membershipProvider = membershipProvider;
        this.config = config;
    }

    //--------------------------------------------------------< UserManager >---
    /**
     * @see UserManager#getAuthorizable(String)
     */
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        checkIsLive();
        Authorizable authorizable = null;
        Tree tree = getUserProvider().getAuthorizable(id);
        if (tree != null) {
            authorizable = getAuthorizable(id, tree);
        }
        return authorizable;
    }

    /**
     * @see UserManager#getAuthorizable(Principal)
     */
    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        checkIsLive();
        return getAuthorizable(getUserProvider().getAuthorizableByPrincipal(principal));
    }

    /**
     * @see UserManager#getAuthorizableByPath(String)
     */
    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        checkIsLive();
        String oakPath = namePathMapper.getOakPath(path);
        if (oakPath == null) {
            throw new RepositoryException("Invalid path " + path);
        }
        return getAuthorizable(getUserProvider().getAuthorizableByPath(oakPath));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        return findAuthorizables(relPath, value, SEARCH_TYPE_AUTHORIZABLE);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        checkIsLive();
        String[] oakPaths =  new String[] {namePathMapper.getOakPath(relPath)};
        AuthorizableType authorizableType = getAuthorizableType(searchType);
        Iterator<Tree> result = userProvider.findAuthorizables(oakPaths, value, null, true, Long.MAX_VALUE, authorizableType);

        return AuthorizableIterator.create(result, this);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        checkIsLive();
        XPathQueryBuilder builder = new XPathQueryBuilder();
        query.build(builder);
        return new XPathQueryEvaluator(builder, this, session.getWorkspace().getQueryManager(), namePathMapper).eval();
    }

    @Override
    public User createUser(final String userID, String password) throws RepositoryException {
        Principal principal = new Principal() {
            @Override
            public String getName() {
                return userID;
            }
        };
        return createUser(userID, password, principal, null);
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
        userProvider.setPassword(userTree, password, true);

        User user = new UserImpl(userID, userTree, this);
        onCreate(user, password);

        log.debug("User created: " + userID);
        return user;
    }

    @Override
    public Group createGroup(final String groupID) throws RepositoryException {
        Principal principal = new Principal() {
            @Override
            public String getName() {
                return groupID;
            }
        };
        return createGroup(groupID, principal, null);
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
        Tree groupTree = getUserProvider().createGroup(groupID, intermediatePath);
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
        for (AuthorizableAction action : getAuthorizableActions()) {
            action.onCreate(user, password, getSession());
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
        for (AuthorizableAction action : getAuthorizableActions()) {
            action.onCreate(group, getSession());
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
        for (AuthorizableAction action : getAuthorizableActions()) {
            action.onRemove(authorizable, getSession());
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
        for (AuthorizableAction action : getAuthorizableActions()) {
            action.onPasswordChange(user, password, getSession());
        }
    }

    private AuthorizableAction[] getAuthorizableActions() {
        return config.getAuthorizableActions();
    }

    //--------------------------------------------------------------------------

    Session getSession() {
        return session;
    }

    NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    UserProvider getUserProvider() {
        return userProvider;
    }

    MembershipProvider getMembershipProvider() {
        return membershipProvider;
    }

    PrincipalProvider getPrincipalProvider() throws RepositoryException {
        if (!(session instanceof JackrabbitSession)) {
            throw new UnsupportedRepositoryOperationException("Principal management not supported");
        }
        JackrabbitSession js = (JackrabbitSession) session;
        return ((PrincipalManagerImpl) js.getPrincipalManager()).getPrincipalProvider();
    }

    @CheckForNull
    Authorizable getAuthorizable(Tree tree) throws RepositoryException {
        if (tree == null) {
            return null;
        }
        return getAuthorizable(userProvider.getAuthorizableId(tree), tree);
    }

    @CheckForNull
    private Authorizable getAuthorizable(String id, Tree tree) throws RepositoryException {
        if (id == null || tree == null) {
            return null;
        }
        if (userProvider.isAuthorizableType(tree, AuthorizableType.USER)) {
            return new UserImpl(userProvider.getAuthorizableId(tree), tree, this);
        } else if (userProvider.isAuthorizableType(tree, AuthorizableType.GROUP)) {
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

    private void setPrincipal(Tree userTree, Principal principal) {
        getUserProvider().setProtectedProperty(userTree, UserConstants.REP_PRINCIPAL_NAME, principal.getName(), PropertyType.STRING);
    }

    private void checkIsLive() throws RepositoryException {
        if (!session.isLive()) {
            throw new RepositoryException("UserManager has been closed.");
        }
    }

    private static AuthorizableType getAuthorizableType(int searchType) {
        switch (searchType) {
            case UserManager.SEARCH_TYPE_USER:
                return AuthorizableType.USER;
            case UserManager.SEARCH_TYPE_GROUP:
                return AuthorizableType.GROUP;
            case UserManager.SEARCH_TYPE_AUTHORIZABLE:
                return AuthorizableType.AUTHORIZABLE;
            default:
                throw new IllegalArgumentException("Invalid search type " + searchType);
        }
    }
}