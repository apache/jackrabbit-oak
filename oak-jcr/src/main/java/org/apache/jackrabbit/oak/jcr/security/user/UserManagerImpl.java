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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.security.user.query.XPathQueryBuilder;
import org.apache.jackrabbit.oak.jcr.security.user.query.XPathQueryEvaluator;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.UserProviderImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfig;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final SessionDelegate sessionDelegate;
    private final UserConfig config;
    private final UserProviderImpl userProvider;
    private final NodeTreeUtil util;

    public UserManagerImpl(SessionDelegate sessionDelegate, Root root, UserConfig config) {
        this.sessionDelegate = sessionDelegate;
        this.config = (config == null) ? new UserConfig("admin") : config;
        userProvider = new UserProviderImpl(sessionDelegate.getContentSession(), root, this.config);

        // FIXME: remove again. only tmp workaround
        this.util = new NodeTreeUtil(sessionDelegate.getSession(), root, sessionDelegate.getNamePathMapper());
    }

    //--------------------------------------------------------< UserManager >---
    /**
     * @see UserManager#getAuthorizable(String)
     */
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        Authorizable authorizable = null;
        Tree tree = userProvider.getAuthorizable(id);
        if (tree != null) {
            authorizable = getAuthorizable(tree);
        }
        return authorizable;
    }

    /**
     * @see UserManager#getAuthorizable(Principal)
     */
    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        return getAuthorizable(userProvider.getAuthorizableByPrincipal(principal));
    }

    /**
     * @see UserManager#getAuthorizableByPath(String)
     */
    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        String oakPath = sessionDelegate.getOakPathOrThrow(path);
        return getAuthorizable(userProvider.getAuthorizableByPath(oakPath));
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        return findAuthorizables(relPath, value, SEARCH_TYPE_AUTHORIZABLE);
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        // TODO : create and execute a query
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        XPathQueryBuilder builder = new XPathQueryBuilder();
        query.build(builder);
        return new XPathQueryEvaluator(builder, this, sessionDelegate.getQueryManager(), sessionDelegate.getNamePathMapper()).eval();
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
        checkValidID(userID);
        checkValidPrincipal(principal, false);

        Tree userTree = userProvider.createUser(userID, intermediatePath);
        setPrincipal(userTree, principal);
        setPassword(userTree, password, true);

        User user = new UserImpl(util.getNode(userTree), userTree, this);
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
        checkValidID(groupID);
        checkValidPrincipal(principal, true);

        Tree groupTree = userProvider.createGroup(groupID, intermediatePath);
        setPrincipal(groupTree, principal);

        Group group = new GroupImpl(util.getNode(groupTree), groupTree, this);
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
        for (AuthorizableAction action : config.getAuthorizableActions()) {
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
        for (AuthorizableAction action : config.getAuthorizableActions()) {
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
        for (AuthorizableAction action : config.getAuthorizableActions()) {
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
        for (AuthorizableAction action : config.getAuthorizableActions()) {
            action.onPasswordChange(user, password, getSession());
        }
    }

    //--------------------------------------------------------------------------
    /**
     *
     *
     * @param userNode The node representing the user.
     * @param password The plaintext password to set.
     * @param forceHash If true the specified password will always be hashed.
     * @throws javax.jcr.RepositoryException If an error occurs
     */
    void setPassword(Tree userNode, String password, boolean forceHash) throws RepositoryException {
        if (password == null) {
            log.debug("Password is null.");
            return;
        }
        String pwHash;
        if (forceHash || PasswordUtility.isPlainTextPassword(password)) {
            try {
                String algorithm = config.getConfigValue(UserConfig.PARAM_PASSWORD_HASH_ALGORITHM, PasswordUtility.DEFAULT_ALGORITHM);
                int iterations = config.getConfigValue(UserConfig.PARAM_PASSWORD_HASH_ITERATIONS, PasswordUtility.DEFAULT_ITERATIONS);
                int saltSize = config.getConfigValue(UserConfig.PARAM_PASSWORD_SALT_SIZE, PasswordUtility.DEFAULT_SALT_SIZE);
                pwHash = PasswordUtility.buildPasswordHash(password, algorithm, saltSize, iterations);
            } catch (NoSuchAlgorithmException e) {
                throw new RepositoryException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RepositoryException(e);
            }
        } else {
            pwHash = password;
        }
        setInternalProperty(userNode, UserConstants.REP_PASSWORD, pwHash, PropertyType.STRING);
    }

    void setPrincipal(Tree userNode, Principal principal) throws RepositoryException {
        if (userNode.getStatus() != Tree.Status.NEW || userNode.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
            throw new RepositoryException("rep:principalName can only be set once on a new node.");
        }
        setInternalProperty(userNode, UserConstants.REP_PRINCIPAL_NAME, principal.getName(), PropertyType.STRING);
    }

    void setInternalProperty(Tree userNode, String oakName, String value, int type) throws RepositoryException {
        CoreValue cv = ValueConverter.toCoreValue(value, type, sessionDelegate);
        userNode.setProperty(oakName, cv);
    }

    void setInternalProperty(Tree userNode, String oakName, String[] values, int type) throws RepositoryException {
        List<CoreValue> cvs = ValueConverter.toCoreValues(values, type, sessionDelegate);
        userNode.setProperty(oakName, cvs);
    }

    Session getSession() {
        return sessionDelegate.getSession();
    }

    NamePathMapper getNamePathMapper() {
        return sessionDelegate.getNamePathMapper();
    }

    UserProvider getUserProvider() {
        return userProvider;
    }

    MembershipProvider getMembershipProvider() {
        return userProvider;
    }

    @CheckForNull
    Authorizable getAuthorizable(Tree tree) throws RepositoryException {
        if (tree == null) {
            return null;
        }
        Node node = util.getNode(tree);
        if (node.isNodeType(getJcrName(UserConstants.NT_REP_USER))) {
            return new UserImpl(node, tree, this);
        } else if (node.isNodeType(getJcrName(UserConstants.NT_REP_GROUP))) {
            return new GroupImpl(node, tree, this);
        } else {
            throw new RepositoryException("Unexpected node type " + node.getPrimaryNodeType().getName() + ". Expected rep:User or rep:Group.");
        }
    }

    String getJcrName(String oakName) {
        return sessionDelegate.getNamePathMapper().getJcrName(oakName);
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
}