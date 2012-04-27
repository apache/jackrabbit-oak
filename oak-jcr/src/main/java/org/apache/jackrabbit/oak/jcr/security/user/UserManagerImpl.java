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

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.ItemNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final SessionContext sessionContext;
    private final UserManagerConfig config;
    private final AuthorizableNodeCreator nodeCreator;

    public UserManagerImpl(SessionContext sessionContext, UserManagerConfig config) {
        this.sessionContext = sessionContext;
        this.config = config;
        nodeCreator = new AuthorizableNodeCreator(sessionContext);
    }

    //--------------------------------------------------------< UserManager >---
    /**
     * @see UserManager#getAuthorizable(String)
     */
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        Authorizable authorizable = null;
        try {
            NodeImpl node = (NodeImpl) sessionContext.getSession().getNodeByIdentifier(buildIdentifier(id));
            authorizable = getAuthorizable(node);
        } catch (ItemNotFoundException e) {
            log.debug("No authorizable with ID " + id);
        }
        return authorizable;
    }

    /**
     * @see UserManager#getAuthorizable(Principal)
     */
    @Override
    public Authorizable getAuthorizable(Principal principal) throws RepositoryException {
        Session session = sessionContext.getSession();
        Authorizable authorizable = null;
        if (principal instanceof ItemBasedPrincipal) {
            String authPath = ((ItemBasedPrincipal) principal).getPath();
            if (session.nodeExists(authPath)) {
                NodeImpl n = (NodeImpl) session.getNode(authPath);
                authorizable = getAuthorizable(n);
            }
        } else {
            // another Principal implementation.
            String name = principal.getName();
            Authorizable a = getAuthorizable(name);
            if (a != null && name.equals(a.getPrincipal().getName())) {
                authorizable = a;
            } else {
                Iterator<Authorizable> result = findAuthorizables(AuthorizableImpl.REP_PRINCIPAL_NAME, name, SEARCH_TYPE_AUTHORIZABLE);
                if (result.hasNext()) {
                    authorizable = result.next();
                }
            }
        }
        // build the corresponding authorizable object
        return authorizable;
    }

    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        // TODO
        return null;
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        // TODO : create and execute a query
        return null;
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        // TODO : create and execute a query
        return null;
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        // TODO : execute the specified query
        return null;
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

        NodeImpl userNode = nodeCreator.createUserNode(userID, intermediatePath);
        setPrincipal(userNode, principal);
        setPassword(userNode, password, true);

        User user = new UserImpl(userNode, this);
        onCreate(user, password);

        log.debug("User created: " + userID + "; " + userNode.getPath());
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

        NodeImpl groupNode = nodeCreator.createGroupNode(groupID, intermediatePath);
        setPrincipal(groupNode, principal);

        Group group = new GroupImpl(groupNode, this);
        onCreate(group);

        log.debug("Group created: " + groupID + "; " + groupNode.getPath());
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
            action.onCreate(user, password, sessionContext.getSession());
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
            action.onCreate(group, sessionContext.getSession());
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
            action.onRemove(authorizable, sessionContext.getSession());
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
            action.onPasswordChange(user, password, sessionContext.getSession());
        }
    }

    //--------------------------------------------------------------------------
    /**
     * @param userID A userID.
     * @return true if the given userID belongs to the administrator user.
     */
    boolean isAdminId(String userID) {
        return config.getAdminId().equals(userID);
    }

    /**
     *
     *
     * @param userNode The node representing the user.
     * @param password The plaintext password to set.
     * @param forceHash If true the specified password will always be hashed.
     * @throws javax.jcr.RepositoryException If an error occurs
     */
    void setPassword(NodeImpl userNode, String password, boolean forceHash) throws RepositoryException {
        if (password != null) {
            log.debug("Password is null.");
            return;
        }
        String pwHash;
        if (forceHash || PasswordUtility.isPlainTextPassword(password)) {
            try {
                String algorithm = config.getConfigValue(UserManagerConfig.PARAM_PASSWORD_HASH_ALGORITHM, PasswordUtility.DEFAULT_ALGORITHM);
                int iterations = config.getConfigValue(UserManagerConfig.PARAM_PASSWORD_HASH_ITERATIONS, PasswordUtility.DEFAULT_ITERATIONS);
                int saltSize = config.getConfigValue(UserManagerConfig.PARAM_PASSWORD_SALT_SIZE, PasswordUtility.DEFAULT_SALT_SIZE);
                pwHash = PasswordUtility.buildPasswordHash(password, algorithm, saltSize, iterations);
            } catch (NoSuchAlgorithmException e) {
                throw new RepositoryException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RepositoryException(e);
            }
        } else {
            pwHash = password;
        }
        setInternalProperty(userNode, AuthorizableImpl.REP_PASSWORD, pwHash, PropertyType.STRING);

    }

    void setPrincipal(NodeImpl userNode, Principal principal) throws RepositoryException {
        // TODO: validate the principal

        if (!userNode.isNew() || userNode.hasProperty(AuthorizableImpl.REP_PRINCIPAL_NAME)) {
            throw new RepositoryException("rep:principalName can only be set once on a new node.");
        }
        setInternalProperty(userNode, AuthorizableImpl.REP_PRINCIPAL_NAME, principal.getName(), PropertyType.STRING);
    }

    void setInternalProperty(NodeImpl userNode, String name, String value, int type) throws RepositoryException {
        CoreValue cv = ValueConverter.toCoreValue(value, type, sessionContext);
        sessionContext.getTree(userNode.getOakPath()).setProperty(name, cv);
    }

    void setInternalProperty(NodeImpl userNode, String name, String[] values, int type) throws RepositoryException {
        List<CoreValue> cvs = ValueConverter.toCoreValues(values, type, sessionContext);
        sessionContext.getTree(userNode.getOakPath()).setProperty(name, cvs);
    }

    void removeInternalProperty(NodeImpl userNode, String name) {
        sessionContext.getTree(userNode.getOakPath()).removeProperty(name);
    }

    private Authorizable getAuthorizable(NodeImpl node) throws RepositoryException {
        if (node.isNodeType(AuthorizableImpl.NT_REP_USER)) {
            return new UserImpl(node, this);
        } else if (node.isNodeType(AuthorizableImpl.NT_REP_GROUP)) {
            return new GroupImpl(node, this);
        } else {
            throw new RepositoryException("Unexpected node type " + node.getPrimaryNodeType().getName() + ". Expected rep:User or rep:Group.");
        }
    }

    private String buildIdentifier(String authorizableID) {
        // TODO
        return null;
    }

    private void checkValidID(String authorizableID) {
        // TODO
    }
}