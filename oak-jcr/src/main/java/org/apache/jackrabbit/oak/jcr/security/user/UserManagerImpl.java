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
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.jcr.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.jcr.value.ValueConverter;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;

/**
 * UserManagerImpl...
 */
public class UserManagerImpl implements UserManager {

    private static final Logger log = LoggerFactory.getLogger(UserManagerImpl.class);

    private final SessionDelegate sessionDelegate;
    private final UserManagerConfig config;
    private final AuthorizableNodeCreator nodeCreator;

    private MembershipManager membershipManager;
    
    public UserManagerImpl(SessionDelegate sessionDelegate, UserManagerConfig config) {
        this.sessionDelegate = sessionDelegate;
        this.config = config;
        nodeCreator = new AuthorizableNodeCreator(sessionDelegate, this.config);
    }

    //--------------------------------------------------------< UserManager >---
    /**
     * @see UserManager#getAuthorizable(String)
     */
    @Override
    public Authorizable getAuthorizable(String id) throws RepositoryException {
        Authorizable authorizable = null;
        try {
            Node node = getSession().getNodeByIdentifier(nodeCreator.getNodeID(id));
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
        Session session = getSession();
        Authorizable authorizable = null;
        if (principal instanceof ItemBasedPrincipal) {
            String authPath = ((ItemBasedPrincipal) principal).getPath();
            if (session.nodeExists(authPath)) {
                Node n = session.getNode(authPath);
                authorizable = getAuthorizable(n);
            }
        } else {
            // another Principal implementation.
            String name = principal.getName();
            Authorizable a = getAuthorizable(name);
            if (a != null && name.equals(a.getPrincipal().getName())) {
                authorizable = a;
            } else {
                String propName = getJcrName(UserConstants.REP_PRINCIPAL_NAME);
                Iterator<Authorizable> result = findAuthorizables(propName, name, SEARCH_TYPE_AUTHORIZABLE);
                if (result.hasNext()) {
                    authorizable = result.next();
                }
            }
        }
        // build the corresponding authorizable object
        return authorizable;
    }

    /**
     * @see UserManager#getAuthorizableByPath(String)
     */
    @Override
    public Authorizable getAuthorizableByPath(String path) throws RepositoryException {
        Session session = getSession();
        if (session.nodeExists(path)) {
            return getAuthorizable(session.getNode(path));
        } else {
            return null;
        }
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value) throws RepositoryException {
        // TODO : create and execute a query
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(String relPath, String value, int searchType) throws RepositoryException {
        // TODO : create and execute a query
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Iterator<Authorizable> findAuthorizables(Query query) throws RepositoryException {
        // TODO : execute the specified query
        throw new UnsupportedOperationException("Not Implemented");
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

        Node userNode = nodeCreator.createUserNode(userID, intermediatePath);
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
        checkValidPrincipal(principal, true);

        Node groupNode = nodeCreator.createGroupNode(groupID, intermediatePath);
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
    void setPassword(Node userNode, String password, boolean forceHash) throws RepositoryException {
        if (password == null) {
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
        setInternalProperty(userNode, UserConstants.REP_PASSWORD, pwHash, PropertyType.STRING);
    }

    void setPrincipal(Node userNode, Principal principal) throws RepositoryException {
        if (!userNode.isNew() || userNode.hasProperty(getJcrName(UserConstants.REP_PRINCIPAL_NAME))) {
            throw new RepositoryException("rep:principalName can only be set once on a new node.");
        }
        setInternalProperty(userNode, UserConstants.REP_PRINCIPAL_NAME, principal.getName(), PropertyType.STRING);
    }

    void setInternalProperty(Node userNode, String oakName, String value, int type) throws RepositoryException {
        CoreValue cv = ValueConverter.toCoreValue(value, type, sessionDelegate);
        sessionDelegate.getNode(userNode).setProperty(oakName, cv);
    }

    void setInternalProperty(Node userNode, String oakName, String[] values, int type) throws RepositoryException {
        List<CoreValue> cvs = ValueConverter.toCoreValues(values, type, sessionDelegate);
        sessionDelegate.getNode(userNode).setProperty(oakName, cvs);
    }

    void setInternalProperty(Node userNode, String oakName, Value[] values) throws RepositoryException {
        List<CoreValue> cvs = ValueConverter.toCoreValues(values, sessionDelegate);
        sessionDelegate.getNode(userNode).setProperty(oakName, cvs);
    }

    void removeInternalProperty(Node userNode, String oakName) throws RepositoryException {
        PropertyDelegate pd = sessionDelegate.getNode(userNode).getProperty(oakName);
        if (pd == null) {
            throw new PathNotFoundException("Missing authorizable property " + oakName);
        } else {
            pd.remove();
        }
    }

    Session getSession() {
        return sessionDelegate.getSession();
    }

    MembershipManager getMembershipManager() {
        if (membershipManager == null) {
            int splitSize = config.getConfigValue(UserManagerConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE, 0);
            if (splitSize < 4) {
                log.warn("Invalid value {} for {}. Expected integer >= 4", splitSize, UserManagerConfig.PARAM_GROUP_MEMBERSHIP_SPLIT_SIZE);
                splitSize = 0;
            }
            membershipManager = new MembershipManager(this, splitSize, sessionDelegate);
        }
        return membershipManager;
    }

    Authorizable getAuthorizable(Node node) throws RepositoryException {
        if (node.isNodeType(getJcrName(UserConstants.NT_REP_USER))) {
            return new UserImpl(node, this);
        } else if (node.isNodeType(getJcrName(UserConstants.NT_REP_GROUP))) {
            return new GroupImpl(node, this);
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