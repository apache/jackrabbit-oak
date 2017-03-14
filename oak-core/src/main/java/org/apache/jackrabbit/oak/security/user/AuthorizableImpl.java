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

import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * Base class for {@code User} and {@code Group} implementations.
 */
abstract class AuthorizableImpl implements Authorizable, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AuthorizableImpl.class);

    private final String id;
    private final Tree tree;
    private final UserManagerImpl userManager;

    private String principalName;
    private AuthorizableProperties properties;
    private int hashCode;

    AuthorizableImpl(@Nonnull String id, @Nonnull Tree tree,
                     @Nonnull UserManagerImpl userManager) throws RepositoryException {
        checkValidTree(tree);

        this.id = id;
        this.tree = tree;
        this.userManager = userManager;
    }

    abstract void checkValidTree(@Nonnull Tree tree) throws RepositoryException;

    static boolean isValidAuthorizableImpl(Authorizable authorizable) {
        return authorizable instanceof AuthorizableImpl;
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public String getID() {
        return id;
    }

    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return getMembership(false);
    }

    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return getMembership(true);
    }

    @Override
    public void remove() throws RepositoryException {
        // don't allow for removal of the administrator even if the executing
        // session has all permissions.
        if (!isGroup() && ((User) this).isAdmin()) {
            throw new RepositoryException("The administrator cannot be removed.");
        }
        userManager.onRemove(this);
        getTree().remove();
    }

    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return getPropertyNames(".");
    }

    @Override
    public Iterator<String> getPropertyNames(String relPath) throws RepositoryException {
        return getAuthorizableProperties().getNames(relPath);
    }

    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        return getAuthorizableProperties().hasProperty(relPath);
    }

    @Override
    public Value[] getProperty(String relPath) throws RepositoryException {
        return getAuthorizableProperties().getProperty(relPath);
    }

    @Override
    public void setProperty(String relPath, Value value) throws RepositoryException {
        getAuthorizableProperties().setProperty(relPath, value);
    }

    @Override
    public void setProperty(String relPath, Value[] values) throws RepositoryException {
        getAuthorizableProperties().setProperty(relPath, values);
    }

    @Override
    public boolean removeProperty(String relPath) throws RepositoryException {
        return getAuthorizableProperties().removeProperty(relPath);
    }

    @Override
    public String getPath() throws RepositoryException {
        return userManager.getNamePathMapper().getJcrPath(getTree().getPath());
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(isGroup() ? "group:" : "user:");
            sb.append(':');
            sb.append(id);
            sb.append('_');
            sb.append(userManager.hashCode());
            hashCode = sb.toString().hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof AuthorizableImpl) {
            AuthorizableImpl otherAuth = (AuthorizableImpl) obj;
            return isGroup() == otherAuth.isGroup() && id.equals(otherAuth.id) && userManager.equals(otherAuth.userManager);
        }
        return false;
    }

    @Override
    public String toString() {
        String typeStr = (isGroup()) ? "Group '" : "User '";
        return new StringBuilder().append(typeStr).append(id).append('\'').toString();
    }

    //--------------------------------------------------------------------------
    @Nonnull
    Tree getTree() {
        if (tree.exists()) {
            return tree;
        } else {
            throw new IllegalStateException("Authorizable " + id + ": underlying tree has been disconnected.");
        }
    }

    @Nonnull
    String getPrincipalName() throws RepositoryException {
        if (principalName == null) {
            PropertyState pNameProp = tree.getProperty(REP_PRINCIPAL_NAME);
            if (pNameProp != null) {
                principalName = pNameProp.getValue(STRING);
            } else {
                String msg = "Authorizable without principal name " + id;
                log.warn(msg);
                throw new RepositoryException(msg);
            }
        }
        return principalName;
    }

    /**
     * @return The user manager associated with this authorizable.
     */
    @Nonnull
    UserManagerImpl getUserManager() {
        return userManager;
    }

    /**
     * @return The membership provider associated with this authorizable
     */
    @Nonnull
    MembershipProvider getMembershipProvider() {
        return userManager.getMembershipProvider();
    }

    /**
     * Returns {@code true} if this authorizable represents the 'everyone' group.
     *
     * @return {@code true} if this authorizable represents the group everyone
     * is member of; {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean isEveryone() throws RepositoryException {
        return isGroup() && EveryonePrincipal.NAME.equals(getPrincipalName());
    }

    /**
     * Retrieve authorizable properties for property related operations.
     *
     * @return The authorizable properties for this user/group.
     */
    private AuthorizableProperties getAuthorizableProperties() {
        if (properties == null) {
            properties = new AuthorizablePropertiesImpl(this, userManager.getNamePathMapper());
        }
        return properties;
    }

    /**
     * Retrieve the group membership of this authorizable.
     *
     * @param includeInherited Flag indicating whether the resulting iterator only
     * contains groups this authorizable is declared member of or if inherited
     * group membership is respected.
     *
     * @return Iterator of groups this authorizable is (declared) member of.
     * @throws RepositoryException If an error occurs.
     */
    @Nonnull
    private Iterator<Group> getMembership(boolean includeInherited) throws RepositoryException {
        if (isEveryone()) {
            return Collections.<Group>emptySet().iterator();
        }

        MembershipProvider mMgr = getMembershipProvider();
        Iterator<String> oakPaths = mMgr.getMembership(getTree(), includeInherited);

        Authorizable everyoneGroup = userManager.getAuthorizable(EveryonePrincipal.getInstance());
        if (everyoneGroup instanceof GroupImpl) {
            String everyonePath = ((GroupImpl) everyoneGroup).getTree().getPath();
            oakPaths = Iterators.concat(oakPaths, ImmutableSet.of(everyonePath).iterator());
        }
        if (oakPaths.hasNext()) {
            AuthorizableIterator groups = AuthorizableIterator.create(oakPaths, userManager, AuthorizableType.GROUP);
            return new RangeIteratorAdapter(groups, groups.getSize());
        } else {
            return RangeIteratorAdapter.EMPTY;
        }
    }
}