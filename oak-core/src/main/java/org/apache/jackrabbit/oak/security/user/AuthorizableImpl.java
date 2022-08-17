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

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.util.Collections;
import java.util.Iterator;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * Base class for {@code User} and {@code Group} implementations.
 */
abstract class AuthorizableImpl implements Authorizable, UserConstants, TreeAware {

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

    AuthorizableImpl(@NotNull String id, @NotNull Tree tree,
                     @NotNull UserManagerImpl userManager) throws RepositoryException {
        checkValidTree(tree);

        this.id = id;
        this.tree = tree;
        this.userManager = userManager;
    }

    abstract void checkValidTree(@NotNull Tree tree) throws RepositoryException;

    static boolean isValidAuthorizableImpl(@NotNull Authorizable authorizable) {
        return authorizable instanceof AuthorizableImpl;
    }

    //-------------------------------------------------------< Authorizable >---
    @NotNull
    @Override
    public String getID() {
        return id;
    }

    @NotNull
    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return memberOfMonitored(false);
    }

    @NotNull
    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return memberOfMonitored(true);
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

    @NotNull
    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return getPropertyNames(".");
    }

    @NotNull
    @Override
    public Iterator<String> getPropertyNames(@NotNull String relPath) throws RepositoryException {
        return getAuthorizableProperties().getNames(relPath);
    }

    @Override
    public boolean hasProperty(@NotNull String relPath) throws RepositoryException {
        return getAuthorizableProperties().hasProperty(relPath);
    }

    @Nullable
    @Override
    public Value[] getProperty(@NotNull String relPath) throws RepositoryException {
        return getAuthorizableProperties().getProperty(relPath);
    }

    @Override
    public void setProperty(@NotNull String relPath, @Nullable Value value) throws RepositoryException {
        getAuthorizableProperties().setProperty(relPath, value);
    }

    @Override
    public void setProperty(@NotNull String relPath, @Nullable Value[] values) throws RepositoryException {
        getAuthorizableProperties().setProperty(relPath, values);
    }

    @Override
    public boolean removeProperty(@NotNull String relPath) throws RepositoryException {
        return getAuthorizableProperties().removeProperty(relPath);
    }

    @NotNull
    @Override
    public String getPath() {
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
    
    //----------------------------------------------------------< TreeAware >---
    @Override
    @NotNull
    public Tree getTree() {
        if (tree.exists()) {
            return tree;
        } else {
            throw new IllegalStateException("Authorizable " + id + ": underlying tree has been disconnected.");
        }
    }

    //--------------------------------------------------------------------------

    @Nullable 
    String getPrincipalNameOrNull() {
        if (principalName == null) {
            PropertyState pNameProp = tree.getProperty(REP_PRINCIPAL_NAME);
            if (pNameProp != null) {
                principalName = pNameProp.getValue(STRING);
            }
        }
        return principalName;
    }
    
    @NotNull
    String getPrincipalName() throws RepositoryException {
        String pName = getPrincipalNameOrNull();
        if (pName == null) {
            String msg = "Authorizable without principal name " + id;
            log.warn(msg);
            throw new RepositoryException(msg);
        }
        return pName;
    }

    /**
     * @return The user manager associated with this authorizable.
     */
    @NotNull
    UserManagerImpl getUserManager() {
        return userManager;
    }

    /**
     * @return The membership provider associated with this authorizable
     */
    @NotNull
    MembershipProvider getMembershipProvider() {
        return userManager.getMembershipProvider();
    }

    @NotNull
    UserMonitor getMonitor() {
        return userManager.getMonitor();
    }

    /**
     * Returns {@code true} if this authorizable represents the 'everyone' group.
     *
     * @return {@code true} if this authorizable represents the group everyone
     * is member of; {@code false} otherwise.
     */
    boolean isEveryone() {
        return Utils.isEveryone(this);
    }

    /**
     * Retrieve authorizable properties for property related operations.
     *
     * @return The authorizable properties for this user/group.
     */
    private AuthorizableProperties getAuthorizableProperties() {
        if (properties == null) {
            properties = new AuthorizablePropertiesImpl(this, userManager.getPartialValueFactory());
        }
        return properties;
    }

    @NotNull
    private Iterator<Group> memberOfMonitored(boolean includeInherited) throws RepositoryException {
        Stopwatch watch = Stopwatch.createStarted();
        Iterator<Group> groups = getMembership(includeInherited);
        getMonitor().doneMemberOf(watch.elapsed(NANOSECONDS), !includeInherited);
        return groups;
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
    @NotNull
    private Iterator<Group> getMembership(boolean includeInherited) throws RepositoryException {
        if (isEveryone()) {
            return Collections.emptyIterator();
        }

        DynamicMembershipProvider dmp = userManager.getDynamicMembershipProvider();
        Iterator<Group> dynamicGroups = dmp.getMembership(this, includeInherited);
        
        MembershipProvider mMgr = getMembershipProvider();
        Iterator<Tree> trees = mMgr.getMembership(getTree(), includeInherited);
        
        if (!trees.hasNext()) {
            return dynamicGroups;
        }
        
        AuthorizableIterator groups = AuthorizableIterator.create(trees, userManager, AuthorizableType.GROUP);
        AuthorizableIterator allGroups = AuthorizableIterator.create(true, dynamicGroups, groups);
        return new RangeIteratorAdapter(allGroups);
    }
}
