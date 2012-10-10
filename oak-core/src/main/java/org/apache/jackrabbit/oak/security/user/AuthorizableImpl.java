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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * AuthorizableImpl...
 */
abstract class AuthorizableImpl implements Authorizable, UserConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AuthorizableImpl.class);

    private final String id;
    private final UserManagerImpl userManager;
    private Node node;
    private int hashCode;

    AuthorizableImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        this.id = id;
        this.userManager = userManager;

        checkValidTree(tree);
    }

    abstract void checkValidTree(Tree tree) throws RepositoryException;

    static boolean isValidAuthorizableImpl(Authorizable authorizable) {
        return authorizable instanceof AuthorizableImpl;
    }

    //-------------------------------------------------------< Authorizable >---
    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getID()
     */
    @Override
    public String getID() {
        return id;
    }

    /**
     * @see Authorizable#declaredMemberOf()
     */
    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return getMembership(false);
    }

    /**
     * @see Authorizable#memberOf()
     */
    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return getMembership(true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#remove()
     */
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

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getPropertyNames()
     */
    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return getPropertyNames(".");
    }

    /**
     * @see Authorizable#getPropertyNames(String)
     */
    @Override
    public Iterator<String> getPropertyNames(String relPath) throws RepositoryException {
        Node node = getNode();
        Node n = node.getNode(relPath);
        if (Text.isDescendantOrEqual(node.getPath(), n.getPath())) {
            List<String> l = new ArrayList<String>();
            for (PropertyIterator it = n.getProperties(); it.hasNext();) {
                Property prop = it.nextProperty();
                if (isAuthorizableProperty(prop, false)) {
                    l.add(prop.getName());
                }
            }
            return l.iterator();
        } else {
            throw new IllegalArgumentException("Relative path " + relPath + " refers to items outside of scope of authorizable " + getID());
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#hasProperty(String)
     */
    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        return node.hasProperty(relPath) && isAuthorizableProperty(node.getProperty(relPath), true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getProperty(String)
     */
    @Override
    public Value[] getProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        Value[] values = null;
        if (node.hasProperty(relPath)) {
            Property prop = node.getProperty(relPath);
            if (isAuthorizableProperty(prop, true)) {
                if (prop.isMultiple()) {
                    values = prop.getValues();
                } else {
                    values = new Value[]{prop.getValue()};
                }
            }
        }
        return values;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value)
     */
    @Override
    public void setProperty(String relPath, Value value) throws RepositoryException {
        String name = Text.getName(relPath);
        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);

        Node n = getOrCreateTargetNode(intermediate);
        // check if the property has already been created as multi valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            Property p = n.getProperty(name);
            if (p.isMultiple()) {
                p.remove();
            }
        }
        n.setProperty(name, value);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public void setProperty(String relPath, Value[] values) throws RepositoryException {
        String name = Text.getName(relPath);
        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);

        Node n = getOrCreateTargetNode(intermediate);
        // check if the property has already been created as single valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            Property p = n.getProperty(name);
            if (!p.isMultiple()) {
                p.remove();
            }
        }
        n.setProperty(name, values);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#removeProperty(String)
     */
    @Override
    public boolean removeProperty(String relPath) throws RepositoryException {
        Node node = getNode();
        if (node.hasProperty(relPath)) {
            Property p = node.getProperty(relPath);
            if (isAuthorizableProperty(p, true)) {
                p.remove();
                return true;
            }
        }
        // no such property or wasn't a property of this authorizable.
        return false;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getPath()
     */
    @Override
    public String getPath() throws RepositoryException {
        return getNode().getPath();
    }

    //-------------------------------------------------------------< Object >---
    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            try {
                Node node = getNode();
                StringBuilder sb = new StringBuilder();
                sb.append(isGroup() ? "group:" : "user:");
                sb.append(node.getSession().getWorkspace().getName());
                sb.append(':');
                sb.append(id);
                hashCode = sb.toString().hashCode();
            } catch (RepositoryException e) {
                log.warn("Error while calculating hash code.",e.getMessage());
            }
        }
        return hashCode;
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AuthorizableImpl) {
            AuthorizableImpl otherAuth = (AuthorizableImpl) obj;
            try {
                Node node = getNode();
                return isGroup() == otherAuth.isGroup() && node.isSame(otherAuth.getNode());
            } catch (RepositoryException e) {
                // should not occur -> return false in this case.
            }
        }
        return false;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        String typeStr = (isGroup()) ? "Group '" : "User '";
        return new StringBuilder().append(typeStr).append(id).append('\'').toString();
    }

    //--------------------------------------------------------------------------
    /**
     * @return The node associated with this authorizable instance.
     * @throws javax.jcr.RepositoryException
     */
    @Nonnull
    Node getNode() throws RepositoryException {
        if (node == null) {
            String jcrPath = userManager.getNamePathMapper().getJcrPath(getTree().getPath());
            node = userManager.getSession().getNode(jcrPath);
        }
        return node;
    }

    @Nonnull
    Tree getTree() {
        Tree tree = getUserProvider().getAuthorizable(id);
        if (tree == null) {
            throw new IllegalStateException("Authorizable not associated with an existing tree");
        }
        return tree;
    }

    String getJcrName(String oakName) {
        return userManager.getNamePathMapper().getJcrName(oakName);
    }

    /**
     * @return The user manager associated with this authorizable.
     */
    @Nonnull
    UserManagerImpl getUserManager() {
        return userManager;
    }

    /**
     * @return The user provider associated with this authorizable
     */
    @Nonnull
    UserProvider getUserProvider() {
        return userManager.getUserProvider();
    }

    /**
     * @return The membership provider associated with this authorizable
     */
    @Nonnull
    MembershipProvider getMembershipProvider() {
        return userManager.getMembershipProvider();
    }

    /**
     * @return The principal name of this authorizable.
     * @throws RepositoryException If no principal name can be retrieved.
     */
    @Nonnull
    String getPrincipalName() throws RepositoryException {
        Tree tree = getTree();
        if (tree.hasProperty(REP_PRINCIPAL_NAME)) {
            return tree.getProperty(REP_PRINCIPAL_NAME).getValue(STRING);
        } else {
            String msg = "Authorizable without principal name " + getID();
            log.warn(msg);
            throw new RepositoryException(msg);
        }
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
     * Returns true if the given property of the authorizable node is one of the
     * non-protected properties defined by the rep:Authorizable node type or a
     * some other descendant of the authorizable node.
     *
     * @param prop Property to be tested.
     * @param verifyAncestor If true the property is tested to be a descendant
     * of the node of this authorizable; otherwise it is expected that this
     * test has been executed by the caller.
     * @return {@code true} if the given property is defined
     * by the rep:authorizable node type or one of it's sub-node types;
     * {@code false} otherwise.
     * @throws RepositoryException If the property definition cannot be retrieved.
     */
    private boolean isAuthorizableProperty(Property prop, boolean verifyAncestor) throws RepositoryException {
        Node node = getNode();
        if (verifyAncestor && !Text.isDescendant(node.getPath(), prop.getPath())) {
            log.debug("Attempt to access property outside of authorizable scope.");
            return false;
        }

        PropertyDefinition def = prop.getDefinition();
        if (def.isProtected()) {
            return false;
        } else if (node.isSame(prop.getParent())) {
            NodeType declaringNt = prop.getDefinition().getDeclaringNodeType();
            return declaringNt.isNodeType(getJcrName(NT_REP_AUTHORIZABLE));
        } else {
            // another non-protected property somewhere in the subtree of this
            // authorizable node -> is a property that can be set using #setProperty.
            return true;
        }
    }

    /**
     * Retrieves the node at {@code relPath} relative to node associated with
     * this authorizable. If no such node exist it and any missing intermediate
     * nodes are created.
     *
     * @param relPath A relative path.
     * @return The corresponding node.
     * @throws RepositoryException If an error occurs or if {@code relPath} refers
     * to a node that is outside of the scope of this authorizable.
     */
    @Nonnull
    private Node getOrCreateTargetNode(String relPath) throws RepositoryException {
        Node n;
        Node node = getNode();
        if (relPath != null) {
            String userPath = node.getPath();
            if (node.hasNode(relPath)) {
                n = node.getNode(relPath);
                if (!Text.isDescendantOrEqual(userPath, n.getPath())) {
                    throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                }
            } else {
                n = node;
                for (String segment : Text.explode(relPath, '/')) {
                    if (n.hasNode(segment)) {
                        n = n.getNode(segment);
                    } else {
                        if (Text.isDescendantOrEqual(userPath, n.getPath())) {
                            n = n.addNode(segment);
                        } else {
                            throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                        }
                    }
                }
            }
        } else {
            n = node;
        }
        return n;
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
        if (oakPaths.hasNext()) {
            AuthorizableIterator groups = AuthorizableIterator.create(oakPaths, userManager, AuthorizableType.GROUP);
            return new RangeIteratorAdapter(groups, groups.getSize());
        } else {
            return RangeIteratorAdapter.EMPTY;
        }
    }
}