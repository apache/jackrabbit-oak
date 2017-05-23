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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oak level implementation of the internal {@code AuthorizableProperties} that
 * is used in those cases where no {@code Session} is associated with the
 * {@code UserManager} and only OAK API methods can be used to read and
 * modify authorizable properties.
 */
class AuthorizablePropertiesImpl implements AuthorizableProperties {

    private static final Logger log = LoggerFactory.getLogger(AuthorizablePropertiesImpl.class);

    private final AuthorizableImpl authorizable;
    private final NamePathMapper namePathMapper;

    AuthorizablePropertiesImpl(@Nonnull AuthorizableImpl authorizable, @Nonnull NamePathMapper namePathMapper) {
        this.authorizable = authorizable;
        this.namePathMapper = namePathMapper;
    }

    //---------------------------------------------< AuthorizableProperties >---
    @Nonnull
    @Override
    public Iterator<String> getNames(@Nonnull String relPath) throws RepositoryException {
        String oakPath = getOakPath(relPath);
        Tree tree = getTree();
        TreeLocation location = getLocation(tree, oakPath);
        Tree parent = location.getTree();
        if (parent != null && Text.isDescendantOrEqual(tree.getPath(), parent.getPath())) {
            List<String> l = new ArrayList<String>();
            for (PropertyState property : parent.getProperties()) {
                String propName = property.getName();
                if (isAuthorizableProperty(tree, location.getChild(propName), false)) {
                    l.add(namePathMapper.getJcrName(propName));
                }
            }
            return l.iterator();
        } else {
            throw new RepositoryException("Relative path " + relPath + " refers to non-existing tree or tree outside of scope of authorizable.");
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#hasProperty(String)
     */
    @Override
    public boolean hasProperty(@Nonnull String relPath) throws RepositoryException {
        String oakPath = getOakPath(relPath);
        return isAuthorizableProperty(getTree(), getLocation(getTree(), oakPath), true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getProperty(String)
     */
    @Override
    public Value[] getProperty(@Nonnull String relPath) throws RepositoryException {
        String oakPath = getOakPath(relPath);
        Tree tree = getTree();
        Value[] values = null;
        PropertyState property = getAuthorizableProperty(tree, getLocation(tree, oakPath), true);
        if (property != null) {
            if (property.isArray()) {
                List<Value> vs = ValueFactoryImpl.createValues(property, namePathMapper);
                values = vs.toArray(new Value[vs.size()]);
            } else {
                values = new Value[]{ValueFactoryImpl.createValue(property, namePathMapper)};
            }
        }
        return values;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value)
     */
    @Override
    public void setProperty(@Nonnull String relPath, @Nullable Value value) throws RepositoryException {
        if (value == null) {
            removeProperty(relPath);
        } else {
            String oakPath = getOakPath(relPath);
            String name = Text.getName(oakPath);
            PropertyState propertyState = PropertyStates.createProperty(name, value);

            String intermediate = (oakPath.equals(name)) ? null : Text.getRelativeParent(oakPath, 1);
            Tree parent = getOrCreateTargetTree(intermediate);
            checkProtectedProperty(parent, propertyState);

            parent.setProperty(propertyState);
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public void setProperty(@Nonnull String relPath, @Nullable Value[] values) throws RepositoryException {
        if (values == null) {
            removeProperty(relPath);
        } else {
            String oakPath = getOakPath(relPath);
            String name = Text.getName(oakPath);
            PropertyState propertyState =
                    PropertyStates.createProperty(name, Arrays.asList(values));

            String intermediate = (oakPath.equals(name)) ? null : Text.getRelativeParent(oakPath, 1);
            Tree parent = getOrCreateTargetTree(intermediate);
            checkProtectedProperty(parent, propertyState);

            parent.setProperty(propertyState);
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#removeProperty(String)
     */
    @Override
    public boolean removeProperty(@Nonnull String relPath) throws RepositoryException {
        String oakPath = getOakPath(relPath);
        Tree node = getTree();
        TreeLocation propertyLocation = getLocation(node, oakPath);
        if (propertyLocation.getProperty() != null) {
            if (isAuthorizableProperty(node, propertyLocation, true)) {
                return propertyLocation.remove();
            } else {
                throw new ConstraintViolationException("Property " + relPath + " isn't a modifiable authorizable property");
            }
        } else {
            checkScope(node.getPath(), propertyLocation.getPath(), relPath);
        }
        // no such property or wasn't a property of this authorizable.
        return false;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Tree getTree() {
        return authorizable.getTree();
    }

    /**
     * Returns true if the given property of the authorizable node is one of the
     * non-protected properties defined by the rep:Authorizable node type or a
     * some other descendant of the authorizable node.
     *
     * @param authorizableTree The tree of the target authorizable.
     * @param propertyLocation Location to be tested.
     * @param verifyAncestor   If true the property is tested to be a descendant
     *                         of the node of this authorizable; otherwise it is expected that this
     *                         test has been executed by the caller.
     * @return {@code true} if the given property is not protected and is defined
     *         by the rep:authorizable node type or one of it's sub-node types;
     *         {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    private boolean isAuthorizableProperty(@Nonnull Tree authorizableTree, @Nonnull TreeLocation propertyLocation, boolean verifyAncestor) throws RepositoryException {
        return getAuthorizableProperty(authorizableTree, propertyLocation, verifyAncestor) != null;
    }

    /**
     * Returns the valid authorizable property identified by the specified
     * property location or {@code null} if that property does not exist or
     * isn't a authorizable property because it is protected or outside of the
     * scope of the {@code authorizableTree}.
     *
     * @param authorizableTree The tree of the target authorizable.
     * @param propertyLocation Location to be tested.
     * @param verifyAncestor   If true the property is tested to be a descendant
     *                         of the node of this authorizable; otherwise it is expected that this
     *                         test has been executed by the caller.
     * @return a valid authorizable property or {@code null} if no such property
     *         exists or fi the property is protected or not defined by the rep:authorizable
     *         node type or one of it's sub-node types.
     * @throws RepositoryException If an error occurs.
     */
    @CheckForNull
    private PropertyState getAuthorizableProperty(@Nonnull Tree authorizableTree, @Nonnull TreeLocation propertyLocation, boolean verifyAncestor) throws RepositoryException {
        PropertyState property = propertyLocation.getProperty();
        if (property == null) {
            return null;
        }

        String authorizablePath = authorizableTree.getPath();
        if (verifyAncestor && !Text.isDescendant(authorizablePath, propertyLocation.getPath())) {
            log.debug("Attempt to access property outside of authorizable scope.");
            return null;
        }

        Tree parent = propertyLocation.getParent().getTree();
        if (parent == null) {
            log.debug("Unable to determine definition of authorizable property at " + propertyLocation.getPath());
            return null;
        }
        ReadOnlyNodeTypeManager nodeTypeManager = authorizable.getUserManager().getNodeTypeManager();
        PropertyDefinition def = nodeTypeManager.getDefinition(parent, property, true);
        if (def.isProtected() || (authorizablePath.equals(parent.getPath())
                && !def.getDeclaringNodeType().isNodeType(UserConstants.NT_REP_AUTHORIZABLE))) {
            return null;
        } // else: non-protected property somewhere in the subtree of the user tree.

        return property;
    }

    private void checkProtectedProperty(@Nonnull Tree parent, @Nonnull PropertyState property) throws RepositoryException {
        ReadOnlyNodeTypeManager nodeTypeManager = authorizable.getUserManager().getNodeTypeManager();
        PropertyDefinition def = nodeTypeManager.getDefinition(parent, property, false);
        if (def.isProtected()) {
            throw new ConstraintViolationException(
                    "Attempt to set an protected property " + property.getName());
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
     *                             to a node that is outside of the scope of this authorizable.
     */
    @Nonnull
    private Tree getOrCreateTargetTree(@CheckForNull String relPath) throws RepositoryException {
        Tree targetTree;
        Tree userTree = getTree();
        if (relPath != null) {
            String userPath = userTree.getPath();
            targetTree = getLocation(userTree, relPath).getTree();
            if (targetTree != null) {
                checkScope(userPath, targetTree.getPath(), relPath);
            } else {
                targetTree = Utils.getOrAddTree(userTree, relPath, JcrConstants.NT_UNSTRUCTURED);
                checkScope(userPath, targetTree.getPath(), relPath);
            }
        } else {
            targetTree = userTree;
        }
        return targetTree;
    }

    @Nonnull
    private static TreeLocation getLocation(@Nonnull Tree tree, @Nonnull String relativePath) {
        TreeLocation loc = TreeLocation.create(tree);
        for (String element : Text.explode(relativePath, '/', false)) {
            if (PathUtils.denotesParent(element)) {
                loc = loc.getParent();
            } else if (!PathUtils.denotesCurrent(element)) {
                loc = loc.getChild(element);
            }  // else . -> skip to next element
        }
        return loc;
    }

    @Nonnull
    private String getOakPath(@CheckForNull String relPath) throws RepositoryException {
        if (relPath == null || relPath.isEmpty() || relPath.charAt(0) == '/') {
            throw new RepositoryException("Relative path expected. Found " + relPath);
        }
        String oakPath = namePathMapper.getOakPath(relPath);
        if (oakPath == null) {
            throw new RepositoryException("Failed to resolve relative path: " + relPath);
        }
        return oakPath;
    }

    private static void checkScope(@Nonnull String userPath, @Nonnull String targetPath, @Nonnull String relPath) throws RepositoryException {
        if (!Text.isDescendantOrEqual(userPath, targetPath)) {
            throw new RepositoryException("Relative path " + relPath + " outside of scope of " + userPath);
        }
    }
}
