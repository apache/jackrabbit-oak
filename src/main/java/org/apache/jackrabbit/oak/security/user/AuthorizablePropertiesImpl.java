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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.LocationUtil;
import org.apache.jackrabbit.oak.util.NodeUtil;
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

    private final UserManagerImpl userManager;
    private final String id;
    private final ReadOnlyNodeTypeManager nodeTypeManager;

    AuthorizablePropertiesImpl(String id, UserManagerImpl userManager,
                               ReadOnlyNodeTypeManager nodeTypeManager) {
        this.userManager = userManager;
        this.id = id;
        this.nodeTypeManager = nodeTypeManager;
    }

    //---------------------------------------------< AuthorizableProperties >---
    @Override
    public Iterator<String> getNames(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree tree = getTree();
        TreeLocation location = getLocation(tree, relPath);
        Tree parent = location.getTree();
        if (parent != null && Text.isDescendantOrEqual(tree.getPath(), parent.getPath())) {
            List<String> l = new ArrayList<String>();
            for (PropertyState property : parent.getProperties()) {
                String propName = property.getName();
                if (isAuthorizableProperty(tree, location.getChild(propName), false)) {
                    l.add(propName);
                }
            }
            return l.iterator();
        } else {
            throw new RepositoryException("Relative path " + relPath + " refers to items outside of scope of authorizable.");
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#hasProperty(String)
     */
    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        return isAuthorizableProperty(getTree(), getLocation(getTree(), relPath), true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getProperty(String)
     */
    @Override
    public Value[] getProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree tree = getTree();
        Value[] values = null;
        PropertyState property = getAuthorizableProperty(tree, getLocation(tree, relPath), true);
        if (property != null) {
            NamePathMapper npMapper = userManager.getNamePathMapper();
            if (property.isArray()) {
                List<Value> vs = ValueFactoryImpl.createValues(property, npMapper);
                values = vs.toArray(new Value[vs.size()]);
            } else {
                values = new Value[]{ValueFactoryImpl.createValue(property, npMapper)};
            }
        }
        return values;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value)
     */
    @Override
    public void setProperty(String relPath, Value value) throws RepositoryException {
        if (value == null) {
            removeProperty(relPath);
        } else {
            checkRelativePath(relPath);

            String name = Text.getName(relPath);
            String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);
            Tree parent = getOrCreateTargetTree(intermediate);
            checkProtectedProperty(parent, name, false, value.getType());

            // check if the property has already been created as multi valued
            // property before -> in this case remove in order to avoid
            // ValueFormatException.
            PropertyState p = parent.getProperty(name);
            if (p != null && p.isArray()) {
                parent.removeProperty(name);
            }
            PropertyState propertyState = PropertyStates.createProperty(name, value);
            parent.setProperty(propertyState);
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public void setProperty(String relPath, Value[] values) throws RepositoryException {
        if (values == null) {
            removeProperty(relPath);
        } else {
            checkRelativePath(relPath);

            String name = Text.getName(relPath);
            String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);
            Tree parent = getOrCreateTargetTree(intermediate);
            int targetType = (values.length == 0) ? PropertyType.UNDEFINED : values[0].getType();
            checkProtectedProperty(parent, name, true, targetType);

            // check if the property has already been created as single valued
            // property before -> in this case remove in order to avoid
            // ValueFormatException.
            PropertyState p = parent.getProperty(name);
            if (p != null && !p.isArray()) {
                parent.removeProperty(name);
            }
            PropertyState propertyState = PropertyStates.createProperty(name, Arrays.asList(values));
            parent.setProperty(propertyState);
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#removeProperty(String)
     */
    @Override
    public boolean removeProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree node = getTree();
        TreeLocation propertyLocation = node.getLocation().getChild(relPath);
        if (propertyLocation.getProperty() != null) {
            if (isAuthorizableProperty(node, propertyLocation, true)) {
                return propertyLocation.remove();
            } else {
                throw new ConstraintViolationException("Property " + relPath + " isn't a modifiable authorizable property");
            }
        }
        // no such property or wasn't a property of this authorizable.
        return false;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Tree getTree() {
        return userManager.getAuthorizableTree(id);
    }

    /**
     * Returns true if the given property of the authorizable node is one of the
     * non-protected properties defined by the rep:Authorizable node type or a
     * some other descendant of the authorizable node.
     *
     * @param authorizableTree The tree of the target authorizable.
     * @param propertyLocation Location to be tested.
     * @param verifyAncestor If true the property is tested to be a descendant
     * of the node of this authorizable; otherwise it is expected that this
     * test has been executed by the caller.
     * @return {@code true} if the given property is not protected and is defined
     * by the rep:authorizable node type or one of it's sub-node types;
     * {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    private boolean isAuthorizableProperty(Tree authorizableTree, TreeLocation propertyLocation, boolean verifyAncestor) throws RepositoryException {
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
     * @param verifyAncestor If true the property is tested to be a descendant
     * of the node of this authorizable; otherwise it is expected that this
     * test has been executed by the caller.
     * @return a valid authorizable property or {@code null} if no such property
     * exists or fi the property is protected or not defined by the rep:authorizable
     * node type or one of it's sub-node types.
     * @throws RepositoryException If an error occurs.
     */
    @CheckForNull
    private PropertyState getAuthorizableProperty(Tree authorizableTree, TreeLocation propertyLocation, boolean verifyAncestor) throws RepositoryException {
        if (propertyLocation == null || TreeLocation.NULL == propertyLocation) {
            return null;
        }

        String authorizablePath = authorizableTree.getPath();
        if (verifyAncestor && !Text.isDescendant(authorizablePath, propertyLocation.getPath())) {
            log.debug("Attempt to access property outside of authorizable scope.");
            return null;
        }

        PropertyState property = propertyLocation.getProperty();
        if (property != null) {
            Tree parent = propertyLocation.getParent().getTree();
            if (parent == null) {
                log.debug("Unable to determine definition of authorizable property at " + propertyLocation.getPath());
                return null;
            }
            PropertyDefinition def = nodeTypeManager.getDefinition(parent, property);
            if (def.isProtected() || (authorizablePath.equals(parent.getPath())
                    && !def.getDeclaringNodeType().isNodeType(UserConstants.NT_REP_AUTHORIZABLE))) {
                return null;
            } // else: non-protected property somewhere in the subtree of the user tree.
        } // else: no such property.

        return property;
    }

    private void checkProtectedProperty(Tree parent, String propertyName, boolean isArray, int type) throws RepositoryException {
        PropertyDefinition def = nodeTypeManager.getDefinition(parent, propertyName, isArray, type, false);
        if (def.isProtected()) {
            throw new ConstraintViolationException("Attempt to set an protected property " + propertyName);
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
    private Tree getOrCreateTargetTree(String relPath) throws RepositoryException {
        Tree targetTree;
        Tree userTree = getTree();
        if (relPath != null) {
            String userPath = userTree.getPath();
            targetTree = getLocation(userTree, relPath).getTree();
            if (targetTree != null) {
                if (!Text.isDescendantOrEqual(userPath, targetTree.getPath())) {
                    throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                }
            } else {
                targetTree = new NodeUtil(userTree).getOrAddTree(relPath, JcrConstants.NT_UNSTRUCTURED).getTree();
                if (!Text.isDescendantOrEqual(userPath, targetTree.getPath())) {
                    throw new RepositoryException("Relative path " + relPath + " outside of scope of " + this);
                }
            }
        } else {
            targetTree = userTree;
        }
        return targetTree;
    }

    @Nonnull
    private static TreeLocation getLocation(Tree tree, String relativePath) {
        return LocationUtil.getTreeLocation(tree.getLocation(), relativePath);
    }

    private static void checkRelativePath(String relativePath) throws RepositoryException {
        if (relativePath == null || relativePath.isEmpty() || relativePath.charAt(0) == '/') {
            throw new RepositoryException("Relative path expected. Found " + relativePath);
        }
    }
}
