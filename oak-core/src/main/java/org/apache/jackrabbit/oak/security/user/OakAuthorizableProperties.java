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
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.util.LocationUtil;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
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
class OakAuthorizableProperties implements AuthorizableProperties {

    private static final Logger log = LoggerFactory.getLogger(OakAuthorizableProperties.class);

    private final UserProvider userProvider;
    private final String id;
    private final NamePathMapper namePathMapper;

    OakAuthorizableProperties(UserProvider userProvider, String id, NamePathMapper namePathMapper) {
        this.userProvider = userProvider;
        this.id = id;
        this.namePathMapper = namePathMapper;
    }

    //---------------------------------------------< AuthorizableProperties >---
    @Override
    public Iterator<String> getNames(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree tree = getTree();
        Tree n = getLocation(tree, relPath).getTree();
        if (n != null && Text.isDescendantOrEqual(tree.getPath(), n.getPath())) {
            List<String> l = new ArrayList<String>();
            for (PropertyState property : n.getProperties()) {
                if (isAuthorizableProperty(tree, property)) {
                    l.add(property.getName());
                }
            }
            return l.iterator();
        } else {
            throw new IllegalArgumentException("Relative path " + relPath + " refers to items outside of scope of authorizable.");
        }
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#hasProperty(String)
     */
    @Override
    public boolean hasProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree tree = getTree();
        TreeLocation propertyLocation = getLocation(tree, relPath);
        return propertyLocation.getProperty() != null && isAuthorizableProperty(tree, propertyLocation, true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getProperty(String)
     */
    @Override
    public Value[] getProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree tree = getTree();
        Value[] values = null;
        TreeLocation propertyLocation = getLocation(tree, relPath);
        PropertyState property = propertyLocation.getProperty();
        if (property != null) {
            if (isAuthorizableProperty(tree, propertyLocation, true)) {
                if (property.isArray()) {
                    List<Value> vs = ValueFactoryImpl.createValues(property, namePathMapper);
                    values = vs.toArray(new Value[vs.size()]);
                } else {
                    values = new Value[]{ValueFactoryImpl.createValue(property, namePathMapper)};
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
        checkRelativePath(relPath);

        String name = Text.getName(relPath);
        if (!isAuthorizableProperty(name)) {
            throw new RepositoryException("Attempt to set an protected property " + name);
        }

        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);
        Tree n = getOrCreateTargetTree(intermediate);
        // check if the property has already been created as multi valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            PropertyState p = n.getProperty(name);
            if (p.isArray()) {
                n.removeProperty(name);
            }
        }
        PropertyState propertyState = PropertyStates.createProperty(name, value);
        n.setProperty(propertyState);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#setProperty(String, javax.jcr.Value[])
     */
    @Override
    public void setProperty(String relPath, Value[] values) throws RepositoryException {
        checkRelativePath(relPath);

        String name = Text.getName(relPath);
        if (!isAuthorizableProperty(name)) {
            throw new RepositoryException("Attempt to set an protected property " + name);
        }

        String intermediate = (relPath.equals(name)) ? null : Text.getRelativeParent(relPath, 1);
        Tree n = getOrCreateTargetTree(intermediate);
        // check if the property has already been created as single valued
        // property before -> in this case remove in order to avoid
        // ValueFormatException.
        if (n.hasProperty(name)) {
            PropertyState p = n.getProperty(name);
            if (!p.isArray()) {
                n.removeProperty(name);
            }
        }
        PropertyState propertyState = PropertyStates.createProperty(name, Arrays.asList(values));
        n.setProperty(propertyState);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#removeProperty(String)
     */
    @Override
    public boolean removeProperty(String relPath) throws RepositoryException {
        checkRelativePath(relPath);

        Tree node = getTree();
        TreeLocation propertyLocation = node.getLocation().getChild(relPath);
        PropertyState property = propertyLocation.getProperty();
        if (property != null) {
            if (isAuthorizableProperty(node, propertyLocation, true)) {
                Tree parent = propertyLocation.getParent().getTree();
                parent.removeProperty(property.getName());
                return true;
            }
        }
        // no such property or wasn't a property of this authorizable.
        return false;
    }

    //------------------------------------------------------------< private >---

    private Tree getTree() {
        return userProvider.getAuthorizable(id);
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
     * @return {@code true} if the given property is defined
     * by the rep:authorizable node type or one of it's sub-node types;
     * {@code false} otherwise.
     */
    private boolean isAuthorizableProperty(Tree authorizableTree, TreeLocation propertyLocation, boolean verifyAncestor) {
        if (verifyAncestor && !Text.isDescendant(authorizableTree.getPath(), propertyLocation.getPath())) {
                log.debug("Attempt to access property outside of authorizable scope.");
                return false;
            }
        return isAuthorizableProperty(authorizableTree, propertyLocation.getProperty());


    }

    private boolean isAuthorizableProperty(Tree authorizableTree, PropertyState property) {
        // FIXME: add proper check for protection and declaring nt of the
        // FIXME: property using nt functionality provided by nt-plugins
        return property != null && isAuthorizableProperty(property.getName());
    }

    private boolean isAuthorizableProperty(String propertyName) {
        String prefix = Text.getNamespacePrefix(propertyName);
        return !NamespaceConstants.RESERVED_PREFIXES.contains(prefix);
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
    private TreeLocation getLocation(Tree tree, String relativePath) {
        TreeLocation target = LocationUtil.getTreeLocation(tree.getLocation(), relativePath);
        return target;
    }

    private static void checkRelativePath(String relativePath) throws RepositoryException {
        if (relativePath == null) {
            throw new RepositoryException("Relative path expected. Found null.");
        }
        if ('/' == relativePath.charAt(0)) {
            throw new RepositoryException("Relative path expected. Found " + relativePath);
        }
    }
}