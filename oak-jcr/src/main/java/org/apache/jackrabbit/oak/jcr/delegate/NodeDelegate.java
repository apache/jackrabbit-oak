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
package org.apache.jackrabbit.oak.jcr.delegate;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_RESIDUAL_CHILD_NODE_DEFINITIONS;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * {@code NodeDelegate} serve as internal representations of {@code Node}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class NodeDelegate extends ItemDelegate {

    /**
     * Create a new {@code NodeDelegate} instance for a valid {@code TreeLocation}. That
     * is for one where {@code getTree() != null}.
     *
     * @param sessionDelegate
     * @param location
     * @return
     */
    static NodeDelegate create(SessionDelegate sessionDelegate, TreeLocation location) {
        return location.getTree() == null
                ? null
                : new NodeDelegate(sessionDelegate, location);
    }

    protected NodeDelegate(SessionDelegate sessionDelegate, Tree tree) {
        super(sessionDelegate, tree.getLocation());
    }

    private NodeDelegate(SessionDelegate sessionDelegate, TreeLocation location) {
        super(sessionDelegate, location);
    }

    @Nonnull
    public String getIdentifier() throws InvalidItemStateException {
        return sessionDelegate.getIdManager().getIdentifier(getTree());
    }

    /**
     * Determine whether this is the root node
     *
     * @return {@code true} iff this is the root node
     */
    public boolean isRoot() throws InvalidItemStateException {
        return getTree().isRoot();
    }

    /**
     * Get the number of properties of the node
     *
     * @return number of properties of the node
     */
    public long getPropertyCount() throws InvalidItemStateException {
        // TODO: Exclude "invisible" internal properties (OAK-182)
        return getTree().getPropertyCount();
    }

    /**
     * Get a property
     *
     * @param relPath oak path
     * @return property at the path given by {@code relPath} or {@code null} if
     *         no such property exists
     */
    @CheckForNull
    public PropertyDelegate getPropertyOrNull(String relPath) throws RepositoryException {
        TreeLocation propertyLocation = getChildLocation(relPath);
        return propertyLocation.getProperty() == null
                ? null
                : new PropertyDelegate(sessionDelegate, propertyLocation);
    }

    /**
     * Get a property. In contrast to {@link #getPropertyOrNull(String)} this
     * method never returns {@code null}. In the case where no property exists
     * at the given path, the returned property delegate throws an
     * {@code InvalidItemStateException} on access. See See OAK-395.
     *
     * @param relPath oak path
     * @return property at the path given by {@code relPath}.
     */
    @Nonnull
    public PropertyDelegate getProperty(String relPath) throws RepositoryException {
        return new PropertyDelegate(sessionDelegate, getChildLocation(relPath));
    }

    /**
     * Get the properties of the node
     *
     * @return properties of the node
     */
    @Nonnull
    public Iterator<PropertyDelegate> getProperties() throws InvalidItemStateException {
        return propertyDelegateIterator(getTree().getProperties().iterator());
    }

    /**
     * Get the number of child nodes
     *
     * @return number of child nodes of the node
     */
    public long getChildCount() throws InvalidItemStateException {
        // TODO: Exclude "invisible" internal child nodes (OAK-182)
        return getTree().getChildrenCount();
    }

    /**
     * Get child node
     *
     * @param relPath oak path
     * @return node at the path given by {@code relPath} or {@code null} if
     *         no such node exists
     */
    @CheckForNull
    public NodeDelegate getChild(String relPath) throws RepositoryException {
        return create(sessionDelegate, getChildLocation(relPath));
    }

    /**
     * Returns an iterator for traversing all the children of this node.
     * If the node is orderable then the iterator will return child nodes in the
     * specified order. Otherwise the ordering of the iterator is undefined.
     *
     * @return child nodes of the node
     */
    @Nonnull
    public Iterator<NodeDelegate> getChildren() throws InvalidItemStateException {
        Tree tree = getTree();
        long count = tree.getChildrenCount();
        if (count == 0) {
            // Optimise the most common case
            return Collections.<NodeDelegate>emptySet().iterator();
        } else if (count == 1) {
            // Optimise another typical case
            Tree child = tree.getChildren().iterator().next();
            if (!child.getName().startsWith(":")) {
                NodeDelegate delegate = new NodeDelegate(sessionDelegate, child);
                return Collections.singleton(delegate).iterator();
            } else {
                return Collections.<NodeDelegate>emptySet().iterator();
            }
        } else {
            return nodeDelegateIterator(tree.getChildren().iterator());
        }
    }

    public void orderBefore(String source, String target)
            throws ItemNotFoundException, InvalidItemStateException {
        Tree tree = getTree();
        if (tree.getChild(source) == null) {
            throw new ItemNotFoundException("Not a child: " + source);
        } else if (target != null && tree.getChild(target) == null) {
            throw new ItemNotFoundException("Not a child: " + target);
        } else {
            tree.getChild(source).orderBefore(target);
        }
    }

    /**
     * Set a property
     *
     * @param propertyState
     * @return the set property
     */
    @Nonnull
    public PropertyDelegate setProperty(PropertyState propertyState) throws RepositoryException {
        Tree tree = getTree();
        String name = propertyState.getName();
        PropertyState old = tree.getProperty(name);
        if (old != null && old.isArray() && !propertyState.isArray()) {
            throw new ValueFormatException("Attempt to assign a single value to multi-valued property.");
        }
        if (old != null && !old.isArray() && propertyState.isArray()) {
            throw new ValueFormatException("Attempt to assign multiple values to single valued property.");
        }
        tree.setProperty(propertyState);
        return new PropertyDelegate(sessionDelegate, tree.getLocation().getChild(name));
    }

    /**
     * Add a child node
     *
     * @param name oak name
     * @return the added node or {@code null} if such a node already exists
     */
    @CheckForNull
    public NodeDelegate addChild(String name) throws InvalidItemStateException {
        Tree tree = getTree();
        return tree.hasChild(name)
                ? null
                : new NodeDelegate(sessionDelegate, tree.addChild(name));
    }

    /**
     * Remove the node if not root. Does nothing otherwise
     */
    public void remove() throws InvalidItemStateException {
        getTree().remove();
    }

    /**
     * Enables or disabled orderable children on the underlying tree.
     *
     * @param enable whether to enable or disable orderable children.
     */
    public void setOrderableChildren(boolean enable)
            throws InvalidItemStateException {
        getTree().setOrderableChildren(enable);
    }

    public String getDefaultChildType(String childName, String typeName)
            throws NoSuchNodeTypeException, ConstraintViolationException,
            InvalidItemStateException {
        Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
        if (typeName != null) {
            Tree type = typeRoot.getChild(typeName);
            if (type == null) {
                throw new NoSuchNodeTypeException(
                        "Node type " + typeName + " does not exist");
            } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
                throw new ConstraintViolationException(
                        "Node type " + typeName + " is abstract");
            } else if (getBoolean(type, JCR_ISMIXIN)) {
                throw new ConstraintViolationException(
                        "Node type " + typeName + " is a mixin type");
            } else {
                return typeName;
            }
        } else {
            List<Tree> types = newArrayList();
            Tree parent = getTree();

            String primary = getName(parent, JCR_PRIMARYTYPE);
            if (primary != null) {
                Tree type = typeRoot.getChild(primary);
                if (type != null) {
                    types.add(type);
                }
            }

            for (String mixin : getNames(parent, JCR_MIXINTYPES)) {
                Tree type = typeRoot.getChild(mixin);
                if (type != null) {
                    types.add(type);
                }
            }

            // first look for named node definitions
            for (Tree type : types) {
                Tree named = type.getChild(OAK_NAMED_CHILD_NODE_DEFINITIONS);
                if (named != null) {
                    Tree definitions = named.getChild(childName);
                    if (definitions != null) {
                        String defaultName =
                                findDefaultPrimaryType(typeRoot, definitions);
                        if (defaultName != null) {
                            return defaultName;
                        }
                    }
                }
            }

            // then check residual definitions
            for (Tree type : types) {
                Tree definitions = type.getChild(OAK_RESIDUAL_CHILD_NODE_DEFINITIONS);
                if (definitions != null) {
                    String defaultName =
                            findDefaultPrimaryType(typeRoot, definitions);
                    if (defaultName != null) {
                        return defaultName;
                    }
                }
            }

            // no matching child node definition found
            throw new ConstraintViolationException(
                    "No default node type available for child node " + childName);
        }
    }

    private String findDefaultPrimaryType(Tree typeRoot, Tree definitions) {
        for (Tree definition : definitions.getChildren()) {
            String defaultName = getName(definition, JCR_DEFAULTPRIMARYTYPE);
            if (defaultName != null) {
                return defaultName;
            }
        }
        return null;
    }

    //------------------------------------------------------------< internal >---

    @Nonnull // FIXME this should be package private. OAK-672
    public Tree getTree() throws InvalidItemStateException {
        Tree tree = getLocation().getTree();
        if (tree == null) {
            throw new InvalidItemStateException();
        }
        return tree;
    }

    // -----------------------------------------------------------< private >---

    private TreeLocation getChildLocation(String relPath) throws RepositoryException {
        if (PathUtils.isAbsolute(relPath)) {
            throw new RepositoryException("Not a relative path: " + relPath);
        }

        return TreeUtil.getTreeLocation(getLocation(), relPath);
    }

    private Iterator<NodeDelegate> nodeDelegateIterator(
            Iterator<Tree> children) {
        return Iterators.transform(
                Iterators.filter(children, new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return !tree.getName().startsWith(":");
                    }
                }),
                new Function<Tree, NodeDelegate>() {
                    @Override
                    public NodeDelegate apply(Tree tree) {
                        return new NodeDelegate(sessionDelegate, tree);
                    }
                });
    }

    private Iterator<PropertyDelegate> propertyDelegateIterator(
            Iterator<? extends PropertyState> properties) throws InvalidItemStateException {
        final TreeLocation location = getLocation();
        return Iterators.transform(
                Iterators.filter(properties, new Predicate<PropertyState>() {
                    @Override
                    public boolean apply(PropertyState property) {
                        return !property.getName().startsWith(":");
                    }
                }),
                new Function<PropertyState, PropertyDelegate>() {
                    @Override
                    public PropertyDelegate apply(PropertyState propertyState) {
                        return new PropertyDelegate(sessionDelegate, location.getChild(propertyState.getName()));
                    }
                });
    }

    // Generic property value accessors. TODO: add to Tree?

    private static boolean getBoolean(Tree tree, String name) {
        PropertyState property = tree.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    @CheckForNull
    private static String getName(Tree tree, String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Nonnull
    private static Iterable<String> getNames(Tree tree, String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

}
