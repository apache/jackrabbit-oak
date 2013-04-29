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

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.JcrConstants.JCR_AUTOCREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTVALUES;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_CREATEDBY;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_LASTMODIFIEDBY;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROTECTED_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROTECTED_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_RESIDUAL_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_SUPERTYPES;

import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
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

    @Override
    public boolean isProtected() throws InvalidItemStateException {
        Tree tree = getTree();

        Tree parent = tree.getParentOrNull();
        if (parent != null) {
            String name = tree.getName();
            Tree typeRoot = sessionDelegate.getRoot().getTreeOrNull(NODE_TYPES_PATH);
            List<Tree> types = getEffectiveType(parent, typeRoot);

            boolean protectedResidual = false;
            for (Tree type : types) {
                if (contains(getNames(type, OAK_PROTECTED_CHILD_NODES), name)) {
                    return true;
                } else if (!protectedResidual) {
                    protectedResidual = getBoolean(
                            type, OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES);
                }
            }

            // Special case: There are one or more protected *residual*
            // child node definitions. Iterate through them to check whether
            // there's a matching, protected one.
            if (protectedResidual) {
                Set<String> typeNames = newHashSet();
                for (Tree type : getEffectiveType(tree, typeRoot)) {
                    typeNames.add(getName(type, JCR_NODETYPENAME));
                    addAll(typeNames, getNames(type, OAK_SUPERTYPES));
                }

                for (Tree type : types) {
                    Tree definitions = type.getChildOrNull(OAK_RESIDUAL_CHILD_NODE_DEFINITIONS);
                    if (definitions != null) {
                        for (String typeName : typeNames) {
                            Tree definition = definitions.getChildOrNull(typeName);
                            if (definition != null
                                    && getBoolean(definition, JCR_PROTECTED)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    boolean isProtected(String property) throws InvalidItemStateException {
        Tree tree = getTree();
        Tree typeRoot = sessionDelegate.getRoot().getTreeOrNull(NODE_TYPES_PATH);
        List<Tree> types = getEffectiveType(tree, typeRoot);

        boolean protectedResidual = false;
        for (Tree type : types) {
            if (contains(getNames(type, OAK_PROTECTED_PROPERTIES), property)) {
                return true;
            } else if (!protectedResidual) {
                protectedResidual = getBoolean(
                        type, OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES);
            }
        }

        // Special case: There are one or more protected *residual*
        // child node definitions. Iterate through them to check whether
        // there's a matching, protected one.
        if (protectedResidual) {
            for (Tree type : types) {
                Tree definitions = type.getChildOrNull(OAK_RESIDUAL_PROPERTY_DEFINITIONS);
                if (definitions != null) {
                    for (Tree definition : definitions.getChildren()) {
                        // TODO: check for matching property type?
                        if (getBoolean(definition, JCR_PROTECTED)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
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
        if (tree.getChildOrNull(source) == null) {
            throw new ItemNotFoundException("Not a child: " + source);
        } else if (target != null && tree.getChildOrNull(target) == null) {
            throw new ItemNotFoundException("Not a child: " + target);
        } else {
            tree.getChildOrNull(source).orderBefore(target);
        }
    }

    public boolean canAddMixin(String typeName) throws RepositoryException {
        Tree typeRoot = sessionDelegate.getRoot().getTreeOrNull(NODE_TYPES_PATH);
        Tree type = typeRoot.getChildOrNull(typeName);
        if (type != null) {
            return !getBoolean(type, JCR_IS_ABSTRACT)
                    && getBoolean(type, JCR_ISMIXIN);
        } else {
            throw new NoSuchNodeTypeException(
                    "Node type " + typeName + " does not exist");
        }
    }

    public void addMixin(String typeName) throws RepositoryException {
        Tree typeRoot = sessionDelegate.getRoot().getTreeOrNull(NODE_TYPES_PATH);

        Tree type = typeRoot.getChildOrNull(typeName);
        if (type == null) {
            throw new NoSuchNodeTypeException(
                    "Node type " + typeName + " does not exist");
        } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
            throw new ConstraintViolationException(
                    "Node type " + typeName + " is abstract");
        } else if (!getBoolean(type, JCR_ISMIXIN)) {
            throw new ConstraintViolationException(
                    "Node type " + typeName + " is a not a mixin type");
        }

        Tree tree = getTree();
        List<String> mixins = newArrayList();

        String primary = getName(tree, JCR_PRIMARYTYPE);
        if (primary != null
                && contains(getNames(type, OAK_PRIMARY_SUBTYPES), primary)) {
            return;
        }

        Set<String> submixins = newHashSet(getNames(type, OAK_MIXIN_SUBTYPES));
        for (String mixin : getNames(tree, JCR_MIXINTYPES)) {
            if (typeName.equals(mixin) || submixins.contains(mixin)) {
                return;
            }
            mixins.add(mixin);
        }

        mixins.add(typeName);
        tree.setProperty(JCR_MIXINTYPES, mixins, NAMES);
        autoCreateItems(tree, type, typeRoot);
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
     * @param name Oak name of the new child node
     * @param typeName Oak name of the type of the new child node,
     *                 or {@code null} if a default type should be used
     * @return the added node or {@code null} if such a node already exists
     */
    @CheckForNull
    public NodeDelegate addChild(String name, String typeName)
            throws RepositoryException {
        Tree tree = getTree();
        if (tree.hasChild(name)) {
            return null;
        }

        Tree typeRoot = sessionDelegate.getRoot().getTreeOrNull(NODE_TYPES_PATH);
        if (typeName == null) {
            typeName = getDefaultChildType(typeRoot, tree, name);
            if (typeName == null) {
                throw new ConstraintViolationException(
                        "No default node type available for node "
                        + PathUtils.concat(tree.getPath(), name));
            }
        }

        Tree child = internalAddChild(tree, name, typeName, typeRoot);

        return new NodeDelegate(sessionDelegate, child);
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

    //------------------------------------------------------------< internal >---

    private Tree internalAddChild(
            Tree parent, String name, String typeName, Tree typeRoot)
            throws RepositoryException {
        Tree type = typeRoot.getChildOrNull(typeName);
        if (type == null) {
            throw new NoSuchNodeTypeException(
                    "Node type " + typeName + " does not exist");
        } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
            throw new ConstraintViolationException(
                    "Node type " + typeName + " is abstract");
        } else if (getBoolean(type, JCR_ISMIXIN)) {
            throw new ConstraintViolationException(
                    "Node type " + typeName + " is a mixin type");
        }

        Tree child = parent.addChild(name);
        child.setProperty(JCR_PRIMARYTYPE, typeName, NAME);
        if (getBoolean(type, JCR_HASORDERABLECHILDNODES)) {
            child.setOrderableChildren(true);
        }
        autoCreateItems(child, type, typeRoot);
        return child;
    }

    private void autoCreateItems(Tree tree, Tree type, Tree typeRoot)
            throws RepositoryException {
        // TODO: use a separate oak:autoCreatePropertyDefinitions
        Tree properties = type.getChildOrNull(OAK_NAMED_PROPERTY_DEFINITIONS);
        if (properties != null) {
            for (Tree definitions : properties.getChildren()) {
                String name = definitions.getName();
                if (name.equals("oak:primaryType")
                        || name.equals("oak:mixinTypes")) {
                    continue;
                } else if (name.equals("oak:uuid")) {
                    name = JCR_UUID;
                }
                for (Tree definition : definitions.getChildren()) {
                    if (getBoolean(definition, JCR_AUTOCREATED)) {
                        if (!tree.hasProperty(name)) {
                            PropertyState property =
                                    autoCreateProperty(name, definition);
                            if (property != null) {
                                tree.setProperty(property);
                            } else {
                                throw new RepositoryException(
                                        "Unable to auto-create value for "
                                        + PathUtils.concat(tree.getPath(), name));
                            }
                        }
                        break;
                    }
                }
            }
        }

        // TODO: use a separate oak:autoCreateChildNodeDefinitions
        Tree childNodes = type.getChildOrNull(OAK_NAMED_CHILD_NODE_DEFINITIONS);
        if (childNodes != null) {
            for (Tree definitions : childNodes.getChildren()) {
                String name = definitions.getName();
                for (Tree definition : definitions.getChildren()) {
                    if (getBoolean(definition, JCR_AUTOCREATED)) {
                        if (!tree.hasChild(name)) {
                            String typeName =
                                    getName(definition, JCR_DEFAULTPRIMARYTYPE);
                            internalAddChild(tree, name, typeName, typeRoot);
                        }
                        break;
                    }
                }
            }
        }
    }

    private PropertyState autoCreateProperty(String name, Tree definition) {
        if (JCR_UUID.equals(name)) {
            String uuid = IdentifierManager.generateUUID();
            return PropertyStates.createProperty(name, uuid, STRING);
        } else if (JCR_CREATED.equals(name)) {
            long now = Calendar.getInstance().getTime().getTime();
            return PropertyStates.createProperty(name, now, DATE);
        } else if (JCR_CREATEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            if (userID != null) {
                return PropertyStates.createProperty(name, userID, STRING);
            }
        } else if (JCR_LASTMODIFIED.equals(name)) {
            long now = Calendar.getInstance().getTime().getTime();
            return PropertyStates.createProperty(name, now, DATE);
        } else if (JCR_LASTMODIFIEDBY.equals(name)) {
            String userID = sessionDelegate.getAuthInfo().getUserID();
            if (userID != null) {
                return PropertyStates.createProperty(name, userID, STRING);
            }
        }

        // does the definition have a default value?
        PropertyState values = definition.getProperty(JCR_DEFAULTVALUES);
        if (values != null) {
            Type<?> type = values.getType();
            if (getBoolean(definition, JCR_MULTIPLE)) {
                return PropertyStates.createProperty(
                        name, values.getValue(type), type);
            } else if (values.count() > 0) {
                type = type.getBaseType();
                return PropertyStates.createProperty(
                        name, values.getValue(type, 0), type);
            }
        }

        return null;
    }

    /**
     * Finds the default primary type for a new child node with the given name.
     *
     * @param typeRoot root of the {@code /jcr:system/jcr:nodeTypes} tree
     * @param parent parent node
     * @param childName name of the new child node
     * @return name of the default type, or {@code null} if not available
     */
    private String getDefaultChildType(
            Tree typeRoot, Tree parent, String childName) {
        List<Tree> types = getEffectiveType(parent, typeRoot);

        // first look for named node definitions
        for (Tree type : types) {
            Tree named = type.getChildOrNull(OAK_NAMED_CHILD_NODE_DEFINITIONS);
            if (named != null) {
                Tree definitions = named.getChildOrNull(childName);
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
            Tree definitions = type.getChildOrNull(OAK_RESIDUAL_CHILD_NODE_DEFINITIONS);
            if (definitions != null) {
                String defaultName =
                        findDefaultPrimaryType(typeRoot, definitions);
                if (defaultName != null) {
                    return defaultName;
                }
            }
        }

        // no matching child node definition found
        return null;
    }

    /**
     * Returns the effective node types of the given node.
     */
    private static List<Tree> getEffectiveType(Tree tree, Tree typeRoot) {
        List<Tree> types = newArrayList();

        String primary = getName(tree, JCR_PRIMARYTYPE);
        if (primary != null) {
            Tree type = typeRoot.getChildOrNull(primary);
            if (type != null) {
                types.add(type);
            }
        }

        for (String mixin : getNames(tree, JCR_MIXINTYPES)) {
            Tree type = typeRoot.getChildOrNull(mixin);
            if (type != null) {
                types.add(type);
            }
        }

        return types;
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
