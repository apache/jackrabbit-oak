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

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_LOCKISDEEP;
import static org.apache.jackrabbit.JcrConstants.JCR_LOCKOWNER;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_LOCKABLE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINED;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINEDS;
import static org.apache.jackrabbit.oak.commons.PathUtils.dropIndexFromName;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_HAS_PROTECTED_RESIDUAL_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_HAS_PROTECTED_RESIDUAL_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PROTECTED_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PROTECTED_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.getBoolean;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.getNames;

import java.util.ArrayList;
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
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.security.AccessControlException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.value.ValueHelper;

/**
 * {@code NodeDelegate} serve as internal representations of {@code Node}s.
 * Most methods of this class throw an {@code InvalidItemStateException}
 * exception if the instance is stale. An instance is stale if the underlying
 * items does not exist anymore.
 */
public class NodeDelegate extends ItemDelegate {

    /** The underlying {@link org.apache.jackrabbit.oak.api.Tree} of this node. */
    private final Tree tree;

    public NodeDelegate(SessionDelegate sessionDelegate, Tree tree) {
        super(sessionDelegate);
        this.tree = tree;
    }

    @Override
    @Nonnull
    public String getName() {
        return tree.getName();
    }

    @Override
    @Nonnull
    public String getPath() {
        return tree.getPath();
    }

    @Override
    @CheckForNull
    public NodeDelegate getParent() {
        return tree.isRoot() || !tree.getParent().exists()
            ? null
            : new NodeDelegate(sessionDelegate, tree.getParent());
    }

    @Override
    public boolean exists() {
        return tree.exists();
    }

    @Override
    @CheckForNull
    public Status getStatus() {
        return tree.getStatus();
    }

    @Nonnull
    public String getIdentifier() throws InvalidItemStateException {
        return IdentifierManager.getIdentifier(getTree());
    }

    @Override
    public boolean isProtected() throws InvalidItemStateException {
        Tree tree = getTree();
        if (tree.isRoot()) {
            return false;
        }

        Tree parent = tree.getParent();
        String nameWithIndex = tree.getName();
        String name = dropIndexFromName(nameWithIndex);
        boolean sns = !name.equals(nameWithIndex);
        Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
        List<Tree> types = TreeUtil.getEffectiveType(parent, typeRoot);

        boolean protectedResidual = false;
        for (Tree type : types) {
            if (contains(TreeUtil.getNames(type, REP_PROTECTED_CHILD_NODES), name)) {
                return true;
            } else if (!protectedResidual) {
                protectedResidual = TreeUtil.getBoolean(
                        type, REP_HAS_PROTECTED_RESIDUAL_CHILD_NODES);
            }
        }

        // Special case: There are one or more protected *residual*
        // child node definitions. Iterate through them to check whether
        // there's a matching, protected one.
        if (protectedResidual) {
            Set<String> typeNames = newHashSet();
            for (Tree type : TreeUtil.getEffectiveType(tree, typeRoot)) {
                typeNames.add(TreeUtil.getName(type, JCR_NODETYPENAME));
                addAll(typeNames, TreeUtil.getNames(type, REP_SUPERTYPES));
            }

            for (Tree type : types) {
                Tree definitions =
                        type.getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
                for (String typeName : typeNames) {
                    Tree definition = definitions.getChild(typeName);
                    if ((!sns || TreeUtil.getBoolean(definition, JCR_SAMENAMESIBLINGS))
                            && TreeUtil.getBoolean(definition, JCR_PROTECTED)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    boolean isProtected(String property) throws InvalidItemStateException {
        Tree tree = getTree();
        Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
        List<Tree> types = TreeUtil.getEffectiveType(tree, typeRoot);

        boolean protectedResidual = false;
        for (Tree type : types) {
            if (contains(TreeUtil.getNames(type, REP_PROTECTED_PROPERTIES), property)) {
                return true;
            } else if (!protectedResidual) {
                protectedResidual = TreeUtil.getBoolean(
                        type, REP_HAS_PROTECTED_RESIDUAL_PROPERTIES);
            }
        }

        // Special case: There are one or more protected *residual*
        // child node definitions. Iterate through them to check whether
        // there's a matching, protected one.
        if (protectedResidual) {
            for (Tree type : types) {
                Tree definitions = type.getChild(REP_RESIDUAL_PROPERTY_DEFINITIONS);
                for (Tree definition : definitions.getChildren()) {
                    // TODO: check for matching property type?
                    if (TreeUtil.getBoolean(definition, JCR_PROTECTED)) {
                        return true;
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
    public PropertyDelegate getPropertyOrNull(String relPath)
            throws RepositoryException {
        Tree parent = tree;
        String name = relPath;

        int slash = relPath.lastIndexOf('/');
        if (slash != -1) {
            parent = getTree(relPath.substring(0, slash));
            name = relPath.substring(slash + 1);
        }

        if (parent != null) {
            PropertyDelegate property =
                    new PropertyDelegate(sessionDelegate, parent, name);
            if (property.exists()) {
                return property;
            }
        }
        return null;
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
        Tree parent = tree;
        String name = relPath;

        int slash = relPath.lastIndexOf('/');
        if (slash != -1) {
            parent = getTree(relPath.substring(0, slash));
            name = relPath.substring(slash + 1);
        }

        return new PropertyDelegate(sessionDelegate, parent, name);
    }

    /**
     * Get the properties of the node
     *
     * @return properties of the node
     */
    @Nonnull
    public Iterator<PropertyDelegate> getProperties() throws InvalidItemStateException {
        return transform(getTree().getProperties().iterator(),
                new Function<PropertyState, PropertyDelegate>() {
                    @Override
                    public PropertyDelegate apply(PropertyState propertyState) {
                        return new PropertyDelegate(sessionDelegate, tree, propertyState.getName());
                    }
                });
    }

    /**
     * Get the number of child nodes
     * <p>
     * If an implementation does know the exact value, it returns it (even if
     * the value is higher than max). If the implementation does not know the
     * exact value, and the child node count is higher than max, it may return
     * Long.MAX_VALUE. The cost of the operation is at most O(max).
     *
     * @param max the maximum value
     * @return number of child nodes of the node
     */
    public long getChildCount(long max) throws InvalidItemStateException {
        return getTree().getChildrenCount(max);
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
        if (relPath.isEmpty()) {
            return this;
        }
        Tree tree = getTree(relPath);
        return tree == null || !tree.exists() ? null : new NodeDelegate(sessionDelegate, tree);
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
        Iterator<Tree> iterator = getTree().getChildren().iterator();
        return transform(
                filter(iterator, new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return tree.exists();
                    }
                }),
                new Function<Tree, NodeDelegate>() {
                    @Override
                    public NodeDelegate apply(Tree tree) {
                        return new NodeDelegate(sessionDelegate, tree);
                    }
                });
    }

    public void orderBefore(String source, String target)
            throws ItemNotFoundException, InvalidItemStateException {
        Tree tree = getTree();
        if (!tree.getChild(source).exists()) {
            throw new ItemNotFoundException("Not a child: " + source);
        } else if (target != null && !tree.getChild(target).exists()) {
            throw new ItemNotFoundException("Not a child: " + target);
        } else {
            tree.getChild(source).orderBefore(target);
        }
    }

    public boolean canAddMixin(String typeName) throws RepositoryException {
        Tree type = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH).getChild(typeName);
        if (type.exists()) {
            return !TreeUtil.getBoolean(type, JCR_IS_ABSTRACT)
                    && TreeUtil.getBoolean(type, JCR_ISMIXIN);
        } else {
            throw new NoSuchNodeTypeException(
                    "Node type " + typeName + " does not exist");
        }
    }

    public void addMixin(String typeName) throws RepositoryException {
        TreeUtil.addMixin(getTree(), typeName, sessionDelegate.getRoot().getTree(NODE_TYPES_PATH), getUserID());
    }

    public void removeMixin(String typeName) throws RepositoryException {
        Tree tree = getTree();
        Set<String> mixins = newLinkedHashSet(getNames(tree, JCR_MIXINTYPES));
        if (!mixins.remove(typeName)) {
            throw new NoSuchNodeTypeException("Mixin " + typeName +" not contained in " + getPath());
        }
        updateMixins(mixins, Collections.singleton(typeName));
    }

    public void setMixins(Set<String> mixinNames) throws RepositoryException {
        Set<String> existingMixins = newLinkedHashSet(getNames(tree, JCR_MIXINTYPES));
        if (existingMixins.isEmpty()) {
            updateMixins(mixinNames, Collections.<String>emptySet());
        } else {
            Set<String> toRemove = newLinkedHashSet();
            for (String name : existingMixins) {
                if (!mixinNames.remove(name)) {
                    toRemove.add(name);
                }
            }
            updateMixins(mixinNames, toRemove);
        }
    }


    public void updateMixins(Set<String> addMixinNames, Set<String> removedOakMixinNames) throws RepositoryException {
        // 1. set all new mixin types including validation
        for (String oakMixinName : addMixinNames) {
            addMixin(oakMixinName);
        }

        if (!removedOakMixinNames.isEmpty()) {
            // 2. retrieve the updated set of mixin types, remove the mixins that should no longer be present
            Set<String> mixinNames = newLinkedHashSet(getNames(getTree(), JCR_MIXINTYPES));
            if (mixinNames.removeAll(removedOakMixinNames)) {
                // FIXME: add mixins to add again as the removal may change the effect of type inheritance as evaluated during #addMixin
                mixinNames.addAll(addMixinNames);
                tree.setProperty(JCR_MIXINTYPES, mixinNames, NAMES);
            }

            // 3. deal with locked nodes
            boolean wasLockable = isNodeType(MIX_LOCKABLE);
            boolean isLockable = isNodeType(MIX_LOCKABLE);
            if (wasLockable && !isLockable && holdsLock(false)) {
                // TODO: This should probably be done in a commit hook
                unlock();
                sessionDelegate.refresh(true);
            }

            // 4. clean up set of properties and child nodes such that all child items
            // have a valid item definition according to the effective node type present
            // after having updated the mixin property. this includes removing all
            // protected properties and child nodes associated with the removed mixin
            // type(s), as there's no way for the client to do that. Other items
            // defined in this mixin type might also need to be removed if there
            // is no longer a matching item definition available.
            Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
            List<Tree> removed = new ArrayList<Tree>();
            for (String name : removedOakMixinNames) {
                removed.add(typeRoot.getChild(name));
            }
            List<Tree> remaining = getNodeTypes(tree, typeRoot);

            for (PropertyState property : tree.getProperties()) {
                String name = property.getName();
                Type<?> type = property.getType();

                Tree oldDefinition = findMatchingPropertyDefinition(removed, name, type, true);
                if (oldDefinition != null) {
                    Tree newDefinition = findMatchingPropertyDefinition(remaining, name, type, true);
                    if (newDefinition == null
                            || (getBoolean(oldDefinition, JCR_PROTECTED)
                            && !getBoolean(newDefinition, JCR_PROTECTED))) {
                        tree.removeProperty(name);
                    }
                }
            }

            for (Tree child : tree.getChildren()) {
                String name = child.getName();
                Set<String> typeNames = newLinkedHashSet();
                for (Tree type : getNodeTypes(child, typeRoot)) {
                    typeNames.add(TreeUtil.getName(type, JCR_NODETYPENAME));
                    addAll(typeNames, getNames(type, REP_SUPERTYPES));
                }

                Tree oldDefinition = findMatchingChildNodeDefinition(removed, name, typeNames);
                if (oldDefinition != null) {
                    Tree newDefinition = findMatchingChildNodeDefinition(remaining, name, typeNames);
                    if (newDefinition == null
                            || (getBoolean(oldDefinition, JCR_PROTECTED)
                            && !getBoolean(newDefinition, JCR_PROTECTED))) {
                        child.remove();
                    }
                }
            }
        }
    }

    /**
     * Set a property
     *
     * @return the set property
     */
    @Nonnull
    public PropertyDelegate setProperty(
            PropertyState propertyState, boolean exactTypeMatch,
            boolean setProtected) throws RepositoryException {
        Tree tree = getTree();
        String name = propertyState.getName();
        Type<?> type = propertyState.getType();

        PropertyState old = tree.getProperty(name);
        if (old != null && old.isArray() && !propertyState.isArray()) {
            throw new ValueFormatException(
                    "Can not assign a single value to multi-valued property: "
                    + propertyState);
        }
        if (old != null && !old.isArray() && propertyState.isArray()) {
            throw new ValueFormatException(
                    "Can not assign multiple values to single valued property: "
                    + propertyState);
        }

        Tree definition = findMatchingPropertyDefinition(
                getNodeTypes(tree), name, type, exactTypeMatch);
        if (definition == null) {
            throw new ConstraintViolationException(
                    "No matching property definition: " + propertyState);
        } else if (!setProtected && TreeUtil.getBoolean(definition, JCR_PROTECTED)) {
            throw new ConstraintViolationException(
                    "Property is protected: " + propertyState);
        }
        Type<?> requiredType =
                Type.fromString(TreeUtil.getString(definition, JCR_REQUIREDTYPE));
        if (requiredType != Type.UNDEFINED) {
            if (TreeUtil.getBoolean(definition, JCR_MULTIPLE)) {
                requiredType = requiredType.getArrayType();
            }
            ValueHelper.checkSupportedConversion(propertyState.getType().tag(), requiredType.tag());
            propertyState = PropertyStates.convert(propertyState, requiredType);
        }

        tree.setProperty(propertyState);
        return new PropertyDelegate(sessionDelegate, tree, name);
    }

    /**
     * Returns the type nodes of the primary and mixin types of the given node.
     *
     * @param tree node
     * @return primary and mixin type nodes
     */
    private List<Tree> getNodeTypes(Tree tree) {
        Root root = sessionDelegate.getRoot();
        return getNodeTypes(tree, root.getTree(NODE_TYPES_PATH));
    }

    private List<Tree> getNodeTypes(Tree tree, Tree typeRoot) {
        // Find applicable node types
        List<Tree> types = newArrayList();
        String primaryName = TreeUtil.getName(tree, JCR_PRIMARYTYPE);
        if (primaryName == null) {
            primaryName = NT_BASE;
        }
        types.add(typeRoot.getChild(primaryName));
        for (String mixinName : TreeUtil.getNames(tree, JCR_MIXINTYPES)) {
            types.add(typeRoot.getChild(mixinName));
        }

        return types;
    }

    private boolean isNodeType(String typeName) {
        return isNodeType(tree, typeName, sessionDelegate.getRoot());
    }

    private boolean isNodeType(Tree tree, String typeName, Root root) {
        return TreeUtil.isNodeType(tree, typeName, root.getTree(NODE_TYPES_PATH));
    }

    private Tree findMatchingPropertyDefinition(
            List<Tree> types, String propertyName, Type<?> propertyType,
            boolean exactTypeMatch) {
        // Escape the property name for looking up a matching definition
        String escapedName;
        if (JCR_PRIMARYTYPE.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_PRIMARY_TYPE;
        } else if (JCR_MIXINTYPES.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_MIXIN_TYPES;
        } else if (JCR_UUID.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_UUID;
        } else {
            escapedName = propertyName;
        }

        String definedType = propertyType.toString();
        String undefinedType = UNDEFINED.toString();
        if (propertyType.isArray()) {
            undefinedType = UNDEFINEDS.toString();
        }

        // First look for a matching named property definition
        Tree fuzzyMatch = null;
        for (Tree type : types) {
            Tree definitions = type
                    .getChild(REP_NAMED_PROPERTY_DEFINITIONS)
                    .getChild(escapedName);
            Tree definition = definitions.getChild(definedType);
            if (definition.exists()) {
                return definition;
            }
            definition = definitions.getChild(undefinedType);
            if (definition.exists()) {
                return definition;
            }
            for (Tree def : definitions.getChildren()) {
                if (propertyType.isArray() == TreeUtil.getBoolean(def, JCR_MULTIPLE)) {
                    if (getBoolean(def, JCR_PROTECTED)) {
                        return null; // no fuzzy matches for protected items
                    } else if (!exactTypeMatch && fuzzyMatch == null) {
                        fuzzyMatch = def;
                    }
                }
            }
        }

        // Then look through any residual property definitions
        for (Tree type : types) {
            Tree definitions = type.getChild(REP_RESIDUAL_PROPERTY_DEFINITIONS);
            Tree definition = definitions.getChild(definedType);
            if (definition.exists()) {
                return definition;
            }
            definition = definitions.getChild(undefinedType);
            if (definition.exists()) {
                return definition;
            }
            if (!exactTypeMatch && fuzzyMatch == null) {
                for (Tree def : definitions.getChildren()) {
                    if (propertyType.isArray() == TreeUtil.getBoolean(def, JCR_MULTIPLE)) {
                        fuzzyMatch = def;
                        break;
                    }
                }
            }
        }

        return fuzzyMatch;
    }

    private Tree findMatchingChildNodeDefinition(
            List<Tree> types, String childNodeName, Set<String> typeNames) {
        // First look for a matching named property definition
        for (Tree type : types) {
            Tree definitions = type
                    .getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)
                    .getChild(childNodeName);
            for (String typeName : typeNames) {
                Tree definition = definitions.getChild(typeName);
                if (definition.exists()) {
                    return definition;
                }
            }
        }

        // Then look through any residual property definitions
        for (Tree type : types) {
            Tree definitions = type
                    .getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
            for (String typeName : typeNames) {
                Tree definition = definitions.getChild(typeName);
                if (definition.exists()) {
                    return definition;
                }
            }
        }

        return null;
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

        Tree typeRoot = sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
        Tree child = TreeUtil.addChild(tree, name, typeName, typeRoot, getUserID());
        return new NodeDelegate(sessionDelegate, child);
    }

    /**
     * Remove this node. This operation never succeeds for the root node.
     *
     * @return {@code true} if the node was removed; {@code false} otherwise.
     */
    @Override
    public boolean remove() throws InvalidItemStateException {
        return getTree().remove();
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

    /**
     * Checks whether this node is locked, either directly or through
     * a deep lock on an ancestor.
     *
     * @return whether this node is locked
     */
    // FIXME: access to locking status should not depend on access rights (OAK-4234)
    public boolean isLocked() {
        return getLock() != null;
    }

    @CheckForNull
    public NodeDelegate getLock() {
        Tree lock = findLock(tree, false);
        if (lock != null) {
            NodeDelegate delegate = new NodeDelegate(sessionDelegate, lock);
            if (delegate.isNodeType(MIX_LOCKABLE)) {
                return delegate;
            } else if (lock.isRoot()) {
                return null;
            } else {
                lock = findLock(lock.getParent(), true);
            }
        }
        return null;
    }

    @CheckForNull
    private Tree findLock(@Nonnull Tree tree, boolean deep) {
        if (holdsLock(tree, deep)) {
            return tree;
        } else if (tree.isRoot()) {
            return null;
        } else {
            return findLock(tree.getParent(), true);
        }
    }

    private boolean holdsLock(Tree tree, boolean deep) {
        // FIXME: access to locking status should not depend on access rights (OAK-4234)
        PropertyState property = tree.getProperty(JCR_LOCKISDEEP);
        return property != null
                && property.getType() == Type.BOOLEAN
                && (!deep || property.getValue(BOOLEAN));
    }

    @CheckForNull
    private Tree findDescendantLock(@Nonnull Tree tree) {
        for (Tree child : tree.getChildren()) {
            if (holdsLock(child, false)) {
                return child;
            } else {
                Tree desc = findDescendantLock(child);
                if (desc != null) {
                    return desc;
                }
            }
        }
        return null;
    }

    /**
     * Checks whether this node holds a lock.
     *
     * @param deep if {@code true}, only check for deep locks
     * @return whether this node holds a lock
     */
    public boolean holdsLock(boolean deep) {
        return holdsLock(tree, deep) && isNodeType(MIX_LOCKABLE);
    }

    public String getLockOwner() {
        PropertyState property = tree.getProperty(JCR_LOCKOWNER);
        if (property != null && property.getType() == Type.STRING) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }

    public boolean isLockOwner(String user) {
        return user != null && user.equals(getLockOwner());
    }

    public void lock(boolean isDeep) throws RepositoryException {
        String path = getPath();

        Root root = sessionDelegate.getContentSession().getLatestRoot();
        Tree tree = root.getTree(path);
        if (!tree.exists()) {
            throw new ItemNotFoundException("Node " + path + " does not exist");
        } else if (!isNodeType(tree, MIX_LOCKABLE, root)) {
            throw new LockException("Node " + path + " is not lockable");
        } else if (tree.hasProperty(JCR_LOCKISDEEP)) {
            throw new LockException("Node " + path + " is already locked");
        }

        // look for locked ancestor
        Tree inheritedLock = findLock(tree, true);
        if (inheritedLock != null) {
            throw new LockException("Node already indirectly locked by " + inheritedLock.getPath());
        }

        // scan for locked descendant
        if (isDeep) {
            Tree descendantLock = findDescendantLock(tree);
            if (descendantLock != null) {
                throw new LockException("Lock conflicts with lock hold by " + descendantLock.getPath());
            }
        }

        try {
            String owner = sessionDelegate.getAuthInfo().getUserID();
            if (owner == null) {
                owner = "";
            }
            tree.setProperty(JCR_LOCKISDEEP, isDeep);
            tree.setProperty(JCR_LOCKOWNER, owner);
            sessionDelegate.commit(root);
        } catch (CommitFailedException e) {
            if (e.isAccessViolation()) {
                throw new AccessControlException(
                        "Access denied to lock node " + path, e);
            } else {
                throw new RepositoryException(
                        "Unable to lock node " + path, e);
            }
        }
    }

    public void unlock() throws RepositoryException {
        String path = getPath();

        Root root = sessionDelegate.getContentSession().getLatestRoot();
        Tree tree = root.getTree(path);
        if (!tree.exists()) {
            throw new ItemNotFoundException("Node " + path + " does not exist");
        } else if (!isNodeType(tree, MIX_LOCKABLE, root)) {
            throw new LockException("Node " + path + " is not lockable");
        } else if (!tree.hasProperty(JCR_LOCKISDEEP)) {
            throw new LockException("Node " + path + " is not locked");
        }

        try {
            tree.removeProperty(JCR_LOCKISDEEP);
            tree.removeProperty(JCR_LOCKOWNER);
            sessionDelegate.commit(root);
        } catch (CommitFailedException e) {
            if (e.isAccessViolation()) {
                throw new AccessControlException(
                        "Access denied to unlock node " + path, e);
            } else {
                throw new RepositoryException(
                        "Unable to unlock node " + path, e);
            }
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("tree", tree).toString();
    }

    //------------------------------------------------------------< internal >---

    @Nonnull // FIXME this should be package private. OAK-672
    public Tree getTree() throws InvalidItemStateException {
        if (!tree.exists()) {
            throw new InvalidItemStateException("Item is stale " + tree.getPath());
        }
        return tree;
    }

    // -----------------------------------------------------------< private >---

    private Tree getTree(String relPath) throws RepositoryException {
        if (PathUtils.isAbsolute(relPath)) {
            throw new RepositoryException("Not a relative path: " + relPath);
        }

        return TreeUtil.getTree(tree, relPath);
    }

    private String getUserID() {
        return sessionDelegate.getAuthInfo().getUserID();
    }

}
