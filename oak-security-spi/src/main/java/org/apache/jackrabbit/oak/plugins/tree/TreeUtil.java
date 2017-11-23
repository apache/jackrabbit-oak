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
package org.apache.jackrabbit.oak.plugins.tree;

import java.util.Calendar;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.util.ISO8601;

import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Lists.newArrayList;
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
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.dropIndexFromName;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_CREATEDBY;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_LASTMODIFIEDBY;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;

/**
 * Utility providing common operations for the {@link org.apache.jackrabbit.oak.api.Tree} that are not provided
 * by the API.
 */
public final class TreeUtil {

    private TreeUtil() {
    }

    @CheckForNull
    public static String getPrimaryTypeName(@Nonnull Tree tree) {
        return getStringInternal(tree, JcrConstants.JCR_PRIMARYTYPE, Type.NAME);
    }

    @CheckForNull
    public static Iterable<String> getStrings(@Nonnull Tree tree, @Nonnull String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        if (property == null) {
            return null;
        } else {
            return property.getValue(STRINGS);
        }
    }

    @CheckForNull
    public static String getString(@Nonnull Tree tree, @Nonnull String propertyName) {
        return getStringInternal(tree, propertyName, Type.STRING);
    }

    @CheckForNull
    public static String getString(@Nonnull Tree tree, @Nonnull String name, @Nullable String defaultValue) {
        String str = getString(tree, name);
        return (str != null) ? str : defaultValue;
    }

    @CheckForNull
    private static String getStringInternal(@Nonnull Tree tree,
                                            @Nonnull String propertyName,
                                            @Nonnull Type<String> type) {
        PropertyState property = tree.getProperty(propertyName);
        if (property != null && !property.isArray()) {
            return property.getValue(type);
        } else {
            return null;
        }
    }

    /**
     * Returns the boolean representation of the property with the specified
     * {@code propertyName}. If the property does not exist or
     * {@link org.apache.jackrabbit.oak.api.PropertyState#isArray() is an array}
     * this method returns {@code false}.
     *
     * @param tree         The target tree.
     * @param propertyName The name of the property.
     * @return the boolean representation of the property state with the given
     *         name. This utility returns {@code false} if the property does not exist
     *         or is an multivalued property.
     */
    public static boolean getBoolean(@Nonnull Tree tree, @Nonnull String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        return property != null && !property.isArray() && property.getValue(BOOLEAN);
    }

    @CheckForNull
    public static String getName(@Nonnull Tree tree, @Nonnull String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @Nonnull
    public static Iterable<String> getNames(@Nonnull Tree tree, @Nonnull String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    public static long getLong(@Nonnull Tree tree, @Nonnull String name, long defaultValue) {
        PropertyState property = tree.getProperty(name);
        if (property != null && !property.isArray()) {
            return property.getValue(LONG);
        } else {
            return defaultValue;
        }
    }

    /**
     * Return the possibly non existing tree located at the passed {@code path} from
     * the location of the start {@code tree} or {@code null} if {@code path} results
     * in a parent of the root.
     *
     * @param tree  start tree
     * @param path  path from the start tree
     * @return  tree located at {@code path} from {@code start} or {@code null}
     */
    @CheckForNull
    public static Tree getTree(@Nonnull Tree tree, @Nonnull String path) {
        for (String element : PathUtils.elements(path)) {
            if (PathUtils.denotesParent(element)) {
                if (tree.isRoot()) {
                    return null;
                } else {
                    tree = tree.getParent();
                }
            } else if (!PathUtils.denotesCurrent(element)) {
                tree = tree.getChild(element);
            }  // else . -> skip to next element
        }
        return tree;
    }

    public static Tree addChild(
            @Nonnull Tree parent, @Nonnull String name,
            @CheckForNull String typeName, @Nonnull Tree typeRoot,
            @CheckForNull String userID) throws RepositoryException {
        if (typeName == null) {
            typeName = getDefaultChildType(typeRoot, parent, name);
            if (typeName == null) {
                String path = PathUtils.concat(parent.getPath(), name);
                throw new ConstraintViolationException(
                        "No default node type available for " + path);
            }
        }

        Tree type = typeRoot.getChild(typeName);
        if (!type.exists()) {
            throw new NoSuchNodeTypeException(
                    "Node type " + typeName + " does not exist");
        } else if (getBoolean(type, JCR_IS_ABSTRACT)
                // OAK-1013: backwards compatibility for abstract default types
                && !typeName.equals(getDefaultChildType(typeRoot, parent, name))) {
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
        autoCreateItems(child, type, typeRoot, userID);
        return child;
    }

    /**
     * Adds a new child tree with the given name and primary type name.
     * This method is a shortcut for calling {@link Tree#addChild(String)} and
     * {@link Tree#setProperty(String, Object, org.apache.jackrabbit.oak.api.Type)}
     * where the property name is {@link JcrConstants#JCR_PRIMARYTYPE}.
     * Note, that this method in addition verifies if the created tree exists
     * and is accessible in order to avoid {@link IllegalStateException} upon
     * subsequent modification of the new child.
     *
     * @param childName       The Oak name of the child item.
     * @param primaryTypeName The Oak name of the primary node type.
     * @return The new child tree with the specified name and primary type.
     * @throws AccessDeniedException If the child does not exist after creation.
     */
    @Nonnull
    public static Tree addChild(@Nonnull Tree tree, @Nonnull String childName, @Nonnull String primaryTypeName) throws AccessDeniedException {
        Tree child = tree.addChild(childName);
        if (!child.exists()) {
            throw new AccessDeniedException();
        }
        child.setProperty(JcrConstants.JCR_PRIMARYTYPE, primaryTypeName, NAME);
        return child;
    }

    /**
     * Combination of {@link Tree#getChild(String)} and adding a child including
     * its jcr:primaryType property (i.e. {@link Tree#addChild(String)} and
     * {@link Tree#setProperty(PropertyState)}) in case no tree exists with the specified name.
     *
     * @param childName       The Oak name of the child item.
     * @param primaryTypeName The Oak name of the primary node type.
     * @return The new child node with the specified name and primary type.
     * @throws AccessDeniedException If the child does not exist after creation.
     */
    @Nonnull
    public static Tree getOrAddChild(@Nonnull Tree tree, @Nonnull String childName, @Nonnull String primaryTypeName) throws AccessDeniedException {
        Tree child = tree.getChild(childName);
        return (child.exists()) ? child : addChild(tree, childName, primaryTypeName);
    }

    public static void addMixin(@Nonnull Tree tree, @Nonnull String mixinName, @Nonnull Tree typeRoot, @CheckForNull String userID) throws RepositoryException {
        Tree type = typeRoot.getChild(mixinName);
        if (!type.exists()) {
            throw new NoSuchNodeTypeException(
                    "Node type " + mixinName + " does not exist");
        } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
            throw new ConstraintViolationException(
                    "Node type " + mixinName + " is abstract");
        } else if (!getBoolean(type, JCR_ISMIXIN)) {
            throw new ConstraintViolationException(
                    "Node type " + mixinName + " is a not a mixin type");
        }

        List<String> mixins = Lists.newArrayList();
        String primary = getName(tree, JCR_PRIMARYTYPE);
        if (primary != null
                && Iterables.contains(getNames(type, NodeTypeConstants.REP_PRIMARY_SUBTYPES), primary)) {
            return;
        }

        Set<String> subMixins = Sets.newHashSet(getNames(type, NodeTypeConstants.REP_MIXIN_SUBTYPES));
        for (String mixin : getNames(tree, NodeTypeConstants.JCR_MIXINTYPES)) {
            if (mixinName.equals(mixin) || subMixins.contains(mixin)) {
                return;
            }
            mixins.add(mixin);
        }

        mixins.add(mixinName);
        tree.setProperty(JcrConstants.JCR_MIXINTYPES, mixins, NAMES);

        autoCreateItems(tree, type, typeRoot, userID);
    }

    public static void autoCreateItems(@Nonnull Tree tree, @Nonnull Tree type, @Nonnull Tree typeRoot, @CheckForNull String userID)
            throws RepositoryException {
        // TODO: use a separate rep:autoCreatePropertyDefinitions
        Tree properties = type.getChild(REP_NAMED_PROPERTY_DEFINITIONS);
        for (Tree definitions : properties.getChildren()) {
            String name = definitions.getName();
            if (name.equals(NodeTypeConstants.REP_PRIMARY_TYPE)
                    || name.equals(NodeTypeConstants.REP_MIXIN_TYPES)) {
                continue;
            } else if (name.equals(NodeTypeConstants.REP_UUID)) {
                name = JCR_UUID;
            }
            for (Tree definition : definitions.getChildren()) {
                if (getBoolean(definition, JCR_AUTOCREATED)) {
                    if (!tree.hasProperty(name)) {
                        PropertyState property =
                                autoCreateProperty(name, definition, userID);
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

        // TODO: use a separate rep:autoCreateChildNodeDefinitions
        // Note that we use only named, non-SNS child node definitions
        // as there can be no reasonable default values for residual or
        // SNS child nodes
        Tree childNodes = type.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS);
        for (Tree definitions : childNodes.getChildren()) {
            String name = definitions.getName();
            for (Tree definition : definitions.getChildren()) {
                if (getBoolean(definition, JCR_AUTOCREATED)) {
                    if (!tree.hasChild(name)) {
                        String typeName =
                                getName(definition, JCR_DEFAULTPRIMARYTYPE);
                        addChild(tree, name, typeName, typeRoot, userID);
                    }
                    break;
                }
            }
        }
    }

    public static PropertyState autoCreateProperty(@Nonnull String name,
                                                   @Nonnull Tree definition,
                                                   @CheckForNull String userID) {
        if (JCR_UUID.equals(name)) {
            String uuid = UUIDUtils.generateUUID();
            return PropertyStates.createProperty(name, uuid, STRING);
        } else if (JCR_CREATED.equals(name)) {
            String now = ISO8601.format(Calendar.getInstance());
            return PropertyStates.createProperty(name, now, DATE);
        } else if (JCR_CREATEDBY.equals(name)) {
            return PropertyStates.createProperty(name, Strings.nullToEmpty(userID), STRING);
        } else if (JCR_LASTMODIFIED.equals(name)) {
            String now = ISO8601.format(Calendar.getInstance());
            return PropertyStates.createProperty(name, now, DATE);
        } else if (JCR_LASTMODIFIEDBY.equals(name)) {
            return PropertyStates.createProperty(name, Strings.nullToEmpty(userID), STRING);
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
    public static String getDefaultChildType(
            Tree typeRoot, Tree parent, String childName) {
        String name = dropIndexFromName(childName);
        boolean sns = !name.equals(childName);
        List<Tree> types = getEffectiveType(parent, typeRoot);

        // first look for named node definitions
        for (Tree type : types) {
            Tree definitions = type
                    .getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)
                    .getChild(name);
            String defaultName = findDefaultPrimaryType(definitions, sns);
            if (defaultName != null) {
                return defaultName;
            }
        }

        // then check residual definitions
        for (Tree type : types) {
            Tree definitions = type
                    .getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
            String defaultName = findDefaultPrimaryType(definitions, sns);
            if (defaultName != null) {
                return defaultName;
            }
        }

        // no matching child node definition found
        return null;
    }

    /**
     * Returns the effective node types of the given node.
     */
    public static List<Tree> getEffectiveType(Tree tree, Tree typeRoot) {
        List<Tree> types = newArrayList();

        String primary = getName(tree, JCR_PRIMARYTYPE);
        if (primary != null) {
            Tree type = typeRoot.getChild(primary);
            if (type.exists()) {
                types.add(type);
            }
        }

        for (String mixin : getNames(tree, JCR_MIXINTYPES)) {
            Tree type = typeRoot.getChild(mixin);
            if (type.exists()) {
                types.add(type);
            }
        }

        return types;
    }

    public static String findDefaultPrimaryType(Tree definitions, boolean sns) {
        for (Tree definition : definitions.getChildren()) {
            String defaultName = getName(definition, JCR_DEFAULTPRIMARYTYPE);
            if (defaultName != null
                    && (!sns || getBoolean(definition, JCR_SAMENAMESIBLINGS))) {
                return defaultName;
            }
        }
        return null;
    }

    public static boolean isNodeType(Tree tree, String typeName, Tree typeRoot) {
        String primaryName = TreeUtil.getName(tree, JCR_PRIMARYTYPE);
        if (typeName.equals(primaryName)) {
            return true;
        } else if (primaryName != null) {
            Tree type = typeRoot.getChild(primaryName);
            if (contains(getNames(type, REP_SUPERTYPES), typeName)) {
                return true;
            }
        }

        for (String mixinName : getNames(tree, JCR_MIXINTYPES)) {
            if (typeName.equals(mixinName)) {
                return true;
            } else {
                Tree type = typeRoot.getChild(mixinName);
                if (contains(getNames(type, REP_SUPERTYPES), typeName)) {
                    return true;
                }
            }
        }

        return false;
    }


    /**
     * Returns {@code true} if the specified {@code tree} is a read-only tree..
     *
     * @param tree The tree object to be tested.
     * @return {@code true} if the specified tree is an immutable read-only tree.
     * @see org.apache.jackrabbit.oak.plugins.tree.ReadOnly
     */
    public static boolean isReadOnlyTree(@Nonnull Tree tree) {
        return tree instanceof ReadOnly;
    }
}