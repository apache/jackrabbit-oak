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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;

import org.apache.jackrabbit.guava.common.collect.Iterables;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.guava.common.collect.Iterables.contains;
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

    @Nullable
    public static String getPrimaryTypeName(@NotNull Tree tree) {
        return getStringInternal(tree, JcrConstants.JCR_PRIMARYTYPE, Type.NAME);
    }

    @Nullable
    public static String getPrimaryTypeName(@NotNull Tree tree, @NotNull LazyValue<Tree> readOnlyTree) {
        String primaryTypeName = null;
        if (tree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            primaryTypeName = TreeUtil.getPrimaryTypeName(tree);
        } else if (tree.getStatus() != Tree.Status.NEW) {
            // OAK-2441: for backwards compatibility with Jackrabbit 2.x try to
            // read the primary type from the underlying node state.
            primaryTypeName = TreeUtil.getPrimaryTypeName(readOnlyTree.get());
        }
        return primaryTypeName;
    }

    @NotNull
    public static Iterable<String> getMixinTypeNames(@NotNull Tree tree) {
        return TreeUtil.getNames(tree, JcrConstants.JCR_MIXINTYPES);
    }

    @NotNull
    public static Iterable<String> getMixinTypeNames(@NotNull Tree tree, @NotNull LazyValue<Tree> readOnlyTree) {
        Iterable<String> mixinNames = emptyList();
        if (tree.hasProperty(JcrConstants.JCR_MIXINTYPES)) {
            mixinNames = getMixinTypeNames(tree);
        } else if (tree.getStatus() != Tree.Status.NEW) {
            // OAK-2441: for backwards compatibility with Jackrabbit 2.x try to
            // read the primary type from the underlying node state.
            mixinNames = TreeUtil.getNames(readOnlyTree.get(), JcrConstants.JCR_MIXINTYPES);
        }
        return mixinNames;
    }

    @Nullable
    public static Iterable<String> getStrings(@NotNull Tree tree, @NotNull String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        if (property == null) {
            return null;
        } else {
            return property.getValue(STRINGS);
        }
    }

    @Nullable
    public static String getString(@NotNull Tree tree, @NotNull String propertyName) {
        return getStringInternal(tree, propertyName, Type.STRING);
    }

    @Nullable
    public static String getString(@NotNull Tree tree, @NotNull String name, @Nullable String defaultValue) {
        String str = getString(tree, name);
        return (str != null) ? str : defaultValue;
    }

    @Nullable
    private static String getStringInternal(@NotNull Tree tree,
                                            @NotNull String propertyName,
                                            @NotNull Type<String> type) {
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
    public static boolean getBoolean(@NotNull Tree tree, @NotNull String propertyName) {
        PropertyState property = tree.getProperty(propertyName);
        return property != null && !property.isArray() && property.getValue(BOOLEAN);
    }

    @Nullable
    public static String getName(@NotNull Tree tree, @NotNull String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAME) {
            return property.getValue(NAME);
        } else {
            return null;
        }
    }

    @NotNull
    public static Iterable<String> getNames(@NotNull Tree tree, @NotNull String name) {
        PropertyState property = tree.getProperty(name);
        if (property != null && property.getType() == NAMES) {
            return property.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    public static long getLong(@NotNull Tree tree, @NotNull String name, long defaultValue) {
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
    @Nullable
    public static Tree getTree(@NotNull Tree tree, @NotNull String path) {
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

    @NotNull
    public static Tree addChild(@NotNull Tree parent, @NotNull String name, @Nullable String typeName, @NotNull Tree typeRoot, @Nullable String userID) throws RepositoryException {
        if (typeName == null) {
            typeName = getDefaultChildType(typeRoot, parent, name);
            if (typeName == null) {
                String path = PathUtils.concat(parent.getPath(), name);
                throw new ConstraintViolationException("No default node type available for " + path);
            }
        }

        Tree type = typeRoot.getChild(typeName);
        if (!type.exists()) {
            throw noSuchNodeTypeException(typeName);
        } else if (getBoolean(type, JCR_IS_ABSTRACT)
                // OAK-1013: backwards compatibility for abstract default types
                && !typeName.equals(getDefaultChildType(typeRoot, parent, name))) {
            throw abstractNodeTypeException(typeName);
        } else if (getBoolean(type, JCR_ISMIXIN)) {
            throw mixinTypeException(typeName, false);
        }

        Tree child = parent.addChild(name);
        child.setProperty(JCR_PRIMARYTYPE, typeName, NAME);
        if (getBoolean(type, JCR_HASORDERABLECHILDNODES)) {
            child.setOrderableChildren(true);
        }
        autoCreateItems(child, type, typeRoot, userID);
        return child;
    }
    
    private static NoSuchNodeTypeException noSuchNodeTypeException(@NotNull String typeName) {
        return new NoSuchNodeTypeException(exceptionMessage(typeName, "does not exist"));
    }
    
    private static ConstraintViolationException abstractNodeTypeException(@NotNull String typeName) {
        return new ConstraintViolationException(exceptionMessage(typeName, "is abstract"));
    }
    
    private static ConstraintViolationException mixinTypeException(@NotNull String typeName, boolean mixinExpected) {
        String not = (mixinExpected) ? "not " : "";
        return new ConstraintViolationException(exceptionMessage(typeName, "is "+not+"a mixin type"));
    }
    
    private static String exceptionMessage(@NotNull String typeName, @NotNull String reason) {
        return String.format("Node type %s %s", typeName, reason);
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
    @NotNull
    public static Tree addChild(@NotNull Tree tree, @NotNull String childName, @NotNull String primaryTypeName) throws AccessDeniedException {
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
    @NotNull
    public static Tree getOrAddChild(@NotNull Tree tree, @NotNull String childName, @NotNull String primaryTypeName) throws AccessDeniedException {
        Tree child = tree.getChild(childName);
        return (child.exists()) ? child : addChild(tree, childName, primaryTypeName);
    }

    /**
     * Add a mixin type to the given {@code tree}. The implementation checks
     * the effective type of the tree and will not add the mixin if it
     * determines the tree is already of type {@code mixinName} through the
     * currently set primary or mixin types, directly or indirectly by type
     * inheritance.
     *
     * @param tree tree where the mixin type is to be added.
     * @param mixinName name of the mixin to add.
     * @param typeRoot tree where type information is stored.
     * @param userID user id or {@code null} if unknown.
     * @throws RepositoryException if {@code mixinName} does not refer to an
     *      existing type or the type it refers to is abstract or the type it
     *      refers to is a primary type.
     */
    public static void addMixin(@NotNull Tree tree, @NotNull String mixinName, @NotNull Tree typeRoot, @Nullable String userID) throws RepositoryException {
        addMixin(tree, t -> getNames(t, JCR_MIXINTYPES), mixinName, typeRoot, userID);
    }

    /**
     * Add a mixin type to the given {@code tree}. The implementation checks
     * the effective type of the tree and will not add the mixin if it
     * determines the tree is already of type {@code mixinName} through the
     * currently set primary or mixin types, directly or indirectly by type
     * inheritance.
     *
     * @param tree tree where the mixin type is to be added.
     * @param existingMixins function to get the currently set mixin types from
     *      a tree.
     * @param mixinName name of the mixin to add.
     * @param typeRoot tree where type information is stored.
     * @param userID user id or {@code null} if unknown.
     * @throws RepositoryException if {@code mixinName} does not refer to an
     *      existing type or the type it refers to is abstract or the type it
     *      refers to is a primary type.
     */
    public static void addMixin(@NotNull Tree tree,
                                @NotNull Function<Tree, Iterable<String>> existingMixins,
                                @NotNull String mixinName,
                                @NotNull Tree typeRoot,
                                @Nullable String userID) throws RepositoryException {
        Tree type = typeRoot.getChild(mixinName);
        if (!type.exists()) {
            throw noSuchNodeTypeException(mixinName);
        } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
            throw abstractNodeTypeException(mixinName);
        } else if (!getBoolean(type, JCR_ISMIXIN)) {
            throw mixinTypeException(mixinName, true);
        }

        List<String> mixins = new ArrayList<>();
        String primary = getName(tree, JCR_PRIMARYTYPE);
        if (primary != null && Iterables.contains(getNames(type, NodeTypeConstants.REP_PRIMARY_SUBTYPES), primary)) {
            return;
        }

        Set<String> subMixins = CollectionUtils.toSet(getNames(type, NodeTypeConstants.REP_MIXIN_SUBTYPES));

        for (String mixin : existingMixins.apply(tree)) {
            if (mixinName.equals(mixin) || subMixins.contains(mixin)) {
                return;
            }
            mixins.add(mixin);
        }

        mixins.add(mixinName);
        tree.setProperty(JCR_MIXINTYPES, mixins, NAMES);

        autoCreateItems(tree, type, typeRoot, userID);
    }

    public static void autoCreateItems(@NotNull Tree tree, @NotNull Tree type, @NotNull Tree typeRoot, @Nullable String userID)
            throws RepositoryException {
        
        autoCreateProperties(tree, type, userID);

        // Note that we use only named, non-SNS child node definitions
        // as there can be no reasonable default values for residual or
        // SNS child nodes
        Tree childNodes = type.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS);
        for (Tree definitions : childNodes.getChildren()) {
            String name = definitions.getName();
            Tree definition = getAutoCreatedDefinition(definitions);
            if (definition != null && !tree.hasChild(name)) {
                String typeName = getName(definition, JCR_DEFAULTPRIMARYTYPE);
                addChild(tree, name, typeName, typeRoot, userID);
            }
        }
    }
    
    private static void autoCreateProperties(@NotNull Tree tree, @NotNull Tree type, @Nullable String userID) throws RepositoryException {
        Tree properties = type.getChild(REP_NAMED_PROPERTY_DEFINITIONS);
        for (Tree definitions : properties.getChildren()) {
            String name = definitions.getName();
            if (name.equals(NodeTypeConstants.REP_PRIMARY_TYPE) || name.equals(NodeTypeConstants.REP_MIXIN_TYPES)) {
                continue;
            } else if (name.equals(NodeTypeConstants.REP_UUID)) {
                name = JCR_UUID;
            }
            Tree definition = getAutoCreatedDefinition(definitions);
            if (definition != null && !tree.hasProperty(name)) {
                PropertyState property = autoCreateProperty(name, definition, userID);
                if (property != null) {
                    tree.setProperty(property);
                } else {
                    throw new RepositoryException("Unable to auto-create value for " + PathUtils.concat(tree.getPath(), name));
                }
            }
        }
    }
    
    @Nullable
    private static Tree getAutoCreatedDefinition(@NotNull Tree definitions) {
        for (Tree definition : definitions.getChildren()) {
            if (getBoolean(definition, JCR_AUTOCREATED)) {
                return definition;
            }
        }
        return null;
    }

    @Nullable
    public static PropertyState autoCreateProperty(@NotNull String name,
                                                   @NotNull Tree definition,
                                                   @Nullable String userID) {
        switch (name) {
            case JCR_UUID:
                return PropertyStates.createProperty(name, UUIDUtils.generateUUID(), STRING);
            case JCR_CREATED:
            case JCR_LASTMODIFIED:    
                return PropertyStates.createProperty(name, ISO8601.format(Calendar.getInstance()), DATE);
            case JCR_CREATEDBY:
            case JCR_LASTMODIFIEDBY:
                return PropertyStates.createProperty(name, Objects.toString(userID, ""), STRING);
            default:
                // no default, continue inspecting the definition
        }

        // does the definition have a default value?
        PropertyState values = definition.getProperty(JCR_DEFAULTVALUES);
        if (values != null) {
            Type<?> type = values.getType();
            if (getBoolean(definition, JCR_MULTIPLE)) {
                return PropertyStates.createProperty(name, values.getValue(type), type);
            } else if (values.count() > 0) {
                type = type.getBaseType();
                return PropertyStates.createProperty(name, values.getValue(type, 0), type);
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
    @Nullable
    public static String getDefaultChildType(@NotNull Tree typeRoot, @NotNull Tree parent, @NotNull String childName) {
        String name = dropIndexFromName(childName);
        boolean sns = !name.equals(childName);
        List<Tree> types = getEffectiveType(parent, typeRoot);

        // first look for named node definitions
        for (Tree type : types) {
            Tree definitions = type.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS).getChild(name);
            String defaultName = findDefaultPrimaryType(definitions, sns);
            if (defaultName != null) {
                return defaultName;
            }
        }

        // then check residual definitions
        for (Tree type : types) {
            Tree definitions = type.getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
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
    @NotNull
    public static List<Tree> getEffectiveType(@NotNull Tree tree, @NotNull Tree typeRoot) {
        List<Tree> types = new ArrayList<>();

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

    @Nullable
    public static String findDefaultPrimaryType(@NotNull Tree definitions, boolean sns) {
        for (Tree definition : definitions.getChildren()) {
            String defaultName = getName(definition, JCR_DEFAULTPRIMARYTYPE);
            if (defaultName != null && (!sns || getBoolean(definition, JCR_SAMENAMESIBLINGS))) {
                return defaultName;
            }
        }
        return null;
    }

    public static boolean isNodeType(@NotNull Tree tree, @NotNull String typeName, @NotNull Tree typeRoot) {
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
    public static boolean isReadOnlyTree(@NotNull Tree tree) {
        return tree instanceof ReadOnly;
    }
}
