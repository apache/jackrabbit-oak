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
package org.apache.jackrabbit.oak.plugins.identifier;

import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import javax.jcr.PropertyType;
import javax.jcr.query.Query;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.QueryUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.guava.common.collect.Iterators.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterators.singletonIterator;
import static org.apache.jackrabbit.guava.common.collect.Iterators.transform;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;

/**
 * TODO document
 */
public class IdentifierManager {

    private static final Logger log = LoggerFactory.getLogger(IdentifierManager.class);

    private final Root root;
    private final EffectiveNodeTypeProvider effectiveNodeTypeProvider;

    public IdentifierManager(Root root) {
        this.root = root;
        this.effectiveNodeTypeProvider = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
    }

    /**
     * @deprecated Use {@link UUIDUtils#generateUUID()}
     */
    @NotNull
    public static String generateUUID() {
        return UUIDUtils.generateUUID();
    }

    /**
     * @deprecated Use {@link UUIDUtils#generateUUID(String)}
     */
    @NotNull
    public static String generateUUID(String hint) {
        return UUIDUtils.generateUUID(hint);
    }

    /**
     * @deprecated Use {@link UUIDUtils#isValidUUID(String)} (String)}
     */
    public static boolean isValidUUID(String uuid) {
        return UUIDUtils.isValidUUID(uuid);
    }

    /**
     * Return the identifier of a tree.
     *
     * @param tree  a tree
     * @return  identifier of {@code tree}
     */
    @NotNull
    public static String getIdentifier(Tree tree) {
        PropertyState property = tree.getProperty(JcrConstants.JCR_UUID);
        if (property != null) {
            return property.getValue(Type.STRING);
        } else if (tree.isRoot()) {
            return "/";
        } else {
            String parentId = getIdentifier(tree.getParent());
            return PathUtils.concat(parentId, tree.getName());
        }
    }

    /**
     * The possibly non existing tree identified by the specified {@code identifier} or {@code null}.
     *
     * @param identifier The identifier of the tree such as exposed by {@link #getIdentifier(Tree)}
     * @return The tree with the given {@code identifier} or {@code null} if no
     *         such tree exists.
     */
    @Nullable
    public Tree getTree(String identifier) {
        if (identifier.startsWith("/")) {
            return root.getTree(identifier);
        } else {
            int k = identifier.indexOf('/');
            String uuid = k == -1
                    ? identifier
                    : identifier.substring(0, k);

            checkArgument(UUIDUtils.isValidUUID(uuid), "Not a valid identifier '" + identifier + '\'');

            Tree tree = resolveUUIDToTree(createPropertyValue(uuid));
            if (tree == null) {
                return null;
            } else if (k == -1) {
                return tree;
            } else {
                Iterable<String> subpath = PathUtils.elements(identifier.substring(k + 1));
                for (String element : subpath) {
                    tree = tree.getChild(element);
                }
                return tree;
            }
        }
    }

    @Nullable
    public Tree getTree(@NotNull PropertyValue referenceValue) {
        checkType(referenceValue.getType());
        return resolveUUIDToTree(referenceValue);
    }
    
    /**
     * The path of the tree identified by the specified {@code identifier} or {@code null}.
     *
     * @param identifier The identifier of the tree such as exposed by {@link #getIdentifier(Tree)}
     * @return The path of the tree with the given {@code identifier} or {@code null} if no
     *         such tree exists or if the tree is not accessible.
     */
    @Nullable
    public String getPath(String identifier) {
        Tree tree = getTree(identifier);
        return tree != null && tree.exists()
            ? tree.getPath()
            : null;
    }

    /**
     * Returns the path of the tree references by the specified (weak)
     * reference {@code PropertyState}.
     *
     * @param referenceValue A (weak) reference value.
     * @return The tree with the given {@code identifier} or {@code null} if no
     *         such tree exists or isn't accessible to the content session.
     */
    @Nullable
    public String getPath(PropertyState referenceValue) {
        checkType(referenceValue.getType());
        return resolveUUID(PropertyValues.create(referenceValue));
    }

    /**
     * Returns the path of the tree references by the specified (weak)
     * reference {@code PropertyState}.
     *
     * @param referenceValue A (weak) reference value.
     * @return The tree with the given {@code identifier} or {@code null} if no
     *         such tree exists or isn't accessible to the content session.
     */
    @Nullable
    public String getPath(PropertyValue referenceValue) {
        checkType(referenceValue.getType());
        return resolveUUID(referenceValue);
    }

    /**
     * Searches all reference properties to the specified {@code tree} that match
     * the given name and node type constraints.
     *
     * @param weak          if {@code true} only weak references are returned. Otherwise only
     *                      hard references are returned.
     * @param tree          The tree for which references should be searched.
     * @param propertyName  A name constraint for the reference properties;
     *                      {@code null} if no constraint should be enforced.
     * @return A set of oak paths of those reference properties referring to the
     *         specified {@code tree} and matching the constraints.
     */
    @NotNull
    public Iterable<String> getReferences(boolean weak, @NotNull Tree tree, @Nullable final String propertyName) {
        if (!effectiveNodeTypeProvider.isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
            return Collections.emptySet(); // shortcut
        }

        final String uuid = getIdentifier(tree);
        String reference = weak ? PropertyType.TYPENAME_WEAKREFERENCE : PropertyType.TYPENAME_REFERENCE;
        String pName = propertyName == null ? "*" : QueryUtils.escapeForQuery(propertyName);
        Map<String, ? extends PropertyValue> bindings = Collections.singletonMap("uuid", PropertyValues.newString(uuid));

        try {
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [nt:base] WHERE PROPERTY([" + pName + "], '" + reference + "') = $uuid" +
                    QueryEngine.INTERNAL_SQL2_QUERY,
                    Query.JCR_SQL2, bindings, NO_MAPPINGS);
            return findPaths(result, uuid, propertyName, weak);
        } catch (ParseException e) {
            log.error("query failed", e);
            return Collections.emptySet();
        }
    }

    @NotNull
    private Iterable<String> findPaths(@NotNull final Result result, @NotNull final String uuid,
                                       @Nullable final String propertyName, final boolean weak) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return Iterators.concat(transform(result.getRows().iterator(), new RowToPaths()::apply));
            }

            class RowToPaths implements Function<ResultRow, Iterator<String>> {
                @Override
                public Iterator<String> apply(ResultRow row) {
                    final String rowPath = row.getPath();

                    class PropertyToPath implements Function<PropertyState, String> {
                        @Override
                        public String apply(PropertyState pState) {
                            if (pState.isArray()) {
                                Type<?> types = (weak) ? Type.WEAKREFERENCES : Type.REFERENCES;
                                if (pState.getType() == types) {
                                    for (String value : pState.getValue(Type.STRINGS)) {
                                        if (uuid.equals(value)) {
                                            return PathUtils.concat(rowPath, pState.getName());
                                        }
                                    }
                                }
                            } else {
                                Type<?> type = (weak) ? Type.WEAKREFERENCE : Type.REFERENCE;
                                if (pState.getType() == type && uuid.equals(pState.getValue(Type.STRING))) {
                                    return PathUtils.concat(rowPath, pState.getName());
                                }
                            }
                            return null;
                        }
                    }

                    // skip references from the version storage (OAK-1196)
                    if (!rowPath.startsWith(VersionConstants.VERSION_STORE_PATH)) {
                            if (propertyName == null) {
                                return filter(
                                        transform(root.getTree(rowPath).getProperties().iterator(), new PropertyToPath()::apply),
                                        x -> x != null);
                            } else {
                                // for a fixed property name, we don't need to look for it, but just assume that
                                // the search found the correct one
                                return singletonIterator(PathUtils.concat(rowPath, propertyName));
                            }
                    }
                    return Collections.emptyIterator();
                }
            }
        };
    }

    /**
     * Searches all reference properties to the specified {@code tree} that match
     * the given {@code propertyName} and the specified, mandatory node type
     * constraint ({@code ntName}). In contrast to {@link #getReferences} this
     * method requires all parameters to be specified, which eases the handling
     * of the result set and doesn't require the trees associated with the
     * result set to be resolved.
     *
     * @param tree The tree for which references should be searched.
     * @param propertyName The name of the reference properties.
     * @param ntName The node type name to be used for the query.
     * @param weak if {@code true} only weak references are returned. Otherwise on hard references are returned.
     * @return A set of oak trees containing reference properties referring to the
     *         specified {@code tree} and matching the constraints.
     */
    @NotNull
    public Iterable<Tree> getReferences(@NotNull Tree tree, @NotNull final String propertyName,
                                          @NotNull String ntName, boolean weak) {
        if (!effectiveNodeTypeProvider.isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
            return Collections.emptySet(); // shortcut
        }

        final String uuid = getIdentifier(tree);
        String reference = weak ? PropertyType.TYPENAME_WEAKREFERENCE : PropertyType.TYPENAME_REFERENCE;
        Map<String, ? extends PropertyValue> bindings = Collections.singletonMap("uuid", PropertyValues.newString(uuid));

        try {
            String escapedPropName = QueryUtils.escapeForQuery(propertyName);
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [" + ntName + "] WHERE PROPERTY([" + escapedPropName + "], '" + reference + "') = $uuid" +
                            QueryEngine.INTERNAL_SQL2_QUERY,
                    Query.JCR_SQL2, bindings, NO_MAPPINGS);

            Iterable<Tree> resultTrees = Iterables.transform(result.getRows(), row -> row.getTree(null));
            return Iterables.filter(resultTrees, tree1 -> !tree1.getPath().startsWith(VersionConstants.VERSION_STORE_PATH)
            );
        } catch (ParseException e) {
            log.error("query failed", e);
            return Collections.emptySet();
        }
    }

    @Nullable
    public String resolveUUID(String uuid) {
        return resolveUUID(createPropertyValue(uuid));
    }
    
    @Nullable
    private String resolveUUID(@NotNull PropertyValue uuid) {
        Tree tree = resolveUUIDToTree(uuid);
        return (tree == null) ? null : tree.getPath();
    }

    @Nullable
    private Tree resolveUUIDToTree(@NotNull PropertyValue uuid) {
        try {
            Map<String, PropertyValue> bindings = Collections.singletonMap("id", uuid);
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id " + 
                    "OPTION(INDEX NAME [uuid], INDEX TAG [uuid])" +
                    QueryEngine.INTERNAL_SQL2_QUERY, 
                    Query.JCR_SQL2,
                    bindings, NO_MAPPINGS);

            Tree tree = null;
            for (ResultRow rr : result.getRows()) {
                if (tree != null) {
                    log.error("multiple results for identifier lookup: " + tree.getPath() + " vs. " + rr.getPath());
                    return null;
                } else {
                    tree = rr.getTree(null);
                }
            }
            return tree;
        } catch (ParseException ex) {
            log.error("query failed", ex);
            return null;
        }
    }
    
    @NotNull
    private static PropertyValue createPropertyValue(@NotNull String uuid) {
        return PropertyValues.create(StringPropertyState.stringProperty("", uuid));
    }
    
    private static void checkType(@NotNull Type propertyType) {
        int type = propertyType.tag();
        if (!(type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE)) {
            throw new IllegalArgumentException("Invalid value type");
        }
    }
}
