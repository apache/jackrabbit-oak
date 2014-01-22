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
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.query.Query;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterators.emptyIterator;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.collect.Iterators.transform;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;

/**
 * TODO document
 */
public class IdentifierManager {

    private static final Logger log = LoggerFactory.getLogger(IdentifierManager.class);

    private final Root root;
    private final ReadOnlyNodeTypeManager nodeTypeManager;

    public IdentifierManager(Root root) {
        this.root = root;
        this.nodeTypeManager = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
    }

    @Nonnull
    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    @Nonnull
    public static String generateUUID(String hint) {
        UUID uuid = UUID.nameUUIDFromBytes(hint.getBytes(Charsets.UTF_8));
        return uuid.toString();
    }

    public static boolean isValidUUID(String uuid) {
        try {
            UUID.fromString(uuid);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Return the identifier of a tree.
     *
     * @param tree  a tree
     * @return  identifier of {@code tree}
     */
    @Nonnull
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
    @CheckForNull
    public Tree getTree(String identifier) {
        if (identifier.startsWith("/")) {
            return root.getTree(identifier);
        } else {
            int k = identifier.indexOf('/');
            String uuid = k == -1
                    ? identifier
                    : identifier.substring(0, k);

            checkArgument(isValidUUID(uuid), "Not a valid identifier '" + identifier + '\'');

            String basePath = resolveUUID(uuid);
            if (basePath == null) {
                return null;
            } else if (k == -1) {
                return root.getTree(basePath);
            } else {
                return root.getTree(PathUtils.concat(basePath, identifier.substring(k + 1)));
            }
        }
    }

    /**
     * The path of the tree identified by the specified {@code identifier} or {@code null}.
     *
     * @param identifier The identifier of the tree such as exposed by {@link #getIdentifier(Tree)}
     * @return The path of the tree with the given {@code identifier} or {@code null} if no
     *         such tree exists or if the tree is not accessible.
     */
    @CheckForNull
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
    @CheckForNull
    public String getPath(PropertyState referenceValue) {
        int type = referenceValue.getType().tag();
        if (type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE) {
            return resolveUUID(referenceValue);
        } else {
            throw new IllegalArgumentException("Invalid value type");
        }
    }

    /**
     * Returns the path of the tree references by the specified (weak)
     * reference {@code PropertyState}.
     *
     * @param referenceValue A (weak) reference value.
     * @return The tree with the given {@code identifier} or {@code null} if no
     *         such tree exists or isn't accessible to the content session.
     */
    @CheckForNull
    public String getPath(PropertyValue referenceValue) {
        int type = referenceValue.getType().tag();
        if (type == PropertyType.REFERENCE || type == PropertyType.WEAKREFERENCE) {
            return resolveUUID(referenceValue);
        } else {
            throw new IllegalArgumentException("Invalid value type");
        }
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
     * @param nodeTypeNames Node type constraints to be enforced when using
     *                      for reference properties; the specified names are expected to be internal
     *                      oak names.
     * @return A set of oak paths of those reference properties referring to the
     *         specified {@code tree} and matching the constraints.
     */
    @Nonnull
    public Iterable<String> getReferences(boolean weak, Tree tree, final String propertyName, final String... nodeTypeNames) {
        if (!nodeTypeManager.isNodeType(tree, JcrConstants.MIX_REFERENCEABLE)) {
            return Collections.emptySet(); // shortcut
        }

        final String uuid = getIdentifier(tree);
        String reference = weak ? PropertyType.TYPENAME_WEAKREFERENCE : PropertyType.TYPENAME_REFERENCE;
        String pName = propertyName == null ? "*" : propertyName;   // TODO: sanitize against injection attacks!?
        Map<String, ? extends PropertyValue> bindings = Collections.singletonMap("uuid", PropertyValues.newString(uuid));

        try {
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [nt:base] WHERE PROPERTY([" + pName + "], '" + reference + "') = $uuid",
                    Query.JCR_SQL2, Long.MAX_VALUE, 0, bindings, NO_MAPPINGS);
            return findPaths(result, uuid, propertyName, nodeTypeNames,
                    weak ? Type.WEAKREFERENCE : Type.REFERENCE,
                    weak ? Type.WEAKREFERENCES : Type.REFERENCES
                    );
        } catch (ParseException e) {
            log.error("query failed", e);
            return Collections.emptySet();
        }
    }

    private Iterable<String> findPaths(final Result result, final String uuid, final String propertyName,
            final String[] nodeTypeNames, final Type<?> type, final Type<?> types) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return Iterators.concat(
                    transform(result.getRows().iterator(), new RowToPaths()));
            }

            class RowToPaths implements Function<ResultRow, Iterator<String>> {
                @Override
                public Iterator<String> apply(ResultRow row) {
                    final String rowPath = row.getPath();

                    class PropertyToPath implements Function<PropertyState, String> {
                        @Override
                        public String apply(PropertyState pState) {
                            if (pState.isArray()) {
                                if (pState.getType() == types) {
                                    for (String value : pState.getValue(Type.STRINGS)) {
                                        if (uuid.equals(value)) {
                                            return PathUtils.concat(rowPath, pState.getName());
                                        }
                                    }
                                }
                            } else {
                                if (pState.getType() == type) {
                                    if (uuid.equals(pState.getValue(Type.STRING))) {
                                        return PathUtils.concat(rowPath, pState.getName());
                                    }
                                }
                            }
                            return null;
                        }
                    }

                    // skip references from the version storage (OAK-1196)
                    if (!rowPath.startsWith(VersionConstants.VERSION_STORE_PATH)) { 
                        Tree tree = root.getTree(rowPath);
                        if (nodeTypeNames.length == 0 || containsNodeType(tree, nodeTypeNames)) {
                            if (propertyName == null) {
                                return filter(
                                        transform(tree.getProperties().iterator(), new PropertyToPath()),
                                        notNull());
                            } else {
                                // for a fixed property name, we don't need to look for it, but just assume that
                                // the search found the correct one
                                return singletonIterator(PathUtils.concat(rowPath, propertyName));
                            }
                        }
                    }
                    return emptyIterator();
                }

                private boolean containsNodeType(Tree tree, String[] nodeTypeNames) {
                    for (String ntName : nodeTypeNames) {
                        if (nodeTypeManager.isNodeType(tree, ntName)) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        };
    }

    @CheckForNull
    public String resolveUUID(String uuid) {
        return resolveUUID(StringPropertyState.stringProperty("", uuid));
    }

    private String resolveUUID(PropertyState uuid) {
        return resolveUUID(PropertyValues.create(uuid));
    }

    private String resolveUUID(PropertyValue uuid) {
        try {
            Map<String, PropertyValue> bindings = Collections.singletonMap("id", uuid);
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id", Query.JCR_SQL2,
                    Long.MAX_VALUE, 0, bindings, NO_MAPPINGS);

            String path = null;
            for (ResultRow rr : result.getRows()) {
                if (path != null) {
                    log.error("multiple results for identifier lookup: " + path + " vs. " + rr.getPath());
                    return null;
                } else {
                    path = rr.getPath();
                }
            }
            return path;
        } catch (ParseException ex) {
            log.error("query failed", ex);
            return null;
        }
    }

}