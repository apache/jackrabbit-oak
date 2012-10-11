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

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.query.Query;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.value.PropertyValue;
import org.apache.jackrabbit.oak.value.PropertyValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * IdentifierManager...
 */
public class IdentifierManager {

    private static final Logger log = LoggerFactory.getLogger(IdentifierManager.class);

    private final Root root;

    public IdentifierManager(Root root) {
        this.root = root;
    }

    @Nonnull
    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    @Nonnull
    public static String generateUUID(String hint) {
        try {
            UUID uuid = UUID.nameUUIDFromBytes(hint.getBytes("UTF-8"));
            return uuid.toString();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpected error while creating uuid", e);
        }
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
     *
     * @param uuid
     * @throws IllegalArgumentException If the specified uuid has an invalid format.
     */
    public static void checkUUIDFormat(String uuid) throws IllegalArgumentException {
        UUID.fromString(uuid);
    }

    @Nonnull
    public String getIdentifier(Tree tree) {
        PropertyState property = tree.getProperty(JcrConstants.JCR_UUID);
        if (property == null) {
            // TODO calculate the identifier from closest referenceable parent
            // TODO and a relative path irrespective of the accessibility of the parent node(s)
            return tree.getPath();
        } else {
            return property.getValue(STRING);
        }
    }

    /**
     * The tree identified by the specified {@code identifier} or {@code null}.
     *
     * @param identifier The identifier of the Node such as exposed by {@link javax.jcr.Node#getIdentifier()}
     * @return The tree with the given {@code identifier} or {@code null} if no
     * such tree exists or isn't accessible to the content session.
     */
    @CheckForNull
    public Tree getTree(String identifier) {
        if (isValidUUID(identifier)) {
            String path = resolveUUID(identifier);
            return (path == null) ? null : root.getTree(path);
        } else {
            // TODO as stated in NodeDelegate#getIdentifier() a non-uuid ID should
            // TODO consisting of closest referenceable parent and a relative path
            // TODO irrespective of the accessibility of the parent node(s)
            return root.getTree(identifier);
        }
    }

    /**
     * The path of the tree identified by the specified {@code identifier} or {@code null}.
     *
     * @param identifier The identifier of the Tree such as exposed by {@link javax.jcr.Node#getIdentifier()}
     * @return The tree with the given {@code identifier} or {@code null} if no
     * such tree exists or isn't accessible to the content session.
     */
    @CheckForNull
    public String getPath(String identifier) {
        if (isValidUUID(identifier)) {
            return resolveUUID(identifier);
        } else {
            // TODO as stated in NodeDelegate#getIdentifier() a non-uuid ID should
            // TODO consisting of closest referenceable parent and a relative path
            // TODO irrespective of the accessibility of the parent node(s)
            Tree tree = root.getTree(identifier);
            return tree == null ? null : tree.getPath();
        }
    }

    /**
     * Returns the path of the tree references by the specified (weak)
     * reference {@code CoreValue value}.
     *
     * @param referenceValue A (weak) reference value.
     * @return The tree with the given {@code identifier} or {@code null} if no
     * such tree exists or isn't accessible to the content session.
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
     * Searches all reference properties to the specified {@code tree} that match
     * the given name and node type constraints.
     *
     * @param weak  if {@code true} only weak references are returned. Otherwise only
     *              hard references are returned.
     * @param tree The tree for which references should be searched.
     * @param propertyName A name constraint for the reference properties;
     * {@code null} if no constraint should be enforced.
     * @param nodeTypeNames Node type constraints to be enforced when using
     * for reference properties.
     * @return A set of oak paths of those reference properties referring to the
     * specified {@code tree} and matching the constraints.
     */
    @Nonnull
    public Set<String> getReferences(boolean weak, Tree tree, final String propertyName, final String... nodeTypeNames) {
        if (!isReferenceable(tree)) {
            return Collections.emptySet();
        } else {
            try {
                final String uuid = getIdentifier(tree);
                String reference = weak ? PropertyType.TYPENAME_WEAKREFERENCE : PropertyType.TYPENAME_REFERENCE;
                String pName = propertyName == null ? "*" : propertyName;   // TODO: sanitize against injection attacks!?
                Map<String, ? extends PropertyValue> bindings = Collections.singletonMap("uuid", PropertyValues.newString(uuid));

                Result result = root.getQueryEngine().executeQuery(
                        "SELECT * FROM [nt:base] WHERE PROPERTY([" + pName + "], '" + reference + "') = $uuid",
                        Query.JCR_SQL2, Long.MAX_VALUE, 0, bindings, root, new NamePathMapper.Default());

                Iterable<String> paths = Iterables.transform(result.getRows(),
                        new Function<ResultRow, String>() {
                            @Override
                            public String apply(ResultRow row) {
                                String pName = propertyName == null
                                        ? findProperty(row.getPath(), uuid)
                                        : propertyName;
                                return PathUtils.concat(row.getPath(), pName);
                            }
                        });

                if (nodeTypeNames.length > 0) {
                    paths = Iterables.filter(paths, new Predicate<String>() {
                        @Override
                        public boolean apply(String path) {
                            Tree tree = root.getTree(PathUtils.getParentPath(path));
                            if (tree != null) {
                                for (String ntName : nodeTypeNames) {
                                    if (hasType(tree, ntName)) {
                                        return true;
                                    }
                                }
                            }
                            return false;
                        }
                    });
                }

                return Sets.newHashSet(paths);
            } catch (ParseException e) {
                log.error("query failed", e);
                return Collections.emptySet();
            }
        }
    }

    private String findProperty(String path, final String uuid) {
        // TODO (OAK-220) PropertyState can only be accessed from parent tree
        Tree tree = root.getTree(path);
        assert tree != null;
        final PropertyState refProp = Iterables.find(tree.getProperties(), new Predicate<PropertyState>() {
            @Override
            public boolean apply(PropertyState pState) {
                if (pState.isArray()) {
                    for (String value : pState.getValue(Type.STRINGS)) {
                        if (uuid.equals(value)) {
                            return true;
                        }
                    }
                    return false;
                }
                else {
                    return uuid.equals(pState.getValue(STRING));
                }
            }
        });

        return refProp.getName();
    }

    private static boolean hasType(Tree tree, String ntName) {
        // TODO use NodeType.isNodeType to determine type membership instead of equality on type names
        PropertyState pType = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (pType != null) {
            String primaryType = pType.getValue(STRING);
            if (ntName.equals(primaryType)) {
                return true;
            }
        }

        PropertyState pMixin = tree.getProperty(JcrConstants.JCR_MIXINTYPES);
        if (pMixin != null) {
            for (String mixinType : pMixin.getValue(STRINGS)) {
                if (ntName.equals(mixinType)) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean isReferenceable(Tree tree) {
        // TODO add proper implementation include node type eval
        return tree.hasProperty(JcrConstants.JCR_UUID);
    }

    @CheckForNull
    private String resolveUUID(String uuid) {
        return resolveUUID(PropertyStates.stringProperty("", uuid));
    }

    private String resolveUUID(PropertyState uuid) {
        try {
            Map<String, PropertyValue> bindings = Collections.singletonMap("id", PropertyValues.create(uuid));
            Result result = root.getQueryEngine().executeQuery(
                    "SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id", Query.JCR_SQL2,
                    Long.MAX_VALUE, 0, bindings, root, new NamePathMapper.Default());

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