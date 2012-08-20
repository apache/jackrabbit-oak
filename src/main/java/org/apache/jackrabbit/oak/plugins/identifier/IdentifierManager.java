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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IdentifierManager...
 */
public class IdentifierManager {

    private static final Logger log = LoggerFactory.getLogger(IdentifierManager.class);

    private final ContentSession contentSession;
    private final Root root;

    public IdentifierManager(ContentSession contentSession, Root root) {
        this.contentSession = contentSession;
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
            throw new RuntimeException("Unexpected error while creating authorizable node", e);
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
            return property.getValue().getString();
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
            return findTreeByJcrUuid(identifier);
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
    public String getPath(CoreValue referenceValue) {
        int type = referenceValue.getType();
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
     * @param tree The tree for which references should be searched.
     * @param propertyName A name constraint for the reference properties;
     * {@code null} if no constraint should be enforced.
     * @param nodeTypeNames Node type constraints to be enforced when using
     * for reference properties.
     * @return A set of oak paths of those reference properties referring to the
     * specified {@code tree} and matching the constraints.
     */
    @Nonnull
    public Set<String> getReferences(Tree tree, String propertyName, String... nodeTypeNames) {
        if (!isReferenceable(tree)) {
            return Collections.emptySet();
        } else {
            String uuid = getIdentifier(tree);
            // TODO execute query.
            throw new UnsupportedOperationException("TODO: Node.getReferences");
        }
    }

    /**
     * Searches all weak reference properties to the specified {@code tree} that
     * match the given name and node type constraints.
     *
     * @param tree The tree for which weak references should be searched.
     * @param propertyName A name constraint for the weak reference properties;
     * {@code null} if no constraint should be enforced.
     * @param nodeTypeNames Node type constraints to be enforced when using
     * for reference properties or {@code null} to avoid node type constraints.
     * @return A set of oak paths of those weak reference properties referring to the
     * specified {@code tree} and matching the constraints.
     */
    @Nonnull
    public Set<String> getWeakReferences(Tree tree, String propertyName, String... nodeTypeNames) {
        if (!isReferenceable(tree)) {
            return Collections.emptySet();
        } else {
            String uuid = getIdentifier(tree);
            // TODO execute query.
            throw new UnsupportedOperationException("TODO: Node.getWeakReferences");
        }
    }

    public boolean isReferenceable(Tree tree) {
        // TODO add proper implementation include node type eval
        return tree.hasProperty(JcrConstants.JCR_UUID);
    }

    @CheckForNull
    private String resolveUUID(String uuid) {
        return resolveUUID(contentSession.getCoreValueFactory().createValue(uuid));
    }

    private String resolveUUID(CoreValue uuid) {
        try {
            Map<String, CoreValue> bindings = Collections.singletonMap("id", uuid);
            Result result = contentSession.getQueryEngine().executeQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id", Query.JCR_SQL2,
                    Long.MAX_VALUE, 0, bindings, new NamePathMapper.Default());

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

    @CheckForNull
    private Tree findTreeByJcrUuid(String uuid) {
        String path = resolveUUID(uuid);
        return (path == null) ? null : root.getTree(path);
    }
}