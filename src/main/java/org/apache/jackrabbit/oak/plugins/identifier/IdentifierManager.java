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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
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

    @CheckForNull
    public Tree getTree(String identifier) {
        if (isValidUUID(identifier)) {
            return findByJcrUuid(identifier);
        } else {
            // TODO as stated in NodeDelegate#getIdentifier() a non-uuid ID should
            // TODO consisting of closest referenceable parent and a relative path
            // TODO irrespective of the accessibility of the parent node(s)
            return root.getTree(identifier);
        }
    }

    @Nonnull
    public Set<String> getReferences(Tree tree, String name) {
        if (!isReferenceable(tree)) {
            return Collections.emptySet();
        } else {
            String uuid = getIdentifier(tree);
            // TODO execute query.
            throw new UnsupportedOperationException("TODO: Node.getReferences");
        }
    }

    @Nonnull
    public Set<String> getWeakReferences(Tree tree, String name) {
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
    private Tree findByJcrUuid(String id) {
        try {
            Map<String, CoreValue> bindings = Collections.singletonMap("id", contentSession.getCoreValueFactory().createValue(id));

            Result result = contentSession.getQueryEngine().executeQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id", Query.JCR_SQL2,
                    contentSession, Long.MAX_VALUE, 0, bindings, new NamePathMapper.Default());

            String path = null;
            for (ResultRow rr : result.getRows()) {
                if (path != null) {
                    log.error("multiple results for identifier lookup: " + path + " vs. " + rr.getPath());
                    return null;
                } else {
                    path = rr.getPath();
                }
            }
            return path == null ? null : root.getTree(path);
        } catch (ParseException ex) {
            log.error("query failed", ex);
            return null;
        }
    }
}