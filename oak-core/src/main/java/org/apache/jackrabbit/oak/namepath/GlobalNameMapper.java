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
package org.apache.jackrabbit.oak.namepath;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.name.NamespaceConstants.NAMESPACES_PATH;
import static org.apache.jackrabbit.oak.plugins.name.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.encodeUri;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NT_REP_UNSTRUCTURED;

/**
 * Name mapper with no local prefix remappings. URI to prefix mappings
 * are read from the repository when for transforming expanded JCR names
 * to prefixed Oak names.
 * <p>
 * Note that even though this class could be used to verify that all prefixed
 * names have valid prefixes, we explicitly don't do that since this is a
 * fairly performance-sensitive part of the codebase and since normally the
 * NameValidator and other consistency checks already ensure that all names
 * being committed or already in the repository should be valid. A separate
 * consistency check can be used if needed to locate and fix any Oak names
 * with invalid namespace prefixes.
 */
public class GlobalNameMapper implements NameMapper {

    protected static boolean isHiddenName(String name) {
        return name.startsWith(":");
    }

    protected static boolean isExpandedName(String name) {
        if (name.startsWith("{")) {
            int brace = name.indexOf('}', 1);
            return brace != -1 && name.substring(1, brace).indexOf(':') != -1;
        } else {
            return false;
        }
    }

    private static Tree setupNamespaces(Map<String, String> global) {
        NodeBuilder namespaces = EmptyNodeState.EMPTY_NODE.builder();
        namespaces.setProperty(JCR_PRIMARYTYPE, NT_REP_UNSTRUCTURED, NAME);
        for (Entry<String, String> entry : global.entrySet()) {
            namespaces.setProperty(
                    Namespaces.escapePropertyKey(entry.getKey()),
                    entry.getValue());
        }
        Namespaces.buildIndexNode(namespaces);
        return new ImmutableTree(namespaces.getNodeState());
    }

    protected final Tree namespaces;
    private final Tree nsdata;

    public GlobalNameMapper(Root root) {
        this(root.getTree(NAMESPACES_PATH));
    }

    public GlobalNameMapper(Map<String, String> namespaces) {
        this(setupNamespaces(namespaces));
    }

    private GlobalNameMapper(Tree namespaces) {
        this.namespaces = namespaces;
        this.nsdata = namespaces.getChild(REP_NSDATA);
    }

    @Override @Nonnull
    public String getJcrName(@Nonnull String oakName) {
        // Sanity checks, can be turned to assertions if needed for performance
        checkNotNull(oakName);
        checkArgument(!isHiddenName(oakName), oakName);
        checkArgument(!isExpandedName(oakName), oakName);

        return oakName;
    }

    @Override @CheckForNull
    public String getOakNameOrNull(@Nonnull String jcrName) {
        if (jcrName.startsWith("{")) {
            return getOakNameFromExpanded(jcrName);
        }

        return jcrName;
    }

    @Nonnull
    @Override
    public String getOakName(@Nonnull String jcrName) throws RepositoryException {
        String oakName = getOakNameOrNull(jcrName);
        if (oakName == null) {
            throw new RepositoryException("Invalid jcr name " + jcrName);
        }
        return oakName;
    }

    @Override
    public Map<String, String> getSessionLocalMappings() {
        return Collections.emptyMap();
    }

    @CheckForNull
    protected String getOakNameFromExpanded(String expandedName) {
        checkArgument(expandedName.startsWith("{"));

        int brace = expandedName.indexOf('}', 1);
        if (brace > 0) {
            String uri = expandedName.substring(1, brace);
            if (uri.isEmpty()) {
                return expandedName.substring(2); // special case: {}name
            } else if (uri.indexOf(':') != -1) {
                // It's an expanded name, look up the namespace prefix
                String oakPrefix = getOakPrefixOrNull(uri);
                if (oakPrefix != null) {
                    return oakPrefix + ':' + expandedName.substring(brace + 1);
                } else {
                    return null; // no matching namespace prefix
                }
            }
        }

        return expandedName; // not an expanded name
    }

    @CheckForNull
    protected String getOakPrefixOrNull(String uri) {
        PropertyState mapping = nsdata.getProperty(encodeUri(uri));
        if (mapping != null && mapping.getType() == STRING) {
            return mapping.getValue(STRING);
        }
        return null;
    }

}
