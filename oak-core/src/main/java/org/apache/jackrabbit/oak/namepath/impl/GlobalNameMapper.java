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
package org.apache.jackrabbit.oak.namepath.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.NAMESPACES_PATH;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NSDATA;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_PREFIXES;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_URIS;
import static org.apache.jackrabbit.oak.plugins.name.Namespaces.encodeUri;
import static org.apache.jackrabbit.oak.plugins.tree.RootFactory.createReadOnlyRoot;
import static org.apache.jackrabbit.oak.plugins.tree.TreeFactory.createReadOnlyTree;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.getString;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.getStrings;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

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

    private final Root root;
    private Tree namespaces;
    private Tree nsdata;

    public GlobalNameMapper(Root root) {
        this.root = root;
        init();
    }

    public GlobalNameMapper(NodeState root) {
        this(createReadOnlyRoot(root));
    }

    private void init() {
        if (root != null) {
            this.namespaces = root.getTree(NAMESPACES_PATH);
            this.nsdata = namespaces.getChild(REP_NSDATA);
        }
    }

    public GlobalNameMapper(Map<String, String> mappings) {
        NodeBuilder forward = EMPTY_NODE.builder();
        NodeBuilder reverse = EMPTY_NODE.builder();

        for (Entry<String, String> entry : mappings.entrySet()) {
            String prefix = entry.getKey();
            if (!prefix.isEmpty()) {
                String uri = entry.getValue();
                forward.setProperty(prefix, uri);
                reverse.setProperty(encodeUri(uri), prefix);
            }
        }
        reverse.setProperty(REP_PREFIXES, mappings.keySet(), STRINGS);
        reverse.setProperty(REP_URIS, mappings.values(), STRINGS);

        this.root = null;
        this.namespaces = createReadOnlyTree(forward.getNodeState());
        this.nsdata = createReadOnlyTree(reverse.getNodeState());
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

    @Override @Nonnull
    public String getOakName(@Nonnull String jcrName) throws RepositoryException {
        String oakName = getOakNameOrNull(jcrName);
        if (oakName == null) {
            throw new RepositoryException("Invalid jcr name " + jcrName);
        }
        return oakName;
    }

    @Nonnull
    @Override
    public Map<String, String> getSessionLocalMappings() {
        return emptyMap();
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
    protected synchronized String getOakPrefixOrNull(String uri) {
        if (uri.isEmpty()) {
            return uri;
        }

        return getNsData(encodeUri(uri));
    }

    @CheckForNull
    protected synchronized String getOakURIOrNull(String prefix) {
        if (prefix.isEmpty()) {
            return prefix;
        }

        return getNamespacesProperty(prefix);
    }

    protected String getNamespacesProperty(String prefix) {
        return getString(namespaces, prefix);
    }

    private String getNsData(String uri) {
        return getString(nsdata, uri);
    }

    protected Iterable<String> getPrefixes() {
        Iterable<String> prefs = getStrings(nsdata, REP_PREFIXES);
        if (prefs != null) {
            return prefs;
        } else {
            return emptyList();
        }
    }

    public void onSessionRefresh() {
        init();
    }

}
