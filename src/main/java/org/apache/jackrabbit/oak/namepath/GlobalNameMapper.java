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

import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
public abstract class GlobalNameMapper implements NameMapper {

    @Override @Nonnull
    public String getJcrName(@Nonnull String oakName) {
        // Sanity checks, can be turned to assertions if needed for performance
        checkNotNull(oakName);
        checkArgument(!oakName.startsWith(":")); // hidden name
        checkArgument(!oakName.startsWith("{")); // expanded name

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
    public boolean hasSessionLocalMappings() {
        return false;
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

    protected abstract Map<String, String> getNamespaceMap();

    @CheckForNull
    protected String getOakPrefixOrNull(String uri) {
        Map<String, String> namespaces = getNamespaceMap();
        for (Map.Entry<String, String> entry : namespaces.entrySet()) {
            if (uri.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

}
