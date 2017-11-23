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

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Root;

/**
 * Name mapper with local namespace mappings.
 */
public class LocalNameMapper extends GlobalNameMapper {

    protected final Map<String, String> local;

    public LocalNameMapper(Root root, Map<String, String> local) {
        super(root);
        this.local = local;
    }

    public LocalNameMapper(
            Map<String, String> global, Map<String, String> local) {
        super(global);
        this.local = local;
    }

    @Override @Nonnull
    public synchronized Map<String, String> getSessionLocalMappings() {
        return local;
    }

    @Override @Nonnull
    public synchronized String getJcrName(@Nonnull String oakName) {
        checkNotNull(oakName);
        checkArgument(!oakName.startsWith(":"), oakName); // hidden name
        checkArgument(!isExpandedName(oakName), oakName); // expanded name

        if (!local.isEmpty()) {
            int colon = oakName.indexOf(':');
            if (colon > 0) {
                String oakPrefix = oakName.substring(0, colon);
                String uri = getNamespacesProperty(oakPrefix);
                if (uri == null) {
                    throw new IllegalStateException(
                            "No namespace mapping found for " + oakName);
                }

                for (Map.Entry<String, String> entry : local.entrySet()) {
                    if (uri.equals(entry.getValue())) {
                        String jcrPrefix = entry.getKey();
                        if (jcrPrefix.equals(oakPrefix)) {
                            return oakName;
                        } else {
                            return jcrPrefix + oakName.substring(colon);
                        }
                    }
                }

                // local mapping not found for this URI, make sure there
                // is no conflicting local mapping for the prefix
                if (local.containsKey(oakPrefix)) {
                    for (int i = 2; true; i++) {
                        String jcrPrefix = oakPrefix + i;
                        if (!local.containsKey(jcrPrefix)) {
                            return jcrPrefix + oakName.substring(colon);
                        }
                    }
                }
            }
        }

        return oakName;
    }

    @Override @CheckForNull
    public synchronized String getOakNameOrNull(@Nonnull String jcrName) {
        checkNotNull(jcrName);

        if (jcrName.startsWith("{")) {
            String oakName = getOakNameFromExpanded(jcrName);
            if (oakName != jcrName) {
                return oakName;
            } // else not an expanded name, so fall through to local mapping
        }

        if (!local.isEmpty()) {
            int colon = jcrName.indexOf(':');
            if (colon > 0) {
                String jcrPrefix = jcrName.substring(0, colon);
                String uri = local.get(jcrPrefix);
                if (uri != null) {
                    String oakPrefix = getOakPrefixOrNull(uri);
                    if (oakPrefix == null) {
                        return null;
                    } else if (jcrPrefix.equals(oakPrefix)) {
                        return jcrName;
                    } else {
                        return oakPrefix + jcrName.substring(colon);
                    }
                }

                // Check that a global mapping is present and not remapped
                String mapping = getNamespacesProperty(jcrPrefix);
                if (mapping != null
                        && local.values().contains(mapping)) {
                    return null;
                }
            }
        }

        return jcrName;
    }

}
