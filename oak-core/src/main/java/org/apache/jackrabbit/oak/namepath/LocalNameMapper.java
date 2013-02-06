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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Name mapper with local namespace mappings.
 */
public abstract class LocalNameMapper extends GlobalNameMapper {

    private final Map<String, String> local;

    protected LocalNameMapper(Map<String, String> local) {
        this.local = local;
    }

    @Override @CheckForNull
    public String getJcrName(String oakName) {
        checkNotNull(oakName);
        checkArgument(!oakName.startsWith(":")); // hidden name
        checkArgument(!oakName.startsWith("{")); // expanded name

        if (hasSessionLocalMappings()) {
            int colon = oakName.indexOf(':');
            if (colon > 0) {
                String oakPrefix = oakName.substring(0, colon);
                String uri = getNamespaceMap().get(oakPrefix);
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
                            local.put(jcrPrefix, uri);
                            return jcrPrefix + oakName.substring(colon);
                        }
                    }
                }
            }
        }

        return oakName;
    }

    @Override @CheckForNull
    public String getOakNameOrNull(String jcrName) {
        checkNotNull(jcrName);

        if (jcrName.startsWith("{")) {
            return getOakNameFromExpanded(jcrName);
        }

        if (hasSessionLocalMappings()) {
            int colon = jcrName.indexOf(':');
            if (colon > 0) {
                String jcrPrefix = jcrName.substring(0, colon);
                String uri = local.get(jcrPrefix);
                if (uri != null) {
                    String oakPrefix = getOakPrefixOrNull(uri);
                    if (jcrPrefix.equals(oakPrefix)) {
                        return jcrName;
                    } else if (oakPrefix != null) {
                        return oakPrefix + jcrName.substring(colon);
                    } else {
                        return null;
                    }
                }

                // Check that a global mapping is present and not remapped
                uri = getNamespaceMap().get(jcrPrefix);
                if (uri == null || local.values().contains(uri)) {
                    return null;
                }
            }
        }

        return jcrName;
    }

    @Override
    public boolean hasSessionLocalMappings() {
        return !local.isEmpty();
    }

}
