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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;

/**
 * Name mapper with local namespace mappings.
 */
public abstract class LocalNameMapper extends GlobalNameMapper {

    public LocalNameMapper(Root root) {
        super(root);
    }

    public LocalNameMapper(Map<String, String> namespaces) {
        super(namespaces);
    }

    @Override @Nonnull
    public abstract Map<String, String> getSessionLocalMappings();

    @Override @CheckForNull
    public String getJcrName(String oakName) {
        checkNotNull(oakName);
        checkArgument(!oakName.startsWith(":"), oakName); // hidden name
        checkArgument(!isExpandedName(oakName), oakName); // expanded name

        Map<String, String> local = getSessionLocalMappings();
        if (!local.isEmpty()) {
            int colon = oakName.indexOf(':');
            if (colon > 0) {
                String oakPrefix = oakName.substring(0, colon);
                PropertyState mapping = namespaces.getProperty(oakPrefix);
                if (mapping == null || mapping.getType() != STRING) {
                    throw new IllegalStateException(
                            "No namespace mapping found for " + oakName);
                }
                String uri = mapping.getValue(STRING);

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
    public String getOakNameOrNull(String jcrName) {
        checkNotNull(jcrName);

        if (jcrName.startsWith("{")) {
            return getOakNameFromExpanded(jcrName);
        }

        Map<String, String> local = getSessionLocalMappings();
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
                PropertyState mapping = namespaces.getProperty(jcrPrefix);
                if (mapping != null
                        && mapping.getType() == STRING
                        && local.values().contains(mapping.getValue(STRING))) {
                    return null;
                }
            }
        }

        return jcrName;
    }

}
