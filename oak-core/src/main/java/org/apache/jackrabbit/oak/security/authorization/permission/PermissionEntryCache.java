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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PermissionEntryCache} caches the permission entries of principals.
 * The cache is held locally for each session and contains a version of the principal permission
 * entries of the session that read them last.
 */
class PermissionEntryCache {

    private static final Logger log = LoggerFactory.getLogger(PermissionEntryCache.class);

    private final Map<String, PrincipalPermissionEntries> entries = new HashMap<>();

    @NotNull
    PrincipalPermissionEntries getFullyLoadedEntries(@NotNull PermissionStore store,
                                                     @NotNull String principalName) {
        PrincipalPermissionEntries ppe = entries.get(principalName);
        if (ppe == null || !ppe.isFullyLoaded()) {
            ppe = store.load(principalName);
            entries.put(principalName, ppe);
        }
        return ppe;
    }

    void init(@NotNull String principalName, long expectedSize) {
        if (!entries.containsKey(principalName)) {
            entries.put(principalName, new PrincipalPermissionEntries(expectedSize));
        }
    }

    void load(@NotNull PermissionStore store,
              @NotNull Collection<PermissionEntry> ret,
              @NotNull String principalName,
              @NotNull String path) {
        if (entries.containsKey(principalName)) {
            PrincipalPermissionEntries ppe = entries.get(principalName);
            Collection<PermissionEntry> pes = ppe.getEntriesByPath(path);
            if (ppe.isFullyLoaded() || pes != null) {
                // no need to read from store
                if (pes != null) {
                    ret.addAll(pes);
                }
            } else {
                // read entries for path from store
                pes = store.load(principalName, path);
                if (pes == null) {
                    // nothing to add to the result collection 'ret'.
                    // nevertheless, remember the absence of any permission entries
                    // in the cache to avoid reading from store again.
                    ppe.rememberNotAccessControlled(path);
                } else {
                    ppe.putEntriesByPath(path, pes);
                    ret.addAll(pes);
                }
            }
        } else {
            log.error("Failed to load entries for principal '%s' at path %s", principalName, path);
        }
    }
}
