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
import java.util.Set;
import java.util.TreeSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@code PermissionEntryCache} caches the permission entries of principals.
 * The cache is held locally for each session and contains a version of the principal permission
 * entries of the session that read them last.
 *
 * TODO:
 * - report cache usage metrics
 * - limit size of local caches based on ppe sizes. the current implementation loads all ppes. this can get a memory
 *   problem, as well as a performance problem for principals with many entries. principals with many entries must
 *   fallback to the direct store.load() methods when providing the entries. if those principals with many entries
 *   are used often, they might get elected to live in the global cache; memory permitting.
 */
class PermissionEntryCache {

    private final Map<String, PrincipalPermissionEntries> entries = new HashMap<String, PrincipalPermissionEntries>();

    @NotNull
    PrincipalPermissionEntries getEntries(@NotNull PermissionStore store,
                                                 @NotNull String principalName) {
        PrincipalPermissionEntries ppe = entries.get(principalName);
        if (ppe == null) {
            ppe = store.load(principalName);
            entries.put(principalName, ppe);
        } else {
            if (!ppe.isFullyLoaded()) {
                ppe = store.load(principalName);
                entries.put(principalName, ppe);
            }
        }
        return ppe;
    }

    void load(@NotNull PermissionStore store,
              @NotNull Map<String, Collection<PermissionEntry>> pathEntryMap,
              @NotNull String principalName) {
        // todo: conditionally load entries if too many
        PrincipalPermissionEntries ppe = getEntries(store, principalName);
        for (Map.Entry<String, Collection<PermissionEntry>> e: ppe.getEntries().entrySet()) {
            Collection<PermissionEntry> pathEntries = pathEntryMap.get(e.getKey());
            if (pathEntries == null) {
                pathEntries = new TreeSet<PermissionEntry>(e.getValue());
                pathEntryMap.put(e.getKey(), pathEntries);
            } else {
                pathEntries.addAll(e.getValue());
            }
        }
    }

    void load(@NotNull PermissionStore store,
              @NotNull Collection<PermissionEntry> ret,
              @NotNull String principalName,
              @NotNull String path) {
        PrincipalPermissionEntries ppe = entries.get(principalName);
        if (ppe == null) {
            ppe = new PrincipalPermissionEntries();
            entries.put(principalName, ppe);
        }
        Collection<PermissionEntry> pes = ppe.getEntriesByPath(path);
        if (pes == null) {
            pes = store.load(null, principalName, path);
            if (pes == null) {
                ppe.rememberNotAccessControlled(path);
            } else {
                ppe.putEntriesByPath(path, pes);
                ret.addAll(pes);
            }
        } else {
            ret.addAll(pes);
        }
    }

    long getNumEntries(@NotNull PermissionStore store,
                       @NotNull String principalName,
                       long max) {
        PrincipalPermissionEntries ppe = entries.get(principalName);
        return ppe == null
                ? store.getNumEntries(principalName, max)
                : ppe.getEntries().size();
    }

    void flush(@NotNull Set<String> principalNames) {
        entries.keySet().removeAll(principalNames);
    }
}
