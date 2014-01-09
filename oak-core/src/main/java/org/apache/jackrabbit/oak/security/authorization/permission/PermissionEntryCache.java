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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * {@code PermissionEntryCache} caches the permission entries of principals.
 * The cache is held globally and contains a version of the principal permission
 * entries of the session that read them last. Each session gets a lazy copy of
 * the cache and needs to verify if each cached principal permission set still
 * reflects the state that the session sees.
 * Every newly loaded principal permission set can be pushed down to the base
 * cache if it does not exist there yet, or if it's newer.
 *
 * Todo:
 * - currently only the entries of 'everyone' are globally cached. this should be improved to dynamically cache those
 *   principals that are used often
 * - report cache usage metrics
 * - limit size of local caches based on ppe sizes. the current implementation loads all ppes. this can get a memory
 *   problem, as well as a performance problem for principals with many entries. principals with many entries must
 *   fallback to the direct store.load() methods when providing the entries. if those principals with many entries
 *   are used often, they might get elected to live in the global cache; memory permitting.
 */
public class PermissionEntryCache {

    private final Map<String, PrincipalPermissionEntries> base = new ConcurrentHashMap<String, PrincipalPermissionEntries>();

    @Nonnull
    public Local createLocalCache() {
        return new Local();
    }

    public void flush(@Nonnull Set<String> principalNames) {
        base.keySet().removeAll(principalNames);
    }

    public final class Local {

        private final Map<String, PrincipalPermissionEntries> entries = new HashMap<String, PrincipalPermissionEntries>();

        private final Set<String> verified = new HashSet<String>();

        private Local() {
            entries.putAll(base);
        }

        @Nonnull
        public PrincipalPermissionEntries getEntries(@Nonnull PermissionStore store,
                                                     @Nonnull String principalName) {
            PrincipalPermissionEntries ppe = entries.get(principalName);
            if (ppe == null) {
                ppe = store.load(principalName);
//                entries.put(principalName, ppe);
            } else {
                if (!verified.contains(principalName)) {
                    if (store.getTimestamp(principalName) != ppe.getTimestamp()) {
                        ppe = store.load(principalName);
                        entries.put(principalName, ppe);
                    }
                    verified.add(principalName);
                }
            }

            /*
            Currently this cache only handles entries for the Everyone principal.
            TODO: the cache should dynamically cache the principals that are used often.
            */
//            if (EveryonePrincipal.NAME.equals(principalName)) {
//                // check if base cache has the entries
//                PrincipalPermissionEntries baseppe = base.get(principalName);
//                if (baseppe == null || ppe.getTimestamp() > baseppe.getTimestamp()) {
//                    base.put(principalName, ppe);
//                }
//            }
            return ppe;
        }

        public void load(@Nonnull PermissionStore store,
                         @Nonnull Map<String, Collection<PermissionEntry>> pathEntryMap,
                         @Nonnull String principalName) {
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

        public void load(@Nonnull PermissionStore store,
                         @Nonnull Collection<PermissionEntry> ret,
                         @Nonnull String principalName,
                         @Nonnull String path) {
            // todo: conditionally load entries if too many
            PrincipalPermissionEntries ppe = getEntries(store, principalName);
            ret.addAll(ppe.getEntries(path));
        }

        public boolean hasEntries(@Nonnull PermissionStore store,
                                  @Nonnull String principalName) {
            // todo: conditionally load entries if too many
            return getNumEntries(store, principalName) > 0;
        }

        public long getNumEntries(@Nonnull PermissionStore store,
                                  @Nonnull String principalName) {
            // todo: conditionally load entries if too many
            return getEntries(store, principalName).getEntries().size();
        }

        public void flush(@Nonnull Set<String> principalNames) {
            verified.removeAll(principalNames);
        }

    }
}