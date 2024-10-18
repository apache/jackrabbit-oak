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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

final class PermissionCacheBuilder {

    private final PermissionStore store;
    private final PermissionEntryCache peCache;

    private Set<String> existingNames;
    private boolean usePathEntryMap;

    private boolean initialized = false;

    PermissionCacheBuilder(@NotNull PermissionStore store) {
        this.store = store;
        this.peCache = new PermissionEntryCache();
    }

    boolean init(@NotNull Set<String> principalNames, @NotNull CacheStrategy cacheStrategy) {
        existingNames = new HashSet<>();
        long cnt = 0;
        for (String name : principalNames) {
            NumEntries ne = store.getNumEntries(name, cacheStrategy.maxSize());
            long n = ne.size;
            /*
            if getNumEntries (n) returns a number bigger than 0, we
            remember this principal name int the 'existingNames' set
            */
            if (n > 0) {
                existingNames.add(name);
                if (cacheStrategy.loadFully(n, cnt)) {
                    peCache.getFullyLoadedEntries(store, name);
                } else {
                    long expectedSize = (ne.isExact) ? n : Long.MAX_VALUE;
                    peCache.init(name, expectedSize);
                }
            }
            /*
            Estimate the total number of access controlled paths (cnt) defined
            for the given set of principals in order to be able to determine if
            the pathEntryMap should be loaded upfront.
            Note however that cache.getNumEntries (n) may return Long.MAX_VALUE
            if the underlying implementation does not know the exact value, and
            the child node count is higher than maxSize (see OAK-2465).
            */
            if (cnt < Long.MAX_VALUE) {
                if (Long.MAX_VALUE == n) {
                    cnt = Long.MAX_VALUE;
                } else {
                    cnt = LongUtils.safeAdd(cnt, n);
                }
            }
        }

        usePathEntryMap = cacheStrategy.usePathEntryMap(cnt);
        initialized = true;
        return existingNames.isEmpty();
    }

    @NotNull
    PermissionCache build() {
        Validate.checkState(initialized);
        if (existingNames.isEmpty()) {
            return EmptyCache.INSTANCE;
        }
        if (usePathEntryMap) {
            // the total number of access controlled paths is smaller that maxSize,
            // so we can load all permission entries for all principals having
            // any entries right away into the pathEntryMap
            Map<String, Collection<PermissionEntry>> pathEntryMap = new HashMap<>();
            for (String name : existingNames) {
                PrincipalPermissionEntries ppe = peCache.getFullyLoadedEntries(store, name);
                for (Map.Entry<String, Collection<PermissionEntry>> e : ppe.getEntries().entrySet()) {
                    String path = e.getKey();
                    Collection<PermissionEntry> pathEntries = pathEntryMap.get(path);
                    if (pathEntries == null) {
                        pathEntries = new TreeSet<>(e.getValue());
                        pathEntryMap.put(path, pathEntries);
                    } else {
                        pathEntries.addAll(e.getValue());
                    }
                }
            }
            if (pathEntryMap.isEmpty()) {
                return EmptyCache.INSTANCE;
            } else {
                return new PathEntryMapCache(pathEntryMap);
            }
        } else {
            return new DefaultPermissionCache(store, peCache, existingNames);
        }
    }

    //------------------------------------< PermissionCache Implementations >---
    /**
     * Default implementation of {@code PermissionCache} wrapping the
     * {@code PermissionEntryCache}, which was previously hold as shared field
     * inside the {@code PermissionEntryProviderImpl}
     */
    private static final class DefaultPermissionCache implements PermissionCache {
        private final PermissionStore store;
        private final PermissionEntryCache cache;
        private final Set<String> existingNames;

        DefaultPermissionCache(@NotNull PermissionStore store, @NotNull PermissionEntryCache cache, Set<String> existingNames) {
            this.store = store;
            this.cache = cache;
            this.existingNames = existingNames;
        }

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull String path) {
            Collection<PermissionEntry> ret = new TreeSet<>();
            for (String name : existingNames) {
                cache.load(store, ret, name, path);
            }
            return ret;
        }

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull Tree accessControlledTree) {
            return (accessControlledTree.hasChild(AccessControlConstants.REP_POLICY)) ?
                    getEntries(accessControlledTree.getPath()) :
                    Collections.emptyList();
        }
    }

    /**
     * Fixed size implementation of {@code PermissionCache} that holds a map
     * containing all existing entries that in this case have been read eagerly
     * upfront. This implementation replaces the optional {@code pathEntryMap}
     * previously present inside the the {@code PermissionEntryProviderImpl}.
     */
    private static final class PathEntryMapCache implements PermissionCache {
        private final Map<String, Collection<PermissionEntry>> pathEntryMap;

        PathEntryMapCache(@NotNull Map<String, Collection<PermissionEntry>> pathEntryMap) {
            this.pathEntryMap = pathEntryMap;
        }

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull String path) {
            Collection<PermissionEntry> entries = pathEntryMap.get(path);
            return (entries != null) ? entries : Collections.emptyList();
        }

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull Tree accessControlledTree) {
            Collection<PermissionEntry> entries = pathEntryMap.get(accessControlledTree.getPath());
            return (entries != null) ? entries : Collections.emptyList();
        }
    }

    /**
     * Empty implementation of {@code PermissionCache} for those cases where
     * for a given (possibly empty) set of principals no permission entries are
     * present.
     */
    private static final class EmptyCache implements PermissionCache {

        private static final PermissionCache INSTANCE = new EmptyCache();

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull String path) {
            return Collections.emptyList();
        }

        @NotNull
        @Override
        public Collection<PermissionEntry> getEntries(@NotNull Tree accessControlledTree) {
            return Collections.emptyList();
        }
    }

}
