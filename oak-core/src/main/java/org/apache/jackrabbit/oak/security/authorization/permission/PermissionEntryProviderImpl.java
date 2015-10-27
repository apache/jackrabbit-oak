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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;

class PermissionEntryProviderImpl implements PermissionEntryProvider {

    public static final String EAGER_CACHE_SIZE_PARAM = "eagerCacheSize";

    private static final long DEFAULT_SIZE = 250;

    /**
     * The set of principal names for which this {@code PermissionEntryProvider}
     * has been created.
     */
    private final Set<String> principalNames;

    /**
     * The set of principal names for which the store contains any permission
     * entries. This set is equals or just a subset of the {@code principalNames}
     * defined above. The methods collecting the entries will shortcut in case
     * this set is empty and thus no permission entries exist for the specified
     * set of principal.
     */
    private final Set<String> existingNames = new HashSet<String>();

    private final PermissionStore store;

    private final PermissionEntryCache cache;

    private final long maxSize;

    private Map<String, Collection<PermissionEntry>> pathEntryMap;

    PermissionEntryProviderImpl(@Nonnull PermissionStore store, @Nonnull PermissionEntryCache cache,
                                @Nonnull Set<String> principalNames, @Nonnull ConfigurationParameters options) {
        this.store = store;
        this.cache = cache;
        this.principalNames = Collections.unmodifiableSet(principalNames);
        this.maxSize = options.getConfigValue(EAGER_CACHE_SIZE_PARAM, DEFAULT_SIZE);
        init();
    }

    private void init() {
        long cnt = 0;
        existingNames.clear();
        for (String name : principalNames) {
            long n = cache.getNumEntries(store, name, maxSize);
            /*
            if cache.getNumEntries (n) returns a number bigger than 0, we
            remember this principal name int the 'existingNames' set
            */
            if (n > 0) {
                existingNames.add(name);
            }
            /*
            Calculate the total number of permission entries (cnt) defined for the
            given set of principals in order to be able to determine if the cache
            should be loaded upfront.
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

        if (cnt > 0 && cnt < maxSize) {
            // the total number of entries is smaller that maxSize, so we can
            // cache all entries for all principals having any entries right away
            pathEntryMap = new HashMap<String, Collection<PermissionEntry>>();
            for (String name : existingNames) {
                cache.load(store, pathEntryMap, name);
            }
        } else {
            pathEntryMap = null;
        }
    }

    //--------------------------------------------< PermissionEntryProvider >---
    @Override
    public void flush() {
        cache.flush(principalNames);
        init();
    }

    @Override
    @Nonnull
    public Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        if (existingNames.isEmpty()) {
            return Iterators.emptyIterator();
        } else {
            return new EntryIterator(predicate);
        }
    }

    @Override
    @Nonnull
    public Collection<PermissionEntry> getEntries(@Nonnull Tree accessControlledTree) {
        if (existingNames.isEmpty()) {
            return Collections.emptyList();
        } else if (pathEntryMap != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(accessControlledTree.getPath());
            return (entries != null) ? entries : Collections.<PermissionEntry>emptyList();
        } else {
            return (accessControlledTree.hasChild(AccessControlConstants.REP_POLICY)) ?
                    loadEntries(accessControlledTree.getPath()) :
                    Collections.<PermissionEntry>emptyList();
        }
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Collection<PermissionEntry> getEntries(@Nonnull String path) {
        if (existingNames.isEmpty()) {
            return Collections.emptyList();
        } else if (pathEntryMap != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(path);
            return (entries != null) ? entries : Collections.<PermissionEntry>emptyList();
        } else {
            return loadEntries(path);
        }
    }

    @Nonnull
    private Collection<PermissionEntry> loadEntries(@Nonnull String path) {
        Collection<PermissionEntry> ret = new TreeSet<PermissionEntry>();
        for (String name : existingNames) {
            cache.load(store, ret, name, path);
        }
        return ret;
    }

    private final class EntryIterator extends AbstractLazyIterator<PermissionEntry> {

        private final EntryPredicate predicate;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Iterators.emptyIterator();

        // the next oak path for which to retrieve permission entries
        private String path;

        private EntryIterator(@Nonnull EntryPredicate predicate) {
            this.predicate = predicate;
            this.path = Strings.nullToEmpty(predicate.getPath());
        }

        @Override
        protected PermissionEntry getNext() {
            PermissionEntry next = null;
            while (next == null) {
                if (nextEntries.hasNext()) {
                    PermissionEntry pe = nextEntries.next();
                    if (predicate.apply(pe)) {
                        next = pe;
                    }
                } else {
                    if (path == null) {
                        break;
                    }
                    nextEntries = getEntries(path).iterator();
                    path = PermissionUtil.getParentPathOrNull(path);
                }
            }
            return next;
        }
    }
}