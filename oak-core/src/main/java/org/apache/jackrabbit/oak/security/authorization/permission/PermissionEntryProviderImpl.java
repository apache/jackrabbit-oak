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
import java.util.Iterator;
import java.util.Set;
import com.google.common.base.Strings;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.jetbrains.annotations.NotNull;

class PermissionEntryProviderImpl implements PermissionEntryProvider {

    private static final String EAGER_CACHE_SIZE_PARAM = "eagerCacheSize";

    private static final long DEFAULT_SIZE = 250;

    /**
     * The set of principal names for which this {@code PermissionEntryProvider}
     * has been created.
     */
    private final Set<String> principalNames;

    private final PermissionStore store;

    private final long maxSize;

    /**
     * Flag to indicate if the the store contains any permission entries for the
     * given set of principal names.
     */
    private boolean noExistingNames;

    private PermissionCache permissionCache;

    PermissionEntryProviderImpl(@NotNull PermissionStore store, @NotNull Set<String> principalNames, @NotNull ConfigurationParameters options) {
        this.store = store;
        this.principalNames = Collections.unmodifiableSet(principalNames);
        this.maxSize = options.getConfigValue(EAGER_CACHE_SIZE_PARAM, DEFAULT_SIZE);
        init();
    }

    private void init() {
        PermissionCacheBuilder builder = new PermissionCacheBuilder(store);
        noExistingNames = builder.init(principalNames, maxSize);
        permissionCache = builder.build();
    }

    //--------------------------------------------< PermissionEntryProvider >---
    @Override
    public void flush() {
        init();
    }

    @Override
    @NotNull
    public Iterator<PermissionEntry> getEntryIterator(@NotNull EntryPredicate predicate) {
        if (noExistingNames) {
            return Collections.emptyIterator();
        } else {
            return new EntryIterator(predicate);
        }
    }

    @Override
    @NotNull
    public Collection<PermissionEntry> getEntries(@NotNull Tree accessControlledTree) {
        return permissionCache.getEntries(accessControlledTree);
    }

    //------------------------------------------------------------< private >---
    @NotNull
    private Collection<PermissionEntry> getEntries(@NotNull String path) {
        return permissionCache.getEntries(path);
    }

    private final class EntryIterator extends AbstractLazyIterator<PermissionEntry> {

        private final EntryPredicate predicate;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Collections.emptyIterator();

        // the next oak path for which to retrieve permission entries
        private String path;

        private EntryIterator(@NotNull EntryPredicate predicate) {
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
