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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;

final class EntryIterator extends AbstractLazyIterator<PermissionEntry> {

    private final Predicate<PermissionEntry> predicate;
    private final EntryCache entryCache;

    // initially set to empty-iterator to trigger reading entries from the cache
    private Iterator<PermissionEntry> nextEntries = Collections.emptyIterator();

    // the next oak path for which to retrieve permission entries
    private String nextPath;

    EntryIterator(@NotNull String path, @NotNull Predicate<PermissionEntry> predicate, @NotNull EntryCache entryCache) {
        this.nextPath = path;
        this.predicate = predicate;
        this.entryCache = entryCache;
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
                // stop the iteration if entries for the root node have already been processed
                if (nextPath == null) {
                    break;
                }
                // obtain entries from the next path in the hierarchy
                nextEntries = entryCache.getEntries(nextPath);
                nextPath = getParentPathOrNull(nextPath);
            }
        }
        return next;
    }

    @Nullable
    private static String getParentPathOrNull(@NotNull String path) {
        String parentPath = PathUtils.getParentPath(path);
        return (path.equals(parentPath)) ? null : parentPath;
    }
}