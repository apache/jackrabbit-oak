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

import static java.util.Collections.emptySet;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code PermissionEntries} holds the permission entries of one principal
 */
class PrincipalPermissionEntries {

    /**
     * max size of the emptyPaths cache.
     */
    private static int MAX_SIZE = Integer.getInteger("oak.PrincipalPermissionEntries.maxSize", 1000);

    private final long expectedSize;

    /**
     * indicating if all entries were loaded.
     */
    private boolean fullyLoaded;

    /**
     * map of permission entries, accessed by path
     */
    private Map<String, Collection<PermissionEntry>> entries = new HashMap<>();
    private final Map<String, Boolean> emptyPaths;

    PrincipalPermissionEntries() {
        this(Long.MAX_VALUE);
    }

    PrincipalPermissionEntries(long expectedSize) {
        this.expectedSize = expectedSize;
        this.fullyLoaded = (expectedSize == 0);
        this.emptyPaths = new LinkedHashMap<String, Boolean>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > MAX_SIZE;
            }
        };
    }

    long getSize() {
        return entries.size() + emptyPaths.size();
    }

    boolean isFullyLoaded() {
        return fullyLoaded;
    }

    void setFullyLoaded(boolean fullyLoaded) {
        this.fullyLoaded = fullyLoaded;
    }

    @NotNull
    Map<String, Collection<PermissionEntry>> getEntries() {
        return entries;
    }

    @Nullable
    Collection<PermissionEntry> getEntriesByPath(@NotNull String path) {
        return emptyPaths.containsKey(path) ? emptySet() : entries.get(path);
    }

    void putEntriesByPath(@NotNull String path, @NotNull Collection<PermissionEntry> pathEntries) {
        entries.put(path, pathEntries);
        if (entries.size() >= expectedSize) {
            setFullyLoaded(true);
        }
    }

    void rememberNotAccessControlled(@NotNull String path) {
        emptyPaths.put(path, null);
    }

    void putAllEntries(@NotNull Map<String, Collection<PermissionEntry>> allEntries) {
        entries.putAll(allEntries);
        setFullyLoaded(true);
    }
}
