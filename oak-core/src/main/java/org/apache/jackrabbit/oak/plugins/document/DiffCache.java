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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A cache for child node diffs.
 */
public interface DiffCache {

    /**
     * Returns a jsop diff for the child nodes at the given path. The returned
     * String may contain the following changes on child nodes:
     * <ul>
     *     <li>Changed child nodes: e.g. {@code ^"foo":{}}</li>
     *     <li>Added child nodes: e.g. {@code +"bar":{}}</li>
     *     <li>Removed child nodes: e.g. {@code -"baz"}</li>
     * </ul>
     * A {@code null} value indicates that this cache does not have an entry
     * for the given revision range at the path.
     *
     * @param from the from revision.
     * @param to the to revision.
     * @param path the path of the parent node.
     * @return the diff or {@code null} if unknown.
     */
    @CheckForNull
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path);

    /**
     * Starts a new cache entry for the diff cache. Actual changes are added
     * to the entry with the {@link Entry#append(String, String)} method.
     *
     * @param from the from revision.
     * @param to the to revision.
     * @return the cache entry.
     */
    @Nonnull
    public Entry newEntry(@Nonnull Revision from,
                          @Nonnull Revision to);

    public interface Entry {

        /**
         * Appends changes about children of the node at the given path.
         *
         * @param path the path of the parent node.
         * @param changes the child node changes.
         */
        public void append(@Nonnull String path,
                           @Nonnull String changes);

        /**
         * Called when all changes have been appended and the entry is ready
         * to be used by the cache.
         */
        public void done();
    }
}
