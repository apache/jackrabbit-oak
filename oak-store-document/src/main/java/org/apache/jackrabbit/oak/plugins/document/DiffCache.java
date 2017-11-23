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
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

/**
 * A cache for child node diffs.
 */
abstract class DiffCache {

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
     * @param loader an optional loader for the cache entry.
     * @return the diff or {@code null} if unknown and no loader was passed.
     */
    @CheckForNull
    abstract String getChanges(@Nonnull RevisionVector from,
                               @Nonnull RevisionVector to,
                               @Nonnull String path,
                               @Nullable Loader loader);

    /**
     * Starts a new cache entry for the diff cache. Actual changes are added
     * to the entry with the {@link Entry#append(String, String)} method.
     *
     * @param from the from revision.
     * @param to the to revision.
     * @param local true indicates that the entry results from a local change,
     * false if it results from an external change
     * @return the cache entry.
     */
    @Nonnull
    abstract Entry newEntry(@Nonnull RevisionVector from,
                            @Nonnull RevisionVector to,
                            boolean local);

    /**
     * @return the statistics for this cache.
     */
    @Nonnull
    abstract Iterable<CacheStats> getStats();

    /**
     * Parses the jsop diff returned by
     * {@link #getChanges(RevisionVector, RevisionVector, String, Loader)} and reports the
     * changes by calling the appropriate methods on {@link Diff}.
     *
     * @param jsop the jsop diff to parse.
     * @param diff the diff handler.
     * @return {@code true} it the complete jsop was processed or {@code false}
     *      if one of the {@code diff} callbacks requested a stop.
     * @throws IllegalArgumentException if {@code jsop} is malformed.
     */
    static boolean parseJsopDiff(@Nonnull String jsop,
                                 @Nonnull Diff diff) {
        if (jsop.trim().isEmpty()) {
            return true;
        }
        JsopTokenizer t = new JsopTokenizer(jsop);
        boolean continueComparison = true;
        while (continueComparison) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            switch (r) {
                case '+': {
                    String name = t.readString();
                    t.read(':');
                    t.read('{');
                    while (t.read() != '}') {
                        // skip properties
                    }
                    continueComparison = diff.childNodeAdded(name);
                    break;
                }
                case '-': {
                    String name = t.readString();
                    continueComparison = diff.childNodeDeleted(name);
                    break;
                }
                case '^': {
                    String name = t.readString();
                    t.read(':');
                    t.read('{');
                    t.read('}');
                    continueComparison = diff.childNodeChanged(name);
                    break;
                }
                default:
                    throw new IllegalArgumentException("jsonDiff: illegal token '"
                            + t.getToken() + "' at pos: " + t.getLastPos() + ' ' + jsop);
            }
        }
        return continueComparison;
    }

    interface Entry {

        /**
         * Appends changes about children of the node at the given path.
         *
         * @param path the path of the parent node.
         * @param changes the child node changes.
         */
        void append(@Nonnull String path,
                    @Nonnull String changes);

        /**
         * Called when all changes have been appended and the entry is ready
         * to be used by the cache.
         * 
         * @return {@code true} if the entry was successfully added to the
         *          cache, {@code false} otherwise.
         */
        boolean done();
    }

    interface Loader {

        String call();
    }

    interface Diff {

        boolean childNodeAdded(String name);

        boolean childNodeChanged(String name);

        boolean childNodeDeleted(String name);

    }
}
