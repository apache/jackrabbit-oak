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
package org.apache.jackrabbit.oak.plugins.index.cursor;

import java.util.List;

import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * This utility class provides factory methods to create commonly used types of
 * {@link Cursor}s.
 */
public class Cursors {

    private Cursors() {
    }
    
    public static void checkMemoryLimit(long count, QueryLimits settings) {
        FilterIterators.checkMemoryLimit(count, settings);
    }    

    public static void checkReadLimit(long count, QueryLimits settings) {
        FilterIterators.checkReadLimit(count, settings);
    }    

    public static Cursor newIntersectionCursor(Cursor a, Cursor b, QueryLimits settings) {
        return new IntersectionCursor(a, b, settings);
    }

    public static Cursor newConcatCursor(List<Cursor> cursors, QueryLimits settings) {
        return new ConcatCursor(cursors, settings);
    }

    public static Cursor newPrefetchCursor(Cursor cursor, PrefetchNodeStore store, int prefetchCount,
            NodeState rootState, List<String> prefetchRelative) {
        return new PrefetchCursor(cursor, store, prefetchCount, rootState, prefetchRelative);
    }

    /**
     * Creates a {@link Cursor} over paths.
     *
     * @param paths the paths to iterate over (must return distinct paths)
     * @return the Cursor.
     */
    public static Cursor newPathCursor(Iterable<String> paths, QueryLimits settings) {
        return new PathCursor(paths.iterator(), true, settings);
    }

    /**
     * Creates a {@link Cursor} over paths, and make the result distinct.
     * The iterator might return duplicate paths
     * 
     * @param paths the paths to iterate over (might contain duplicate entries)
     * @return the Cursor.
     */
    public static Cursor newPathCursorDistinct(Iterable<String> paths, QueryLimits settings) {
        return new PathCursor(paths.iterator(), true, settings);
    }

    /**
     * Returns a traversing cursor based on the path restriction in the given
     * {@link Filter}.
     * 
     * @param filter the filter.
     * @param rootState the root {@link NodeState}.
     * @return the {@link Cursor}.
     */
    public static Cursor newTraversingCursor(Filter filter,
                                             NodeState rootState) {
        return new TraversingCursor(filter, rootState);
    }

    /**
     * Returns a cursor wrapper, which returns the ancestor rows at the given
     * <code>level</code> of the wrapped cursor <code>c</code>. With
     * <code>level</code> e.g. set to <code>1</code>, the returned cursor
     * iterates over the parent rows of the passed cursor <code>c</code>. The
     * returned cursor guarantees distinct rows.
     *
     * @param c the cursor to wrap.
     * @param level the ancestor level. Must be {@code >= 1}.
     * @return cursor over the ancestors of <code>c</code> at <code>level</code>.
     */
    public static Cursor newAncestorCursor(Cursor c, int level, QueryLimits settings) {
        requireNonNull(c);
        checkArgument(level >= 1);
        return new AncestorCursor(c, level, settings);
    }


}
