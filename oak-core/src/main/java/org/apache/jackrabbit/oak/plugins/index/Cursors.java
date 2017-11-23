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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

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
        checkNotNull(c);
        checkArgument(level >= 1);
        return new AncestorCursor(c, level, settings);
    }

    /**
     * A Cursor implementation where the remove method throws an
     * UnsupportedOperationException.
     */
    public abstract static class AbstractCursor implements Cursor {
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public long getSize(SizePrecision precision, long max) {
            return -1;
        }

    }

    /**
     * This class allows to iterate over the parent nodes of the wrapped cursor.
     */
    private static class AncestorCursor extends PathCursor {

        public AncestorCursor(Cursor cursor, int level, QueryLimits settings) {
            super(transform(cursor, level), true, settings);
        }

        private static Iterator<String> transform(Cursor cursor, final int level) {
            Iterator<String> unfiltered = Iterators.transform(cursor,
                    new Function<IndexRow, String>() {
                @Override
                public String apply(@Nullable IndexRow input) {
                    return input != null ? input.getPath() : null;
                }
            });
            Iterator<String> filtered = Iterators.filter(unfiltered,
                    new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String input) {
                    return input != null && PathUtils.getDepth(input) >= level;
                }
            });
            return Iterators.transform(filtered, new Function<String, String>() {
                @Override
                public String apply(String input) {
                    return PathUtils.getAncestorPath(input, level);
                }
            });
        }
    }

    /**
     * <code>PathCursor</code> implements a simple {@link Cursor} that iterates
     * over a {@link String} based path {@link Iterable}.
     */
    public static class PathCursor extends AbstractCursor {

        private final Iterator<String> iterator;

        public PathCursor(Iterator<String> paths, boolean distinct, final QueryLimits settings) {
            Iterator<String> it = paths;
            if (distinct) {
                it = Iterators.filter(it, new Predicate<String>() {
                    
                    private final HashSet<String> known = new HashSet<String>();

                    @Override
                    public boolean apply(@Nullable String input) {
                        FilterIterators.checkMemoryLimit(known.size(), settings);
                        // Set.add returns true for new entries
                        return known.add(input);
                    }
                    
                });
            }
            this.iterator = it;
        }

        @Override
        public IndexRow next() {
            // TODO support jcr:score and possibly rep:excerpt
            String path = iterator.next();
            return new IndexRowImpl(isAbsolute(path) ? path : "/" + path);
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
    }

    /**
     * A cursor that reads all nodes in a given subtree.
     */
    private static class TraversingCursor extends AbstractCursor {

        private static final Logger LOG = LoggerFactory.getLogger(TraversingCursor.class);

        private final Filter filter;

        private final Deque<Iterator<? extends ChildNodeEntry>> nodeIterators =
                Queues.newArrayDeque();

        private String parentPath;

        private String currentPath;

        private long readCount;

        private boolean init;
        
        private boolean closed;
        
        private final QueryLimits settings;
        
        public TraversingCursor(Filter filter, NodeState rootState) {
            this.filter = filter;
            this.settings = filter.getQueryLimits();

            String path = filter.getPath();
            parentPath = null;
            currentPath = "/";
            NodeState parent = null;
            NodeState node = rootState;
            
            if (filter.containsNativeConstraint()) {
                // OAK-4313: if no other index was found,
                // then, for native queries, we won't match anything
                return;
            }

            if (filter.isAlwaysFalse()) {
                // nothing can match this filter, leave nodes empty
                return;
            }

            if (!path.equals("/")) {
                for (String name : path.substring(1).split("/")) {
                    parentPath = currentPath;
                    currentPath = PathUtils.concat(parentPath, name);

                    parent = node;
                    node = parent.getChildNode(name);
                }
                if (!node.exists()) {
                    // nothing can match this filter, leave nodes empty
                    return;
                }
            }
            Filter.PathRestriction restriction = filter.getPathRestriction();
            switch (restriction) {
            case NO_RESTRICTION:
            case EXACT:
            case ALL_CHILDREN:
                nodeIterators.add(Iterators.singletonIterator(
                        new MemoryChildNodeEntry(currentPath, node)));
                parentPath = "";
                break;
            case PARENT:
                if (parent != null) {
                    nodeIterators.add(Iterators.singletonIterator(
                            new MemoryChildNodeEntry(parentPath, parent)));
                    parentPath = "";
                }
                break;
            case DIRECT_CHILDREN:
                nodeIterators.add(node.getChildNodeEntries().iterator());
                parentPath = currentPath;
                break;
            default:
                throw new IllegalArgumentException("Unknown restriction: " + restriction);
            }
        }

        @Override
        public IndexRow next() {
            if (closed) {
                throw new IllegalStateException("This cursor is closed");
            }
            if (!init) {
                fetchNext();
                init = true;
            }
            IndexRowImpl result = new IndexRowImpl(currentPath);
            fetchNext();
            return result;
        }
        
        @Override 
        public boolean hasNext() {
            if (!closed && !init) {
                fetchNext();
                init = true;
            }
            return !closed;
        }

        private void fetchNext() {
            while (!nodeIterators.isEmpty()) {
                Iterator<? extends ChildNodeEntry> iterator = nodeIterators.getLast();
                if (iterator.hasNext()) {
                    ChildNodeEntry entry = iterator.next();

                    readCount++;
                    if (readCount % 1000 == 0) {
                        FilterIterators.checkReadLimit(readCount, settings);
                        LOG.warn("Traversed " + readCount + " nodes with filter " + filter + "; consider creating an index or changing the query");
                    }

                    NodeState node = entry.getNodeState();

                    String name = entry.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        continue;
                    }
                    currentPath = PathUtils.concat(parentPath, name);

                    PathRestriction r = filter.getPathRestriction();
                    if (r == PathRestriction.ALL_CHILDREN || 
                            r == PathRestriction.NO_RESTRICTION) {
                        nodeIterators.addLast(node.getChildNodeEntries().iterator());
                        parentPath = currentPath;
                    }
                    return;
                } else {
                    nodeIterators.removeLast();
                    parentPath = PathUtils.getParentPath(parentPath);
                }
            }
            currentPath = null;
            closed = true;
        }

    }
    
    /**
     * A cursor that intersects two cursors.
     */
    private static class IntersectionCursor extends AbstractCursor {

        private final HashMap<String, IndexRow> secondSet = new HashMap<String, IndexRow>();
        private final HashSet<String> seen = new HashSet<String>();
        private final Cursor first, second;
        private final QueryLimits settings;
        private boolean init;
        private boolean closed;
        private IndexRow current;
        
        IntersectionCursor(Cursor first, Cursor second, QueryLimits settings) {
            this.first = first;
            this.second = second;
            this.settings = settings;
        }
        
        @Override
        public IndexRow next() {
            if (closed) {
                throw new IllegalStateException("This cursor is closed");
            }
            if (!init) {
                fetchNext();
                init = true;
                if (closed) {
                    throw new IllegalStateException("This cursor is closed");
                }
            }
            IndexRow result = current;
            // fetchNext();
            init = false;
            return result;
        }

        @Override
        public boolean hasNext() {
            if (!closed && !init) {
                fetchNext();
                init = true;
            }
            return !closed;
        }
        
        private void fetchNext() {
            while (true) {
                if (!first.hasNext()) {
                    closed = true;
                    return;
                }
                IndexRow c = first.next();
                String p = c.getPath();
                if (seen.contains(p)) {
                    continue;
                }
                if (secondSet.remove(p) != null) {
                    current = c;
                    markSeen(p);
                    return;
                }
                while (second.hasNext()) {
                    IndexRow s = second.next();
                    String p2 = s.getPath();
                    if (p.equals(p2)) {
                        current = c;
                        markSeen(p);
                        return;
                    }
                    secondSet.put(p2, s);
                    FilterIterators.checkMemoryLimit(secondSet.size(), settings);
                }
            }
        }
        
        private void markSeen(String path) {
            seen.add(path);
            FilterIterators.checkMemoryLimit(seen.size(), settings);
        }
        
        @Override
        public long getSize(SizePrecision precision, long max) {
            // this is the worst case
            long a = first.getSize(precision, max);
            long b = second.getSize(precision, max);
            if (a < 0 || b < 0) {
                return -1;
            }
            return QueryImpl.saturatedAdd(a, b);
        }
        
    }

    /**
     * A cursor that combines multiple cursors into a single cursor.
     */
    private static class ConcatCursor extends AbstractCursor {

        private final HashSet<String> seen = new HashSet<String>();
        private final List<Cursor> cursors;
        private final QueryLimits settings;
        private boolean init;
        private boolean closed;

        private Cursor currentCursor;
        private int cursorListIndex;
        private IndexRow current;

        ConcatCursor(List<Cursor> cursors, QueryLimits settings) {
            this.cursors = cursors;
            this.settings = settings;
            nextCursor();
        }
        
        private void nextCursor() {
            if (cursorListIndex >= cursors.size()) {
                init = true;
                closed = true;
            } else {
                currentCursor = cursors.get(cursorListIndex++);
            }
        }

        @Override
        public IndexRow next() {
            if (closed) {
                throw new IllegalStateException("This cursor is closed");
            }
            if (!init) {
                fetchNext();
                init = true;
            }
            IndexRow result = current;
            init = false;
            return result;
        }

        @Override
        public boolean hasNext() {
            if (!closed && !init) {
                fetchNext();
                init = true;
            }
            return !closed;
        }

        private void fetchNext() {
            while (true) {
                while (!currentCursor.hasNext()) {
                    nextCursor();
                    if (closed) {
                        return;
                    }
                }
                IndexRow c = currentCursor.next();
                String p = c.getPath();
                if (seen.contains(p)) {
                    continue;
                }
                current = c;
                markSeen(p);
                return;
            }
        }

        private void markSeen(String path) {
            seen.add(path);
            FilterIterators.checkMemoryLimit(seen.size(), settings);
        }
        
        @Override
        public long getSize(SizePrecision precision, long max) {
            // this is the worst case (duplicate entries are counted twice)
            long total = 0;
            for (Cursor c : cursors) {
                long t = c.getSize(precision, max);
                if (t < 0) {
                    return -1;
                }
                total = QueryImpl.saturatedAdd(total, t);
            }
            return total;
        }

    }
}
