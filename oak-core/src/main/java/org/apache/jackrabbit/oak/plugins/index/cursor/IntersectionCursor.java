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

import java.util.HashMap;
import java.util.HashSet;

import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;

/**
 * A cursor that intersects two cursors.
 */
class IntersectionCursor extends AbstractCursor {

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