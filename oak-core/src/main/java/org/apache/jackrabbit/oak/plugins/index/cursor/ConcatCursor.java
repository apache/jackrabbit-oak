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

import java.util.HashSet;
import java.util.List;

import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;

/**
 * A cursor that combines multiple cursors into a single cursor.
 */
class ConcatCursor extends AbstractCursor {

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