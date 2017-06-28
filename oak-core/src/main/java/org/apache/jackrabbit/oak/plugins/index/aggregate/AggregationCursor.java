/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.plugins.index.Cursors.AbstractCursor;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

/**
 * An aggregation aware cursor.
 */
class AggregationCursor extends AbstractCursor {

    private final Cursor cursor;
    private final QueryIndex.NodeAggregator aggregator;
    private final NodeState rootState;

    private boolean init;
    private boolean closed;

    /**
     * the current row
     */
    private IndexRow currentRow;

    /**
     * the path of the current item of the cursor
     */
    private String currentPath;

    /**
     * all of the item's known aggregates
     */
    private Iterator<String> aggregates;

    /**
     * should enforce uniqueness of the aggregated paths
     */
    private Set<String> seenPaths = new HashSet<String>();

    public AggregationCursor(Cursor cursor, QueryIndex.NodeAggregator aggregator,
            NodeState rootState) {
        this.cursor = cursor;
        this.aggregator = aggregator;
        this.rootState = rootState;
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
        if (aggregates != null && aggregates.hasNext()) {
            currentPath = aggregates.next();
            init = true;
            return;
        }
        aggregates = null;
        if (cursor.hasNext()) {
            currentRow = cursor.next();
            if (!currentRow.isVirtualRow()) {
                String path = currentRow.getPath();
                aggregates = Iterators.filter(Iterators.concat(
                        Iterators.singletonIterator(path),
                        aggregator.getParents(rootState, path)), Predicates
                        .not(Predicates.in(seenPaths)));
            }
            fetchNext();
            return;
        }
        closed = true;
    }

    @Override
    public IndexRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        seenPaths.add(currentPath);
        init = false;
        if (currentRow.getPath().equals(currentPath)) {
            return currentRow;
        }
        // create a new overlayed index row,
        // where the path is different but all other
        // properties are kept
        return new IndexRow() {

            @Override
            public boolean isVirtualRow() {
                return false;
            }

            @Override
            public String getPath() {
                return currentPath;
            }

            @Override
            public PropertyValue getValue(String columnName) {
                return currentRow.getValue(columnName);
            }
            
        };
    }
    
    @Override
    public long getSize(SizePrecision precision, long max) {
        return cursor.getSize(precision, max);
    }
    
}