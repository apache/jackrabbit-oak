/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.old;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An index that stores the index data in a {@code MicroKernel}.
 * 
 * @deprecated the revisionId info has been removed
 */
public class PropertyContentIndex implements QueryIndex {

    private final PropertyIndex index;

    public PropertyContentIndex(PropertyIndex index) {
        this.index = index;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        String propertyName = index.getPropertyName();
        Filter.PropertyRestriction restriction = filter.getPropertyRestriction(propertyName);
        if (restriction == null) {
            return Double.MAX_VALUE;
        }
        if (restriction.first != restriction.last) {
            // only support equality matches (for now)
            return Double.MAX_VALUE;
        }
        boolean unique = index.isUnique();
        return unique ? 2 : 20;
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        String propertyName = index.getPropertyName();
        Filter.PropertyRestriction restriction = filter.getPropertyRestriction(propertyName);
        return "propertyIndex \"" + restriction.propertyName + " " + restriction.toString() + '"';
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        String propertyName = index.getPropertyName();
        Filter.PropertyRestriction restriction = filter.getPropertyRestriction(propertyName);
        if (restriction == null) {
            throw new IllegalArgumentException("No restriction for " + propertyName);
        }
        PropertyValue first = restriction.first;
        String f = first == null ? null : first.getValue(Type.STRING);
        // TODO revisit code after the removal of revisionId
        String revisionId = "";
        Iterator<String> it = index.getPaths(f, revisionId);
        return new ContentCursor(it);
    }


    @Override
    public String getIndexName() {
        return index.getIndexNodeName();
    }

    /**
     * The cursor to for this index.
     */
    static class ContentCursor implements Cursor {

        private final Iterator<String> it;

        private String currentPath;

        public ContentCursor(Iterator<String> it) {
            this.it = it;
        }

        @Override
        public IndexRow currentRow() {
            return new IndexRowImpl(currentPath);
        }

        @Override
        public boolean next() {
            if (it.hasNext()) {
                currentPath = it.next();
                return true;
            }
            return false;
        }
    }
    
    @Override
    public String toString() {
        return index.toString();
    }

}
