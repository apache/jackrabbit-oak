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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.TypeCodes;
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
public class PrefixContentIndex implements QueryIndex {

    private final PrefixIndex index;

    public PrefixContentIndex(PrefixIndex index) {
        this.index = index;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (getPropertyTypeRestriction(filter) != null) {
            return 100;
        }
        return Double.MAX_VALUE;
    }

    private Filter.PropertyRestriction getPropertyTypeRestriction(Filter filter) {
        for (Filter.PropertyRestriction restriction : filter.getPropertyRestrictions()) {
            if (restriction == null) {
                continue;
            }
            if (restriction.first != restriction.last) {
                // only support equality matches (for now)
                continue;
            }
            if (restriction.propertyType == PropertyType.UNDEFINED) {
                continue;
            }
            String prefix = TypeCodes.encode(restriction.propertyType, "");
            if (prefix.equals(index.getPrefix())) {
                return restriction;
            }
        }
        return null;
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        Filter.PropertyRestriction restriction = getPropertyTypeRestriction(filter);
        if (restriction == null) {
            throw new IllegalArgumentException("No restriction for *");
        }
        // TODO need to use correct json representation
        String v = restriction.first.getValue(Type.STRING);
        v = index.getPrefix() + v;
        return "prefixIndex \"" + v + '"';
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        Filter.PropertyRestriction restriction = getPropertyTypeRestriction(filter);
        if (restriction == null) {
            throw new IllegalArgumentException("No restriction for *");
        }
        // TODO need to use correct json representation
        String v = restriction.first.getValue(Type.STRING);
        v = index.getPrefix() + v;
        // TODO revisit code after the removal of revisionId
        String revisionId = "";
        Iterator<String> it = index.getPaths(v, revisionId);
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

        public ContentCursor(Iterator<String> it) {
            this.it = it;
        }

        @Override
        public IndexRow next() {
            String pathAndProperty = it.next();
            String path = PathUtils.getParentPath(pathAndProperty);
            return new IndexRowImpl(path);
        }
        
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }
        
    }
    
    @Override
    public String toString() {
        return index.toString();
    }

}
