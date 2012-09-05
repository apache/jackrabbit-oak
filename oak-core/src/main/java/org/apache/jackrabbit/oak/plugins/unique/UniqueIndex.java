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
package org.apache.jackrabbit.oak.plugins.unique;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.Cursor;
import org.apache.jackrabbit.oak.spi.Filter;
import org.apache.jackrabbit.oak.spi.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.IndexRow;
import org.apache.jackrabbit.oak.spi.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class UniqueIndex implements QueryIndex {

    @Override
    public String getIndexName() {
        return "jcr:uuid";
    }

    @Override
    public double getCost(Filter filter) {
        PropertyRestriction pr = filter.getPropertyRestriction("jcr:uuid");
        if (pr != null
                && "/".equals(filter.getPath())
                && filter.getPathRestriction() == Filter.PathRestriction.ALL_CHILDREN
                && filter.getFulltextConditions().isEmpty()
                && filter.getPropertyRestrictions().size() == 1) {
            return 1.0;
        } else {
            return Double.MAX_VALUE;
        }
    }

    @Override
    public String getPlan(Filter filter) {
        return "jcr:uuid";
    }

    @Override
    public Cursor query(Filter filter, String revisionId, NodeState root) {
        NodeState state = root.getChildNode("jcr:system");
        if (state != null) {
            state = state.getChildNode(":unique");
        }
        if (state != null) {
            state = state.getChildNode("jcr:uuid");
        }
        if (state == null) {
            state = MemoryNodeState.EMPTY_NODE;
        }

        List<String> paths = Lists.newArrayList();
        PropertyRestriction pr = filter.getPropertyRestriction("jcr:uuid");
        String value = pr.first.getString();
        try {
            value = URLEncoder.encode(value, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // ignore
        }
        PropertyState property = state.getProperty(pr.first.getString());
        if (property != null && !property.isArray()) {
            paths.add(property.getValue().getString());
        }
        final Iterator<String> iterator = paths.iterator();
        return new Cursor() {
            private String currentPath = null;
            @Override
            public boolean next() {
                if (iterator.hasNext()) {
                    currentPath = "/" + iterator.next();
                    return true;
                } else {
                    return false;
                }
            }
            @Override
            public IndexRow currentRow() {
                return new IndexRowImpl(currentPath);
            }
        };
    }

}
