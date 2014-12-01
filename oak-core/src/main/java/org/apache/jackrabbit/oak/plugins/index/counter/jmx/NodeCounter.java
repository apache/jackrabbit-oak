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
package org.apache.jackrabbit.oak.plugins.index.counter.jmx;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * A mechanism to retrieve node counter data.
 */
public class NodeCounter implements NodeCounterMBean {
    
    private final NodeStore store;
    
    public NodeCounter(NodeStore store) {
        this.store = store;
    }
    
    private static NodeState child(NodeState n, String... path) {
        return child(n, Arrays.asList(path));
    }

    private static NodeState child(NodeState n, Iterable<String> path) {
        for (String p : path) {
            if (n == null) {
                break;
            }
            if (p.length() > 0) {
                n = n.getChildNode(p);
            }
        }
        return n;
    }
    
    @Override
    public long getEstimatedNodeCount(String path) {
        // check if there is a property in the node itself
        // (for property index nodes)
        NodeState s = child(store.getRoot(),
                PathUtils.elements(path));
        if (s == null) {
            // node not found
            return -1;
        }
        PropertyState p = s.getProperty(NodeCounterEditor.COUNT_PROPERTY_NAME);
        if (p != null) {
            return p.getValue(Type.LONG);
        }
        
        // check in the counter index (if it exists)
        s = child(store.getRoot(),
                IndexConstants.INDEX_DEFINITIONS_NAME,
                "counter",
                NodeCounterEditor.DATA_NODE_NAME);
        if (s == null) {
            // no index
            return -1;
        }
        s = child(s, PathUtils.elements(path));
        if (s == null) {
            // we have an index, but no data
            return 0;
        }
        p = s.getProperty(NodeCounterEditor.COUNT_PROPERTY_NAME);
        if (p == null) {
            // we have an index, but no data
            return 0;
        }
        return p.getValue(Type.LONG);
    }
    
    @Override
    public String getEstimatedChildNodeCounts(String path, int level) {
        StringBuilder buff = new StringBuilder();
        collectCounts(buff, path, level);
        return buff.toString();
    }
    
    private void collectCounts(StringBuilder buff, String path, int level) {
        long count = getEstimatedNodeCount(path);
        if (count > 0) {
            if (buff.length() > 0) {
                buff.append(",\n");
            }
            buff.append(path).append(": ").append(count);
        }
        if (level <= 0) {
            return;
        }
        NodeState s = child(store.getRoot(),
                PathUtils.elements(path));
        if (!s.exists()) {
            return;
        }
        for (ChildNodeEntry c : s.getChildNodeEntries()) {
            String child = PathUtils.concat(path, c.getName());
            collectCounts(buff, child, level - 1);
        }
    }

}
