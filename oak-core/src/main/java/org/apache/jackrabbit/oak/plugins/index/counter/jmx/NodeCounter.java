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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;

/**
 * A mechanism to retrieve node counter data.
 */
public class NodeCounter extends AnnotatedStandardMBean implements NodeCounterMBean {
    
    /**
     * Approximate count using the hashed name (deterministically, so that after
     * adding a removing all nodes the count goes back to zero).
     */
    public final static boolean COUNT_HASH = 
            Boolean.parseBoolean(System.getProperty("oak.countHashed", "true"));
    
    public static final boolean USE_OLD_COUNTER = Boolean.getBoolean("oak.index.useCounterOld");

    private final NodeStore store;
    
    public NodeCounter(NodeStore store) {
        super(NodeCounterMBean.class);
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
        return getEstimatedNodeCount(store.getRoot(), path, false);
    }

    /**
     * Get the estimated number of nodes for a given path.
     * 
     * @param root the root
     * @param path the path
     * @param max whether to get the maximum expected number of nodes (the
     *            stored value plus the resolution)
     * @return -1 if unknown, 0 if the node does not exist (or, if max is false,
     *         if there are probably not many descendant nodes), or the
     *         (maximum) estimated number of descendant nodes
     */
    public static long getEstimatedNodeCount(NodeState root, String path, boolean max) {
        if (USE_OLD_COUNTER) {
            return NodeCounterOld.getEstimatedNodeCount(root, path, max);
        } else {
            return doGetEstimatedNodeCount(root, path, max);
        }
    }

    private static long doGetEstimatedNodeCount(NodeState root, String path, boolean max) {
        // check if there is a property in the node itself
        // (for property index nodes)
        NodeState s = child(root,
                PathUtils.elements(path));
        if (s == null || !s.exists()) {
            // node not found
            return 0;
        }
        if (!max) {
            long syncCount = ApproximateCounter.getCountSync(s);
            if (syncCount != -1) {
                return syncCount;
            }
        }
        if (COUNT_HASH) {
            return getCombinedCount(root, path, s, max);
        }
        return getEstimatedNodeCountOld(root, s, path, max);
    }
    
    private static long getEstimatedNodeCountOld(NodeState root, NodeState s, String path, boolean max) {
        // old code from here
        PropertyState p = s.getProperty(NodeCounterEditor.COUNT_PROPERTY_NAME);
        if (p != null) {
            long x = p.getValue(Type.LONG);
            if (max) {
                // in the node itself, we just add the resolution
                x += ApproximateCounter.COUNT_RESOLUTION;
            }
            return x;
        }
        // check in the counter index (if it exists)
        s = child(root,
                IndexConstants.INDEX_DEFINITIONS_NAME,
                "counter");
        if (s == null || !s.exists()) {
            // no index
            return -1;
        }

        if (!dataNodeExists(s)) {
            // no index data (not yet indexed, or very few nodes)
            return -1;
        }

        long sum = getIndexingData(s, path)
                .map(n -> n.getProperty(NodeCounterEditor.COUNT_PROPERTY_NAME))
                .filter(Objects::nonNull)
                .mapToLong(v -> v.getValue(Type.LONG))
                .sum();
        if (sum == 0) {
            return max ? ApproximateCounter.COUNT_RESOLUTION * 20 : 0;
        } else {
            return sum + (max ? ApproximateCounter.COUNT_RESOLUTION : 0);
        }
    }
    
    private static long getCombinedCount(NodeState root, String path, NodeState s, boolean max) {
        Long value = getCombinedCountIfAvailable(s);
        if (value != null) {
            return value + (max ? ApproximateCounter.COUNT_RESOLUTION : 0);
        }
        // check in the counter index (if it exists)
        s = child(root,
                IndexConstants.INDEX_DEFINITIONS_NAME,
                "counter");
        if (s == null || !s.exists()) {
            // no index
            return -1;
        }

        if (!dataNodeExists(s)) {
            // no index data (not yet indexed, or very few nodes)
            return -1;
        }

        long sum = getIndexingData(s, path)
                .map(NodeCounter::getCombinedCountIfAvailable)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum();
        if (sum == 0) {
            return max ? ApproximateCounter.COUNT_RESOLUTION * 20 : 0;
        } else {
            return sum + (max ? ApproximateCounter.COUNT_RESOLUTION : 0);
        }
    }
    
    private static Long getCombinedCountIfAvailable(NodeState s) {
        boolean found = false;
        long x = 0;
        PropertyState p = s.getProperty(NodeCounterEditor.COUNT_HASH_PROPERTY_NAME);
        if (p != null) {
            found = true;
            x = p.getValue(Type.LONG);
        }
        p = s.getProperty(NodeCounterEditor.COUNT_PROPERTY_NAME);
        if (p != null) {
            found = true;
            x += p.getValue(Type.LONG);
        }
        return found ? x : null;
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
        ArrayList<String> names = new ArrayList<String>();
        for (ChildNodeEntry c : s.getChildNodeEntries()) {
            names.add(c.getName());
        }
        Collections.sort(names);
        for (String cn : names) {
            s.getChildNode(cn);
            String child = PathUtils.concat(path, cn);
            collectCounts(buff, child, level - 1);
        }
    }

    private static Stream<NodeState> getIndexingData(NodeState indexDefinition, String path) {
        Iterable<String> pathElements = PathUtils.elements(path);

        return StreamSupport.stream(indexDefinition.getChildNodeEntries().spliterator(), false)
                .filter(NodeCounter::isDataNodeName)
                .map(ChildNodeEntry::getNodeState)
                .map(n -> child(n, pathElements))
                .filter(Objects::nonNull)
                .filter(NodeState::exists);
    }

    private static boolean isDataNodeName(ChildNodeEntry childNodeEntry) {
        String name = childNodeEntry.getName();
        return NodeCounterEditor.DATA_NODE_NAME.equals(name)
                || (name.startsWith(":") && name.endsWith("-" + Multiplexers.stripStartingColon(NodeCounterEditor.DATA_NODE_NAME)));
    }

    private static boolean dataNodeExists(NodeState indexDefinition) {
        return StreamSupport
                .stream(indexDefinition.getChildNodeEntries().spliterator(), false)
                .anyMatch(NodeCounter::isDataNodeName);
    }

}
