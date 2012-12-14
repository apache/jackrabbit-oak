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
package org.apache.jackrabbit.oak.plugins.index.p2.strategy;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ContentMirrorStoreStrategy implements IndexStoreStrategy {

    @Override
    public void remove(NodeBuilder index, String key, Iterable<String> values) {
        if (!index.hasChildNode(key)) {
            return;
        }
        NodeBuilder child = index.child(key);
        Queue<NodeBuilder> parentQueue = new LinkedList<NodeBuilder>();
        for (String rm : values) {
            if (PathUtils.denotesRoot(rm)) {
                child.removeProperty("match");
            } else {
                NodeBuilder indexEntry = child;
                Iterator<String> segments = PathUtils.elements(rm).iterator();
                while (segments.hasNext()) {
                    String segment = segments.next();
                    if (segments.hasNext()) {
                        parentQueue.add(indexEntry);
                        indexEntry = indexEntry.child(segment);
                    } else {
                        // last segment
                        if (indexEntry.hasChildNode(segment)) {
                            indexEntry.removeNode(segment);
                        }
                    }
                }
            }
        }
        // prune the index: remove all children that have no children
        // and no "match" property progressing bottom up
        // see OAK-520
        // while (!parentQueue.isEmpty()) {
        // NodeBuilder node = parentQueue.poll();
        // for (String name : node.getChildNodeNames()) {
        // NodeBuilder segment = node.child(name);
        // if (segment.getChildNodeCount() == 0
        // && segment.getProperty("match") == null) {
        // segment.removeNode(name);
        // }
        // }
        // }
        // finally remove the index node if empty
        if (child.getChildNodeCount() == 0) {
            index.removeNode(key);
        }
    }

    @Override
    public void insert(NodeBuilder index, String key, boolean unique,
            Iterable<String> values) throws CommitFailedException {
        NodeBuilder child = index.child(key);

        for (String add : values) {
            NodeBuilder indexEntry = child;
            Iterator<String> segments = PathUtils.elements(add).iterator();
            while (segments.hasNext()) {
                String segment = segments.next();
                indexEntry = indexEntry.child(segment);
            }
            indexEntry.setProperty("match", true);
        }
        long matchCount = countMatchingLeaves(child.getNodeState());
        if (matchCount == 0) {
            index.removeNode(key);
        } else if (unique && matchCount > 1) {
            throw new CommitFailedException("Uniqueness constraint violated");
        }
    }

    static int countMatchingLeaves(NodeState state) {
        if (state == null) {
            return 0;
        }
        int count = 0;
        if (state.getProperty("match") != null) {
            count++;
        }
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            count += countMatchingLeaves(entry.getNodeState());
        }
        return count;
    }

    @Override
    public Set<String> find(NodeState index, Iterable<String> values) {
        Set<String> paths = new HashSet<String>();
        for (String p : values) {
            NodeState property = index.getChildNode(p);
            if (property != null) {
                // We have an entry for this value, so use it
                getMatchingPaths(property, "", paths);
            }
        }
        return paths;
    }

    private void getMatchingPaths(NodeState state, String path,
            Set<String> paths) {
        PropertyState ps = state.getProperty("match");
        if (ps != null && !ps.isArray() && ps.getValue(Type.BOOLEAN)) {
            paths.add(path);
        }
        for (ChildNodeEntry c : state.getChildNodeEntries()) {
            String name = c.getName();
            NodeState childState = c.getNodeState();
            getMatchingPaths(childState, PathUtils.concat(path, name), paths);
        }
    }

    @Override
    public int count(NodeState index, Iterable<String> values) {
        int count = 0;
        for (String p : values) {
            count += countMatchingLeaves(index.getChildNode(p));
        }
        return count;
    }

}
