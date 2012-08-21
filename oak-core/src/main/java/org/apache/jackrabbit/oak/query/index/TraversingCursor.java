/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query.index;

import static org.apache.jackrabbit.oak.spi.Filter.PathRestriction.ALL_CHILDREN;
import java.util.Deque;
import java.util.Iterator;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.Cursor;
import org.apache.jackrabbit.oak.spi.Filter;
import org.apache.jackrabbit.oak.spi.IndexRow;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;

/**
 * A cursor that reads all nodes in a given subtree.
 */
public class TraversingCursor implements Cursor {

    private final Filter filter;

    private final Deque<Iterator<? extends ChildNodeEntry>> nodes =
            Queues.newArrayDeque();

    private String parentPath;

    private String currentPath;

    public TraversingCursor(Filter filter, NodeState root) {
        this.filter = filter;

        String path = filter.getPath();
        parentPath = null;
        currentPath = "/";
        NodeState parent = null;
        NodeState node = root;
        if (!path.equals("/")) {
            for (String name : path.substring(1).split("/")) {
                parentPath = currentPath;
                currentPath = PathUtils.concat(parentPath, name);

                parent = node;
                node = parent.getChildNode(name);

                if (node == null) {
                    // nothing can match this filter, leave nodes empty
                    return;
                }
            }
        }

        switch (filter.getPathRestriction()) {
        case EXACT:
        case ALL_CHILDREN:
            nodes.add(Iterators.singletonIterator(
                    new MemoryChildNodeEntry(currentPath, node)));
            parentPath = "";
            break;
        case PARENT:
            if (parent != null) {
                nodes.add(Iterators.singletonIterator(
                        new MemoryChildNodeEntry(parentPath, parent)));
                parentPath = "";
            }
            break;
        case DIRECT_CHILDREN:
            nodes.add(node.getChildNodeEntries().iterator());
            parentPath = currentPath;
            break;
        }
    }

    @Override
    public IndexRow currentRow() {
        return new IndexRowImpl(currentPath);
    }

    @Override
    public boolean next() {
        while (!nodes.isEmpty()) {
            Iterator<? extends ChildNodeEntry> iterator = nodes.getLast();
            if (iterator.hasNext()) {
                ChildNodeEntry entry = iterator.next();
                NodeState node = entry.getNodeState();

                String name = entry.getName();
                currentPath = PathUtils.concat(parentPath, name);

                if (filter.getPathRestriction() == ALL_CHILDREN) {
                    nodes.addLast(node.getChildNodeEntries().iterator());
                    parentPath = currentPath;
                }
                return true;
            } else {
                nodes.removeLast();
                parentPath = PathUtils.getParentPath(parentPath);
            }
        }
        currentPath = null;
        return false;
    }

}
