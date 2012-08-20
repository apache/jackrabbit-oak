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
import javax.jcr.PropertyType;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.StringValue;
import org.apache.jackrabbit.oak.spi.Cursor;
import org.apache.jackrabbit.oak.spi.Filter;
import org.apache.jackrabbit.oak.spi.Filter.PropertyRestriction;
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

                if (matchesFilter(node, currentPath)) {
                    return true;
                }
            } else {
                nodes.removeLast();
                parentPath = PathUtils.getParentPath(parentPath);
            }
        }
        currentPath = null;
        return false;
    }

    private boolean matchesFilter(NodeState node, String path) {
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if ("jcr:path".equals(pr.propertyName)) {
                if (!matchesValue(new StringValue(path), pr)) {
                    return false;
                }
            } else {
                PropertyState property = getProperty(node, pr.propertyName);
                if (property == null) {
                    return false;
                } else if (pr.first != null || pr.last != null) {
                    boolean matches = false;
                    for (CoreValue value : property.getValues()) {
                        if (matchesValue(value, pr)) {
                            matches = true;
                        }
                    }
                    if (!matches) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static PropertyState getProperty(NodeState node, String path) {
        int slash = path.indexOf('/');
        while (slash != -1) {
            node = node.getChildNode(path.substring(0, slash));
            if (node == null) {
                return null;
            }
            path = path.substring(slash + 1);
            slash = path.indexOf('/');
        }
        return node.getProperty(path);
    }

    private static boolean matchesValue(CoreValue value, PropertyRestriction pr) {
        int first = -1;
        if (pr.first != null) {
            first = compareValues(pr.first, value, pr.first.getType());
        }
        if (first > 0 || (first == 0 && !pr.firstIncluding)) {
            return false;
        }

        int last = -1;
        if (pr.last != null) {
            last = compareValues(value, pr.last, pr.last.getType());
        }
        if (last > 0 || (last == 0 && !pr.lastIncluding)) {
            return false;
        }

        return true;
    }

    private static int compareValues(CoreValue a, CoreValue b, int type) {
        if (type == PropertyType.BOOLEAN) {
            return Boolean.valueOf(a.getBoolean()).compareTo(
                    Boolean.valueOf(b.getBoolean()));
        } else if (type == PropertyType.DECIMAL) {
            return a.getDecimal().compareTo(b.getDecimal());
        } else if (type == PropertyType.DOUBLE) {
            return Double.compare(a.getDouble(), b.getDouble());
        } else if (type == PropertyType.LONG) {
            return Long.signum(a.getLong() - b.getLong());
        } else {
            return a.getString().compareTo(b.getString());
        }
    }

}
