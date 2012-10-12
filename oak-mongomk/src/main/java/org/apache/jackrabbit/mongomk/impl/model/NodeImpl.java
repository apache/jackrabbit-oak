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
package org.apache.jackrabbit.mongomk.impl.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeDiffHandler;
import org.apache.jackrabbit.mk.util.RangeIterator;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * FIXME - It'd be nice if this class is somewhat consolidated with Oak's AbstractNode.
 *
 * Implementation of {@link Node}.
 */
public class NodeImpl implements Node {

    private static final List<Node> EMPTY = Collections.emptyList();

    private Map<String, Node> childEntries;
    private String path;
    private Map<String, Object> properties;
    private Long revisionId;

    /**
     * Constructs a new {@code NodeImpl}.
     *
     * @param path The path.
     */
    public NodeImpl(String path) {
        this.path = path;
        this.childEntries = new HashMap<String, Node>();
        this.properties = new HashMap<String, Object>();
    }

    /**
     * Adds the given {@link Node} as child.
     *
     * @param child The {@code node} to add.
     */
    public void addChild(Node child) {
        childEntries.put(child.getName(), child);
    }

    // FIXME - Need to decide whether the next three methods should return
    // children or descendants.

    @Override
    public Node getChildNodeEntry(String name) {
        Set<Node> children = getDescendants(false);
        for (Iterator<Node> iterator = children.iterator(); iterator.hasNext();) {
            Node node = iterator.next();
            if (node.getName().equals(name)) {
                return node;
            }
        }
        return null;
    }

    @Override
    public int getChildNodeCount() {
        return childEntries.size();
    }

    @Override
    public Iterator<Node> getChildNodeEntries(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (offset == 0 && count == -1) {
            return childEntries.values().iterator();
        }

        if (offset >= childEntries.size() || count == 0) {
            return EMPTY.iterator();
        }

        if (count == -1 || (offset + count) > childEntries.size()) {
            count = childEntries.size() - offset;
        }

        return new RangeIterator<Node>(childEntries.values().iterator(), offset, count);
    }

    public void addProperty(String key, Object value) {
        properties.put(key, value);
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public Set<Node> getDescendants(boolean includeThis) {
        Set<Node> descendants = new HashSet<Node>();
        if (includeThis) {
            descendants.add(this);
        }

        getDescendants(this, descendants);

        return Collections.unmodifiableSet(descendants);
    }

    private void getDescendants(Node node, Set<Node> descendants) {
        for (Iterator<Node> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            descendants.add(child);
            getDescendants(child, descendants);
        }
    }

    @Override
    public String getName() {
        return PathUtils.getName(path);
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public Long getRevisionId() {
        return revisionId;
    }

    public void setRevisionId(Long revisionId) {
        this.revisionId = revisionId;
    }

    @Override
    public void diff(Node other, NodeDiffHandler handler) {

        // Note: Most of this functionality is mirrored from AbstractNode with
        // the hopes that the two functionality can be consolidated at some point.

        // Compare properties
        Map<String, Object> oldProps = getProperties();
        Map<String, Object> newProps = other.getProperties();

        for (Map.Entry<String, Object> entry : oldProps.entrySet()) {
            String name = entry.getKey();
            Object val = oldProps.get(name);
            Object newVal = newProps.get(name);
            if (newVal == null) {
                handler.propDeleted(name, val.toString());
            } else {
                if (!val.equals(newVal)) {
                    handler.propChanged(name, val.toString(), newVal.toString());
                }
            }
        }

        for (Map.Entry<String, Object> entry : newProps.entrySet()) {
            String name = entry.getKey();
            if (!oldProps.containsKey(name)) {
                handler.propAdded(name, entry.getValue().toString());
            }
        }

        // Compare child node entries
        for (Iterator<Node> it = getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            Node newChild = other.getChildNodeEntry(child.getName());
            if (newChild == null) {
                handler.childNodeDeleted(new ChildNodeEntry(child.getName(), null));
            } else {
                // FIXME - Not so sure if this comparison is correct.
                //if (child.getRevisionId() != newChild.getRevisionId()) {
                if (!child.equals(newChild)) {
                    handler.childNodeChanged(new ChildNodeEntry(child.getName(), null),
                            null /*newId*/);
                }
            }
        }

        for (Iterator<Node> it = other.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            if (getChildNodeEntry(child.getName()) == null) {
                handler.childNodeAdded(new ChildNodeEntry(child.getName(), null));
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((this.childEntries == null) ? 0 : this.childEntries.hashCode());
        result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
        result = (prime * result) + ((this.properties == null) ? 0 : this.properties.hashCode());
        result = (prime * result) + ((this.revisionId == null) ? 0 : this.revisionId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        NodeImpl other = (NodeImpl) obj;
        if (this.childEntries == null) {
            if (other.childEntries != null) {
                return false;
            }
        } else if (!this.childEntries.equals(other.childEntries)) {
            return false;
        }
        if (this.path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!this.path.equals(other.path)) {
            return false;
        }
        if (this.properties == null) {
            if (other.properties != null) {
                return false;
            }
        } else if (!this.properties.equals(other.properties)) {
            return false;
        }
        if (this.revisionId == null) {
            if (other.revisionId != null) {
                return false;
            }
        } else if (!this.revisionId.equals(other.revisionId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("NodeImpl ");
        builder.append("path=");
        builder.append(this.path);

        if (this.revisionId != null) {
            builder.append(", revisionId=");
            builder.append(this.revisionId);
        }

        if (!this.childEntries.isEmpty()) {
            builder.append(", children=[");
            Set<String> childNames = childEntries.keySet();
            int childCount = childNames.size();
            int i = 0;
            for (String childName : childEntries.keySet()) {
                if (i < childCount - 1) {
                    builder.append(childName + ", ");
                } else {
                    builder.append(childName);
                }
            }
            builder.append("]");
        }

        if (!this.properties.isEmpty()) {
            builder.append(", properties=");
            builder.append(this.properties);
        }

        return builder.toString();
    }
}