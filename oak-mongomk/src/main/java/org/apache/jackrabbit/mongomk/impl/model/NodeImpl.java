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
 * Implementation of {@link Node}.
 */
public class NodeImpl implements Node {

    private static final List<Node> EMPTY = Collections.emptyList();

    private Map<String, Node> childEntries;
    private String path;
    private Map<String, String> properties;
    private Long revisionId;

    /**
     * Constructs a new {@code NodeImpl}.
     *
     * @param path The path.
     */
    public NodeImpl(String path) {
        this.path = path;
        this.childEntries = new HashMap<String, Node>();
        this.properties = new HashMap<String, String>();
    }

    /**
     * Adds the given {@link Node} as child.
     *
     * @param child The {@code node} to add.
     */
    public void addChildNodeEntry(Node child) {
        String childName = PathUtils.getName(child.getPath());
        childEntries.put(childName, child);
    }

    @Override
    public Node getChildNodeEntry(String name) {
        return childEntries.get(name);
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

    public void removeChildNodeEntry(String name) {
        childEntries.remove(name);
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
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
        Map<String, String> oldProps = getProperties();
        Map<String, String> newProps = other.getProperties();

        for (Map.Entry<String, String> entry : oldProps.entrySet()) {
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

        for (Map.Entry<String, String> entry : newProps.entrySet()) {
            String name = entry.getKey();
            if (!oldProps.containsKey(name)) {
                handler.propAdded(name, entry.getValue().toString());
            }
        }

        // Compare child node entries
        for (Iterator<Node> it = getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            String childName = PathUtils.getName(child.getPath());
            Node newChild = other.getChildNodeEntry(childName);
            if (newChild == null) {
                handler.childNodeDeleted(new ChildNodeEntry(childName, null));
            } else {
                if (!child.equals(newChild)) {
                    handler.childNodeChanged(new ChildNodeEntry(childName, null),
                            null /*newId*/);
                }
            }
        }

        for (Iterator<Node> it = other.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node child = it.next();
            String childName = PathUtils.getName(child.getPath());
            if (getChildNodeEntry(childName) == null) {
                handler.childNodeAdded(new ChildNodeEntry(childName, null));
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((childEntries == null) ? 0 : childEntries.hashCode());
        result = (prime * result) + ((path == null) ? 0 : path.hashCode());
        result = (prime * result) + ((properties == null) ? 0 : properties.hashCode());
        result = (prime * result) + ((revisionId == null) ? 0 : revisionId.hashCode());
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        NodeImpl other = (NodeImpl) obj;
        if (childEntries == null) {
            if (other.childEntries != null) {
                return false;
            }
        } else if (!childEntries.equals(other.childEntries)) {
            return false;
        }
        if (path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!path.equals(other.path)) {
            return false;
        }
        if (properties == null) {
            if (other.properties != null) {
                return false;
            }
        } else if (!properties.equals(other.properties)) {
            return false;
        }
        if (revisionId == null) {
            if (other.revisionId != null) {
                return false;
            }
        } else if (!revisionId.equals(other.revisionId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("NodeImpl ");
        builder.append("path=");
        builder.append(path);

        if (revisionId != null) {
            builder.append(", revisionId=");
            builder.append(revisionId);
        }

        if (!childEntries.isEmpty()) {
            builder.append(", children=[");
            Set<String> childNames = childEntries.keySet();
            int childCount = childNames.size();
            int i = 0;
            for (String childName : childEntries.keySet()) {
                if (i++ < childCount - 1) {
                    builder.append(childName + ", ");
                } else {
                    builder.append(childName);
                }
            }
            builder.append("]");
        }

        if (!properties.isEmpty()) {
            builder.append(", properties=");
            builder.append(properties);
        }

        return builder.toString();
    }
}