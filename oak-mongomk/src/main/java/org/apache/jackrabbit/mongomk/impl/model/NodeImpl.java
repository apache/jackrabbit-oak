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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mk.util.RangeIterator;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.oak.commons.PathUtils;


/**
 * Implementation of {@link Node}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class NodeImpl implements Node {

    private static final List<Node> EMPTY = Collections.emptyList();

    private long childCount;
    private Map<String, Node> children;
    private String name;
    private String path;
    private Map<String, Object> properties;
    private Long revisionId;

    /**
     * Constructs a new {@code NodeImpl}.
     */
    public NodeImpl() {
    }

    /**
     * Constructs a new {@code NodeImpl}.
     *
     * @param path The path.
     */
    public NodeImpl(String path) {
        this(path, (Set<Node>) null, null);
    }

    /**
     * Constructs a new {@code NodeImpl}.
     *
     * @param path The path.
     * @param children The children.
     */
    public NodeImpl(String path, Node[] children) {
        this(path, new HashSet<Node>(Arrays.asList(children)), null);
    }

    /**
     * Constructs a new {@code NodeImpl}.
     *
     * @param path The path.
     * @param children The children.
     * @param properties The properties.
     */
    public NodeImpl(String path, Set<Node> children, Map<String, Object> properties) {
        setPath(path);
        setChildren(children);
        setProperties(properties);
    }

    /**
     * Adds the given {@link Node} as child.
     *
     * @param child The {@code node} to add.
     */
    public void addChild(Node child) {
        if (children == null) {
            children = new HashMap<String, Node>();
        }

        children.put(child.getName(), child);
    }

    @Override
    public long getChildCount() {
        return childCount;
    }

    /**
     * Sets the child count.
     *
     * @param childCount The child count.
     */
    public void setChildCount(long childCount) {
        this.childCount = childCount;
    }

    @Override
    public Set<Node> getChildren() {
        return this.children != null? Collections.unmodifiableSet(new HashSet<Node>(this.children.values()))
                : null;
    }

    /**
     * Sets the children.
     *
     * @param children The children or null.
     */
    public void setChildren(Set<Node> children) {
        if (children == null) {
            this.children = null;
            return;
        }

        this.children = new HashMap<String, Node>();
        for (Node child : children) {
            this.children.put(child.getName(), child);
        }
    }

    public Iterator<Node> getChildEntries(int offset, int count) {
        if (offset < 0 || count < -1) {
            throw new IllegalArgumentException();
        }

        if (children == null) {
            return EMPTY.iterator();
        }

        if (offset == 0 && count == -1) {
            return children.values().iterator();
        }

        if (offset >= children.size() || count == 0) {
            return EMPTY.iterator();
        }

        if (count == -1 || (offset + count) > children.size()) {
            count = children.size() - offset;
        }

        return new RangeIterator<Node>(children.values().iterator(), offset, count);
    }

    @Override
    public Set<Node> getDescendants(boolean includeThis) {
        Set<Node> descendants = new HashSet<Node>();
        if (includeThis) {
            descendants.add(this);
        }

        getDescendantsRecursive(this, descendants);

        return Collections.unmodifiableSet(descendants);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        return path;
    }

    /**
     * Sets the path of this node which will automatically set the name of the node as well.
     *
     * @param path The path.
     */
    public void setPath(String path) {
        this.path = path;
        name = PathUtils.getName(path);
    }

    @Override
    public Map<String, Object> getProperties() {
        return this.properties != null? Collections.unmodifiableMap(this.properties) : null;
    }

    /**
     * Sets the properties of this node.
     *
     * @param properties The properties.
     */
    public void setProperties(Map<String, Object> properties) {
        if (properties != null) {
            properties = new HashMap<String, Object>(properties);
        }

        this.properties = properties;
    }

    @Override
    public Long getRevisionId() {
        return revisionId;
    }

    @Override
    public void setRevisionId(Long revisionId) {
        this.revisionId = revisionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((this.children == null) ? 0 : this.children.hashCode());
        result = (prime * result) + ((this.name == null) ? 0 : this.name.hashCode());
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
        if (this.children == null) {
            if (other.children != null) {
                return false;
            }
        } else if (!this.children.equals(other.children)) {
            return false;
        }
        if (this.name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!this.name.equals(other.name)) {
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
        builder.append("NodeImpl [");
        if (this.name != null) {
            builder.append("name=");
            builder.append(this.name);
            builder.append(", ");
        }
        if (this.path != null) {
            builder.append("path=");
            builder.append(this.path);
            builder.append(", ");
        }
        if (this.revisionId != null) {
            builder.append("revisionId=");
            builder.append(this.revisionId);
            builder.append(", ");
        }
        if (this.properties != null) {
            builder.append("properties=");
            builder.append(this.properties);
            builder.append(", ");
        }
        if (this.children != null) {
            builder.append("children=");
            builder.append(this.children);
        }
        builder.append("]");
        return builder.toString();
    }

    private void getDescendantsRecursive(Node node, Set<Node> descendants) {
        Set<Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                descendants.add(child);
                this.getDescendantsRecursive(child, descendants);
            }
        }
    }
}
