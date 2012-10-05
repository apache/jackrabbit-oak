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
 * FIXME - rename to either AbstractNode or DefaultNode.
 *
 * Implementation of {@link Node}.
 */
public class NodeImpl implements Node {

    private static final List<Node> EMPTY = Collections.emptyList();

    private Map<String, Node> children;
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
        return children != null? children.size() : 0;
    }

    @Override
    public Iterator<Node> getChildNodeEntries(int offset, int count) {
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
    public Map<String, Object> getProperties() {
        return this.properties != null? Collections.unmodifiableMap(this.properties) : null;
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

    public void setRevisionId(Long revisionId) {
        this.revisionId = revisionId;
    }

    // FIXME - do this right.
    @Override
    public void diff(Node other, NodeDiffHandler handler) {
        // compare properties

        Map<String, Object> oldProps = getProperties();
        Map<String, Object> newProps = other.getProperties();

        if (oldProps != null) {
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
        }

        if (newProps != null) {
            for (Map.Entry<String, Object> entry : newProps.entrySet()) {
                String name = entry.getKey();
                if (!oldProps.containsKey(name)) {
                    handler.propAdded(name, entry.getValue().toString());
                }
            }
        }

        // compare child node entries

//        if (other instanceof Node) {
//            // OAK-46: Efficient diffing of large child node lists
//
//            // delegate to ChildNodeEntries implementation
//            ChildNodeEntries otherEntries = ((AbstractNode) other).childEntries;
//            for (Iterator<ChildNodeEntry> it = childEntries.getAdded(otherEntries); it.hasNext(); ) {
//                handler.childNodeAdded(it.next());
//            }
//            for (Iterator<ChildNodeEntry> it = childEntries.getRemoved(otherEntries); it.hasNext(); ) {
//                handler.childNodeDeleted(it.next());
//            }
//            for (Iterator<ChildNodeEntry> it = childEntries.getModified(otherEntries); it.hasNext(); ) {
//                ChildNodeEntry old = it.next();
//                ChildNodeEntry modified = otherEntries.get(old.getName());
//                handler.childNodeChanged(old, modified.getId());
//            }
//            return;
//        }
        if (other instanceof Node) { // FIXME - probably not needed.

            Set<Node> thisEntries = getDescendants(false);
            Set<Node> otherEntries = other.getDescendants(false);

            // FIXME - These are hacks for now.

            // getModified
            boolean shallReturn = false;
            for (Iterator<Node> iterator = thisEntries.iterator(); iterator.hasNext();) {
                Node thisEntry = iterator.next();
                for (Iterator<Node> iterator2 = otherEntries.iterator(); iterator2.hasNext();) {
                    Node otherEntry = iterator2.next();
                    if (thisEntry.getPath().equals(otherEntry.getPath())
                            && thisEntry.getRevisionId() != otherEntry.getRevisionId()) {
                        handler.childNodeChanged(new ChildNodeEntry(thisEntry.getName(), null),
                                null /*newId*/);
                        shallReturn = true;
                    }
                }
            }

            if (shallReturn) {
                return;
            }

            // getRemoved
            for (Iterator<Node> iterator = thisEntries.iterator(); iterator.hasNext();) {
                Node thisEntry = iterator.next();
                boolean removed = true;
                for (Iterator<Node> iterator2 = otherEntries.iterator(); iterator2.hasNext();) {
                    Node otherEntry = iterator2.next();
                    if (otherEntry.getPath().equals(thisEntry.getPath())) {
                        removed = false;
                        break;
                    }
                }
                if (removed) {
                    handler.childNodeDeleted(new ChildNodeEntry(thisEntry.getName(), null));
                }
            }

            return;
        }

        for (Iterator<Node> it = getDescendants(false).iterator(); it.hasNext(); ) {
            Node child = it.next();
            Node newChild = other.getChildNodeEntry(child.getName());
            if (newChild == null) {
                handler.childNodeDeleted(new ChildNodeEntry(child.getName(), null));
            } else {
                /*
                if (!child.getId().equals(newChild.getId())) {
                    handler.childNodeChanged(child, newChild.getId());
                }
                */
            }
        }
        for (Iterator<Node> it = getDescendants(false).iterator(); it.hasNext(); ) {
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
        result = (prime * result) + ((this.children == null) ? 0 : this.children.hashCode());
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

        if (this.children != null) {
            builder.append(", children=[");
            Set<String> childNames = children.keySet();
            int childCount = childNames.size();
            int i = 0;
            for (String childName : children.keySet()) {
                if (i < childCount - 1) {
                    builder.append(childName + ", ");
                } else {
                    builder.append(childName);
                }
            }
            builder.append("]");
        }

        if (this.properties != null) {
            builder.append(", properties=");
            builder.append(this.properties);
        }

        return builder.toString();
    }
}