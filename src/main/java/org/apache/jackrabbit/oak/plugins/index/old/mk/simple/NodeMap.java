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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;

/**
 * A map of nodes that are accessible by id.
 */
public class NodeMap {

    public static final String MAX_MEMORY_CHILDREN = "maxMemoryChildren";
    public static final String DESCENDANT_COUNT = "descendantCount";
    public static final String DESCENDANT_INLINE_COUNT = "descendantInlineCount";
    public static final String HASH = "hash";
    public static final String NODE_VERSION = "nodeVersion";

    private static final int DEFAULT_MAX_MEMORY_CHILDREN = 2000;

    protected boolean descendantCount;

    protected int descendantInlineCount = -1;

    protected boolean hash;

    protected boolean nodeVersion;

    final HashMap<Long, NodeImpl> temp = new HashMap<Long, NodeImpl>();
    private Map<Long, NodeImpl> nodes = Collections.synchronizedMap(new HashMap<Long, NodeImpl>());
    private AtomicLong nextId = new AtomicLong();
    private NodeId rootId;
    private int maxMemoryChildren = DEFAULT_MAX_MEMORY_CHILDREN;

    public NodeId addNode(NodeImpl node) {
        return addNode(node, true);
    }

    private NodeId addNode(NodeImpl node, boolean allowInline) {
        NodeId x = node.getId();
        if (x == null) {
            if (allowInline && node.getDescendantInlineCount() < descendantInlineCount) {
                x = NodeId.getInline(node);
            } else {
                x = NodeId.get(nextId.incrementAndGet());
                nodes.put(x.getLong(), node);
                if (hash) {
                    temp.put(x.getLong(), node);
                }
            }
            node.setId(x);
        }
        return x;
    }

    public NodeImpl getNode(long x) {
        return nodes.get(x);
    }

    public void setSetting(String key, String value) {
        if (key.equals(MAX_MEMORY_CHILDREN)) {
            maxMemoryChildren = Integer.parseInt(value);
        } else if (key.equals(DESCENDANT_COUNT)) {
            descendantCount = Boolean.parseBoolean(value);
        } else if (key.equals(DESCENDANT_INLINE_COUNT)) {
            descendantInlineCount = Integer.parseInt(value);
        } else if (key.equals(HASH)) {
            hash = Boolean.parseBoolean(value);
        } else if (key.equals(NODE_VERSION)) {
            nodeVersion = Boolean.parseBoolean(value);
        } else {
            throw ExceptionFactory.get("Unknown setting: " + key);
        }
    }

    public void setMaxMemoryChildren(int max) {
        maxMemoryChildren = max;
    }

    public int getMaxMemoryChildren() {
        return maxMemoryChildren;
    }

    public void close() {
        // ignore
    }

    public NodeId getId(NodeId id) {
        return id;
    }

    public NodeId commit(NodeImpl root) {
        rootId = addNode(root, false);
        if (hash) {
            final NodeMap map = this;
            root.visit(new ChildVisitor() {
                @Override
                public void accept(NodeId childId) {
                    if (childId.isInline()) {
                        NodeImpl t = childId.getNode(map);
                        if (hash) {
                            t.getHash();
                        }
                        t.visit(this);
                    } else {
                        NodeImpl t = temp.get(childId.getLong());
                        if (t != null) {
                            t.visit(this);
                            if (hash) {
                                t.getHash();
                            }
                        }
                    }
                }
            });
            temp.clear();
        }
        return rootId;
    }

    public NodeId getRootId() {
        return rootId;
    }

    public String formatId(NodeId id) {
        if (id.isInline()) {
            return id.getNode(this).asString();
        }
        return "n" + Long.toHexString(id.getLong());
    }

    public NodeId parseId(String id) {
        if (id.startsWith("{")) {
            return parseInline(id);
        }
        return NodeId.get(Long.parseLong(id.substring(1), 16));
    }

    protected NodeId parseInline(String id) {
        NodeImpl inline = NodeImpl.fromString(this, id);
        NodeId inlineId = NodeId.getInline(inline);
        inline.setId(inlineId);
        return inlineId;
    }

    public boolean isId(String value) {
        return value.startsWith("n") || value.startsWith("{");
    }

    public NodeImpl getInfo(String path) {
        NodeImpl n = new NodeImpl(this, 0);
        n.setProperty("nodes", "" + nodes.size());
        n.setProperty("root", "" + rootId);
        return n;
    }

    public boolean getDescendantCount() {
        return descendantCount;
    }

    public void setDescendantCount(boolean descendantCount) {
        this.descendantCount = descendantCount;
    }

    public void setDescendantInlineCount(int descendantInlineCount) {
        this.descendantInlineCount = descendantInlineCount;
    }

    public boolean getHash() {
        return hash;
    }

    public boolean getNodeVersion() {
        return nodeVersion;
    }

}
