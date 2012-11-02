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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;

/**
 * A large list of nodes, using a data structure similar to a trie.
 */
public class NodeListTrie implements NodeList {

    ArrayList<Child> children;
    private final int prefixLength;

    private final NodeMap map;
    private final long revId;
    private long size;

    private NodeListTrie(NodeMap map, ArrayList<Child> children, int prefixLength, long revId) {
        this.map = map;
        this.children = children;
        this.prefixLength = prefixLength;
        this.revId = revId;
        for (Child c : children) {
            size += getList(c).size();
        }
    }

    NodeListTrie(NodeMap map, NodeList list, long size, long revId) {
        this.map = map;
        this.children = new ArrayList<Child>();
        this.revId = revId;
        int len = 0;
        for (int j = 0; len == 0; j++) {
            String last = null;
            for (long i = 0;; i++) {
                String n = list.getName(i);
                if (n == null) {
                    break;
                }
                String p = getPrefix(n, j);
                if (last == null) {
                    last = p;
                } else if (!p.equals(last)) {
                    len = j;
                    break;
                }
            }
        }
        this.prefixLength = len;
        for (long i = 0;; i++) {
            String n = list.getName(i);
            if (n == null) {
                break;
            }
            add(n, list.get(n));
        }
        this.size = size;
    }

    private static String getPrefix(String name, int len) {
        if (name.length() < len) {
            return name + new String(new char[len - name.length()]);
        }
        return name.substring(0, len);
    }

    private void addChild(int index, String name, NodeListSmall partList) {
        if (partList.size == 0) {
            return;
        }
        Child c = new Child();
        c.prefix = getPrefix(name, prefixLength);
        NodeImpl n = new NodeImpl(map, revId);
        n.setNodeList(partList);
        c.id = map.addNode(n);
        children.add(index, c);
    }

    @Override
    public boolean containsKey(String name) {
        int index = getChildIndex(name);
        if (index < 0) {
            return false;
        }
        Child c = children.get(index);
        NodeList list = getList(c);
        return list.containsKey(name);
    }

    NodeList getList(Child c) {
        return c.id.getNode(map).getNodeList();
    }

    NodeList getListClone(Child c) {
        NodeImpl n = c.id.getNode(map);
        n = n.createClone(revId);
        c.id = map.addNode(n);
        return n.getNodeList();
    }

    @Override
    public NodeId get(String name) {
        int index = getChildIndex(name);
        if (index < 0) {
            index = -index - 1;
        }
        Child c = children.get(index);
        NodeList list = getList(c);
        return list.get(name);
    }

    @Override
    public String getName(long pos) {
        int i = 0;
        for (; i < children.size(); i++) {
            Child c = children.get(i);
            long size = getList(c).size();
            if (size > pos) {
                NodeList list = getList(c);
                return list.getName(pos);
            }
            pos -= size;
        }
        return null;
    }

    @Override
    public Iterator<String> getNames(long offset, final int maxCount) {
        int i = 0;
        for (; i < children.size(); i++) {
            Child c = children.get(i);
            long size = getList(c).size();
            if (size > offset) {
                break;
            }
            offset -= size;
        }
        final int start = i;
        final long off = offset;
        Iterator<String> it = new Iterator<String>() {
            int pos = start;
            int remaining = maxCount;
            long offset = off;
            Iterator<String> it;
            @Override
            public boolean hasNext() {
                if (it != null && it.hasNext()) {
                    return true;
                }
                while (pos < children.size()) {
                    it = getList(children.get(pos++)).getNames(offset, remaining);
                    offset = 0;
                    if (it.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    remaining--;
                    return it.next();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return it;
    }

    private int getChildIndex(String name) {
        String prefix = getPrefix(name, prefixLength);
        int min = 0, max = children.size() - 1;
        while (min <= max) {
            int test = (min + max) >>> 1;
            int compare = children.get(test).prefix.compareTo(prefix);
            if (compare == 0) {
                return test;
            }
            if (compare > 0) {
                max = test - 1;
            } else if (compare < 0) {
                min = test + 1;
            }
        }
        // not found: return negative insertion point
        return -(min + 1);
    }

    @Override
    public void add(String name, NodeId x) {
        int index = getChildIndex(name);
        if (index < 0) {
            index = -index - 1;
            NodeListSmall list = new NodeListSmall();
            list.add(name, x);
            addChild(index, name, list);
        } else {
            Child c = children.get(index);
            NodeList list = getListClone(c);
            list.add(name, x);
        }
        size++;
    }

    @Override
    public void replace(String name, NodeId x) {
        int index = getChildIndex(name);
        if (index < 0) {
            throw ExceptionFactory.get("Node not found: " + name);
        } else {
            Child c = children.get(index);
            NodeList list = getListClone(c);
            list.replace(name, x);
        }
    }

    @Override
    public NodeId remove(String name) {
        int index = getChildIndex(name);
        if (index < 0) {
            throw ExceptionFactory.get("Node not found: " + name);
        }
        Child c = children.get(index);
        NodeList list = getListClone(c);
        return list.remove(name);
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * A child entry.
     */
    static class Child {

        NodeId id;
        String prefix;

        @Override
        public String toString() {
            return prefix;
        }

    }

    @Override
    public NodeList createClone(NodeMap map, long revId) {
        if (revId == this.revId) {
            return this;
        }
        if (size < map.getMaxMemoryChildren() / 2) {
            NodeListSmall s = new NodeListSmall();
            for (Iterator<String> it = getNames(0, Integer.MAX_VALUE); it.hasNext();) {
                String n = it.next();
                s.add(n, get(n));
            }
            return s;
        }
        ArrayList<Child> newChildren = new ArrayList<Child>();
        int len = 0;
        for (Child c : children) {
            Child c2 = new Child();
            c2.id = map.addNode(c.id.getNode(map));
            c2.prefix = c.prefix;
            len = Math.max(len, c.prefix.length());
            newChildren.add(c2);
        }
        NodeListTrie result = new NodeListTrie(map, newChildren, len, revId);
        if (children.size() > map.getMaxMemoryChildren()) {
            return new NodeListTrie(map, result, size, revId);
        }
        return result;
    }

    @Override
    public void visit(ChildVisitor v) {
        for (Child c : children) {
            v.accept(c.id);
        }
    }

    @Override
    public void append(JsopWriter json, NodeMap map) {
        for (Child c : children) {
            json.key(NodeImpl.CHILDREN);
            NodeId x = c.id;
            NodeId y = map.getId(x);
            if (x != y) {
                c.id = y;
            }
            json.encodedValue(map.formatId(y));
            json.key(NodeImpl.NAMES).value(c.prefix);
        }
        json.key(NodeImpl.COUNT).value(size);
    }

    /**
     * Read a large child node list.
     *
     * @param t the tokenizer
     * @param map the node map
     * @param firstNodeId the node id of the first child
     * @return the node list
     */
    static NodeListTrie read(JsopTokenizer t, NodeMap map, String firstNodeId) {
        Child c = new Child();
        c.id = map.parseId(firstNodeId);
        ArrayList<Child> children = new ArrayList<Child>();
        children.add(c);
        int len = 0;
        long size = 0;
        while (t.matches(',')) {
            String k = t.readString();
            t.read(':');
            if (k.endsWith(NodeImpl.COUNT)) {
                size = Long.parseLong(t.readRawValue());
            } else if (k.equals(NodeImpl.CHILDREN)) {
                String nodeId = t.readRawValue();
                c = new Child();
                c.id = map.parseId(nodeId);
                children.add(c);
            } else if (k.equals(NodeImpl.NAMES)) {
                c.prefix = t.readString();
                len = Math.max(len, c.prefix.length());
            } else {
                throw ExceptionFactory.get("Unexpected " + k);
            }
        }
        NodeListTrie list = new NodeListTrie(map, children, len, 0);
        list.size = size;
        return list;
    }

    @Override
    public int getMemory() {
        return children.size() * 100;
    }

    @Override
    public void updateHash(NodeMap map, OutputStream out) throws IOException {
        for (Child c : children) {
            byte[] hash = c.id.getHash();
            IOUtils.writeString(out, c.prefix);
            if (hash == null) {
                hash = map.getNode(c.id.getLong()).getHash();
            }
            out.write(hash);
        }
    }

}
