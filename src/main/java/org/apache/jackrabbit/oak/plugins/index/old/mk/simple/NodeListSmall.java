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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;
import org.apache.jackrabbit.oak.plugins.index.old.ArrayUtils;
import org.apache.jackrabbit.mk.util.IOUtils;

/**
 * A list of child nodes that fits in memory.
 */
public class NodeListSmall implements NodeList {

    /**
     * The number of (direct) child nodes.
     */
    int size;

    /**
     * The child node names, in order they were added.
     */
    String[] names;

    /**
     * The child node ids.
     */
    private NodeId[] children;

    /**
     * The sort index.
     */
    private int[] sort;

    /**
     * The index of the last child node name lookup (to speed up name lookups).
     */
    private int lastNameIndexCache;

    NodeListSmall() {
        this(ArrayUtils.EMPTY_STRING_ARRAY, NodeId.EMPTY_ARRAY, ArrayUtils.EMPTY_INTEGER_ARRAY, 0);
    }

    private NodeListSmall(String[] names, NodeId[] children, int[] sort, int size) {
        this.names = names;
        this.children = children;
        this.sort = sort;
        this.size = size;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean containsKey(String name) {
        return find(name) >= 0;
    }

    private int find(String name) {
        // copy, to avoid concurrency issues
        int last = lastNameIndexCache;
        if (last < size && names[sort[last]].equals(name)) {
            return last;
        }
        int min = 0, max = size - 1;
        while (min <= max) {
            int test = (min + max) >>> 1;
            int compare = names[sort[test]].compareTo(name);
            if (compare == 0) {
                lastNameIndexCache = test;
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
    public NodeId get(String name) {
        int index = find(name);
        if (index < 0) {
            return null;
        }
        return children[sort[index]];
    }

    @Override
    public void add(String name, NodeId x) {
        int index = find(name);
        if (index >= 0) {
            throw ExceptionFactory.get("Node already exists: " + name);
        }
        index = -index - 1;
        name = StringCache.cache(name);
        names = ArrayUtils.arrayInsert(names, size, name);
        children = ArrayUtils.arrayInsert(children, size, x);
        sort = ArrayUtils.arrayInsert(sort, index, size);
        size++;
    }

    @Override
    public void replace(String name, NodeId x) {
        int index = find(name);
        if (index < 0) {
            throw ExceptionFactory.get("Node not found: " + name);
        }
        children = ArrayUtils.arrayReplace(children, sort[index], x);
    }

    @Override
    public String getName(long pos) {
        return pos >= names.length ? null : names[(int) pos];
    }

    @Override
    public Iterator<String> getNames(final long offset, final int maxCount) {
        return  new Iterator<String>() {
            int pos = (int) offset;
            int remaining = maxCount;
            @Override
            public boolean hasNext() {
                return pos < size && remaining > 0;
            }
            @Override
            public String next() {
                remaining--;
                return names[pos++];
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public NodeId remove(String name) {
        int index = find(name);
        if (index < 0) {
            throw ExceptionFactory.get("Node not found: " + name);
        }
        int s = sort[index];
        NodeId result = children[s];
        names = ArrayUtils.arrayRemove(names, s);
        children = ArrayUtils.arrayRemove(children, s);
        sort = ArrayUtils.arrayRemove(sort, index);
        if (s != size - 1) {
            for (int i = 0; i < sort.length; i++) {
                if (sort[i] >= s) {
                    sort[i]--;
                }
            }
        }
        size--;
        return result;
    }

    @Override
    public String toString() {
        JsopWriter json = new JsopBuilder();
        json.object();
        for (int i = 0; i < size; i++) {
            json.key(names[i]).value(children[i].toString());
        }
        json.endObject();
        return json.toString();
    }

    @Override
    public NodeList createClone(NodeMap map, long revId) {
        NodeList result = new NodeListSmall(names, children, sort, size);
        if (size > map.getMaxMemoryChildren()) {
            return new NodeListTrie(map, result, size, revId);
        }
        return result;
    }

    @Override
    public void visit(ChildVisitor v) {
        for (NodeId c : children) {
            v.accept(c);
        }
    }

    @Override
    public void append(JsopWriter json, NodeMap map) {
        for (int i = 0; i < size; i++) {
            json.key(names[i]);
            NodeId x = children[i];
            NodeId y = map.getId(x);
            if (x != y) {
                children[i] = y;
            }
            json.encodedValue(map.formatId(y));
        }
    }

    @Override
    public int getMemory() {
        int memory = 100;
        for (int i = 0; i < names.length; i++) {
            memory += names[i].length() * 2 + 8;
        }
        return memory;
    }

    @Override
    public int hashCode() {
        if (size == 0) {
            return 0;
        }
        return Arrays.hashCode(names) ^
                Arrays.hashCode(children) ^
                Arrays.hashCode(sort);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof NodeListSmall) {
            NodeListSmall o = (NodeListSmall) other;
            if (size == o.size) {
                if (size == 0) {
                    return true;
                }
                return Arrays.equals(sort, o.sort) &&
                        Arrays.equals(children, o.children) &&
                        Arrays.equals(names, o.names);
            }
        }
        return false;
    }

    @Override
    public void updateHash(NodeMap map, OutputStream out) throws IOException {
        if (children != null) {
            try {
                for (int s : sort) {
                    String n = names[s];
                    IOUtils.writeString(out, n);
                    NodeId c = children[s];
                    byte[] hash = c.getHash();
                    if (hash == null) {
                        hash = map.getNode(c.getLong()).getHash();
                    }
                    out.write(hash);
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
