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

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.StringUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;

import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * An in-memory node, including all child nodes.
 */
public class NodeImpl implements Cache.Value {

    /**
     * The child node count.
     */
    public static final String CHILDREN_COUNT = ":childNodeCount";

    /**
     * The total number of child nodes.
     */
    public static final String DESCENDANT_COUNT = ":size";

    /**
     * The total number of child nodes that are stored inline.
     */
    public static final String DESCENDANT_INLINE_COUNT = ":sizeInline";

    /**
     * The content hash.
     */
    public static final String HASH = ":hash";

    /**
     * The node version.
     */
    public static final String NODE_VERSION = ":nodeVersion";

    /**
     * Used when there are many child nodes.
     * The id of an internal node.
     */
    static final String CHILDREN = ":children";

    /**
     * Used when there are many child nodes.
     */
    static final String NAMES = ":names";

    /**
     * Used when there are many child nodes.
     * The number of child nodes for an internal node.
     */
    static final String COUNT = ":childCount";

    private static final boolean NODE_NAME_AS_PROPERTY = false;

    /**
     * The node name.
     */
    private static final String NAME = ":name";

    private final long revId;
    private final NodeMap map;
    private String[] propertyValuePairs;
    private NodeList childNodes;
    private String path;
    private NodeId id;
    private int memory;
    private long descendantCount;
    private int descendantInlineCount;
    private long totalChildNodeCount;
    private byte[] hash;
    private String nodeVersion;

    public NodeImpl(NodeMap map, long revId) {
        this.map = map;
        this.revId = revId;
    }

    NodeId getId() {
        return id;
    }

    public void setId(NodeId id) {
        this.id = id;
    }

    public NodeImpl createClone(long revId) {
        if (revId == this.revId) {
            return this;
        }
        NodeImpl clone = new NodeImpl(map, revId);
        if (propertyValuePairs != null) {
            String[] s = new String[propertyValuePairs.length];
            System.arraycopy(propertyValuePairs, 0, s, 0, s.length);
            clone.propertyValuePairs = s;
        }
        if (childNodes != null) {
            clone.childNodes = childNodes.createClone(map, revId);
            clone.descendantCount = descendantCount;
            clone.descendantInlineCount = descendantInlineCount;
        }
        return clone;
    }

    public long getDescendantCount() {
        return descendantCount;
    }

    public long getDescendantInlineCount() {
        return descendantInlineCount;
    }

    public boolean exists(String path) {
        if (childNodes == null) {
            return false;
        }
        int index = PathUtils.getNextSlash(path, 0);
        if (index < 0) {
            return childNodes.containsKey(path);
        }
        String child = path.substring(0, index);
        if (!childNodes.containsKey(child)) {
            return false;
        }
        NodeImpl n = getChildNode(child);
        if (n == null) {
            return false;
        }
        return n.exists(path.substring(index + 1));
    }

    public NodeImpl getNode(String path) {
        if (path.length() == 0) {
            return this;
        }
        if (childNodes == null) {
            return null;
        }
        int index = PathUtils.getNextSlash(path, 0);
        if (index < 0) {
            return getChildNode(path);
        }
        String child = path.substring(0, index);
        NodeImpl n = getChildNode(child);
        if (n == null) {
            return null;
        }
        return n.getNode(path.substring(index + 1));
    }

    private NodeImpl getChildNode(String name) {
        NodeId id = childNodes.get(name);
        return id == null ? null : id.getNode(map);
    }

    public NodeImpl cloneAndAddChildNode(String path, boolean before, String position, NodeImpl newNode, long revId) {
        int index = PathUtils.getNextSlash(path, 0);
        if (index < 0) {
            NodeImpl clone = createClone(revId);
            clone.addChildNode(path, before, position, newNode);
            return clone;
        }
        String child = path.substring(0, index);
        if (childNodes == null) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        NodeImpl n = getChildNode(child);
        if (n == null) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        long diffDescendant = -n.descendantCount;
        // long diffInline = -n.descendantInlineCount;
        NodeImpl n2 = n.cloneAndAddChildNode(path.substring(index + 1), before, position, newNode, revId);
        NodeImpl clone = setChild(child, n2, revId);
        diffDescendant += n2.descendantCount;
        // diffInline += n2.descendantInlineCount;
        clone.descendantCount += diffDescendant;
        clone.descendantInlineCount += diffDescendant;
        return clone;
    }

    public NodeImpl cloneAndRemoveChildNode(String path, long revId) {
        int index = PathUtils.getNextSlash(path, 0);
        if (index < 0) {
            NodeImpl clone = createClone(revId);
            clone.removeChildNode(path);
            return clone;
        }
        String child = path.substring(0, index);
        NodeImpl n = getChildNode(child);
        if (n == null) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        long diffDescendant = -n.descendantCount;
        long diffInline = -n.descendantInlineCount;
        NodeImpl n2 = n.cloneAndRemoveChildNode(path.substring(index + 1), revId);
        NodeImpl clone = setChild(child, n2, revId);
        diffDescendant += n2.descendantCount;
        diffInline += n2.descendantInlineCount;
        clone.descendantCount += diffDescendant;
        clone.descendantInlineCount += diffInline;
        return clone;
    }

    public NodeImpl cloneAndSetProperty(String path, String value, long revId) {
        int index = PathUtils.getNextSlash(path, 0);
        if (index < 0) {
            NodeImpl clone = createClone(revId);
            clone.setProperty(path, value);
            return clone;
        }
        String child = path.substring(0, index);
        NodeImpl n = getChildNode(child);
        if (n == null) {
            throw ExceptionFactory.get("Node not found: " + path);
        }
        NodeImpl n2 = n.cloneAndSetProperty(path.substring(index + 1), value, revId);
        NodeImpl c = setChild(child, n2, revId);
        return c;
    }

    public boolean hasProperty(String propertyName) {
        return propertyValuePairs != null && search(propertyName, propertyValuePairs) >= 0;
    }

    /**
     * Return the index of the key within the array of key-value pairs. If
     * found, the method returns the index of the key, if not this method
     * returns (- index - 2). See also Arrays.binarySearch.
     *
     * @param key the key
     * @param pair the key-value pair
     * @return the index
     */
    private static int search(String key, String[] pair) {
        int low = 0;
        int high = pair.length / 2 - 1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            String middle = pair[mid * 2];
            int result = middle.compareTo(key);
            if (result < 0) {
                low = mid + 1;
            } else if (result > 0) {
                high = mid - 1;
            } else {
                return mid * 2;
            }
        }
        // not found
        return -(low * 2 + 2);
    }

    public String getProperty(String propertyName) {
        if (propertyValuePairs == null) {
            return null;
        }
        int index = search(propertyName, propertyValuePairs);
        if (index < 0) {
            return null;
        }
        return propertyValuePairs[index + 1];
    }

    public void append(JsopWriter json, int depth, long offset, int count, boolean childNodeCount) {
        json.object();
        String[] pv = propertyValuePairs;
        if (pv != null) {
            for (int i = 0, size = pv.length; i < size; i += 2) {
                json.key(pv[i]).encodedValue(pv[i + 1]);
            }
        }
        if (map.hash) {
            json.key(HASH).value(StringUtils.convertBytesToHex(getHash()));
        }
        if (map.nodeVersion) {
            json.key(NODE_VERSION).value(id.toString());
        }
        if (childNodes == null) {
            if (childNodeCount) {
                json.key(CHILDREN_COUNT).value(0);
            }
        } else {
            if (childNodeCount) {
                json.key(CHILDREN_COUNT).value(childNodes.size());
            }
            if (descendantCount > childNodes.size()) {
                if (map.getDescendantCount()) {
                    json.key(DESCENDANT_COUNT).value(descendantCount);
                    json.key(DESCENDANT_INLINE_COUNT).value(descendantInlineCount);
                }
            }
            if (count != 0) {
                for (Iterator<String> it = childNodes.getNames(offset, count); it.hasNext();) {
                    String s = it.next();
                    json.key(s);
                    if (depth <= 0) {
                        json.object().endObject();
                    } else {
                        getChildNode(s).append(json, depth - 1, 0, count, childNodeCount);
                    }
                }
            }
        }
        json.endObject();
    }

    void addChildNode(String name, NodeImpl node) {
        addChildNode(name, false, null, node);
    }

    void addChildNode(String name, boolean before, String position, NodeImpl node) {
        if (childNodes == null) {
            childNodes = new NodeListSmall();
        } else if (childNodes.containsKey(name)) {
            throw ExceptionFactory.get("Node already exists: " + name);
        }
        if (NODE_NAME_AS_PROPERTY) {
            node.setProperty(NAME, JsopBuilder.encode(name));
        }
        childNodes.add(name, map.addNode(node));
        descendantCount += node.descendantCount + 1;
        if (node.getId().isInline()) {
            descendantInlineCount += node.descendantInlineCount + 1;
        }
        if (before || position != null) {
            boolean moveNext = false;
            ArrayList<String> move = new ArrayList<String>();
            for (Iterator<String> it = childNodes.getNames(0, Integer.MAX_VALUE); it.hasNext();) {
                String entry = it.next();
                if (entry.equals(name)) {
                    // don't move new entry
                } else if (moveNext) {
                    move.add(entry);
                } else if (before && position == null) {
                    move.add(entry);
                    moveNext = true;
                } else if (entry.equals(position)) {
                    if (before) {
                        move.add(entry);
                    }
                    moveNext = true;
                }
            }
            for (String m : move) {
                childNodes.add(m, childNodes.remove(m));
            }
        }
    }

    private void removeChildNode(String name) {
        if (childNodes == null) {
            throw ExceptionFactory.get("Node not found: " + name);
        }
        if (childNodes.size() == 1 || descendantCount <= 1) {
            descendantCount = 0;
            descendantInlineCount = 0;
        } else {
            NodeImpl n = childNodes.get(name).getNode(map);
            descendantCount -= n.descendantCount + 1;
            if (n.getId().isInline()) {
                descendantInlineCount -= n.descendantInlineCount + 1;
            }
        }
        childNodes.remove(name);
        if (childNodes.size() == 0) {
            childNodes = null;
        }
    }

    public void setProperty(String name, String value) {
        propertyValuePairs = updatePair(propertyValuePairs, name, value);
    }

    private static String[] updatePair(String[] pairs, String key, String value) {
        if (pairs == null) {
            if (value == null) {
                return null;
            } else {
                return new String[] { key, value };
            }
        }
        int index = search(key, pairs);
        if (index < 0) {
            if (value == null) {
                return pairs;
            }
            index = -index - 2;
            String[] newPairs = new String[pairs.length + 2];
            if (index > 0) {
                System.arraycopy(pairs, 0, newPairs, 0, index);
            }
            int len = newPairs.length - index;
            if (len > 2) {
                System.arraycopy(pairs, index, newPairs, index + 2, len - 2);
            }
            pairs = newPairs;
            pairs[index] = StringCache.cache(key);
        } else if (value == null) {
            if (pairs.length == 2) {
                return null;
            }
            String[] newPairs = new String[pairs.length - 2];
            if (index > 0) {
                System.arraycopy(pairs, 0, newPairs, 0, index);
            }
            int len = newPairs.length - index;
            if (len > 0) {
                System.arraycopy(pairs, index + 2, newPairs, index, len);
            }
            return newPairs;
        }
        pairs[index + 1] = StringCache.cache(value);
        return pairs;
    }

    public static NodeImpl parse(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        NodeMap map = new NodeMap();
        return NodeImpl.parse(map, t, 0);
    }

    public static NodeImpl parse(NodeMap map, JsopReader t, long revId) {
        return parse(map, t, revId, null);
    }

    public static NodeImpl parse(NodeMap map, JsopReader t, long revId, String path) {
        NodeImpl node = new NodeImpl(map, revId);
        if (path != null) {
            node.setPath(path);
        }
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    String childPath = path == null || key == null ? null : PathUtils.concat(path, key);
                    node.addChildNode(key, false, null, parse(map, t, revId, childPath));
                } else {
                    String value = t.readRawValue().trim();
                    if (key.length() > 0 && key.charAt(0) == ':') {
                        if (key.equals(CHILDREN_COUNT)) {
                            node.totalChildNodeCount = Long.parseLong(value);
                        } else if (key.equals(HASH)) {
                            value = JsopTokenizer.decodeQuoted(value);
                            node.hash = StringUtils.convertHexToBytes(value);
                        } else if (key.equals(NODE_VERSION)) {
                            value = JsopTokenizer.decodeQuoted(value);
                            node.nodeVersion = value;
                        } else if (key.equals(DESCENDANT_COUNT)) {
                            node.descendantCount = Long.parseLong(value);
                        } else {
                            node.setProperty(key, value);
                        }
                    } else {
                        node.setProperty(key, value);
                    }
                }
            } while (t.matches(','));
            t.read('}');
        }
        return node;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public String getChildNodeName(long pos) {
        if (childNodes == null || childNodes.size() <= pos) {
            return null;
        }
        return childNodes.getName(pos);
    }

    public Iterator<String> getChildNodeNames(int maxCount) {
        if (childNodes == null || childNodes.size() == 0) {
            return new ArrayList<String>().iterator();
        }
        return childNodes.getNames(0, maxCount);
    }

    public int getPropertyCount() {
        return propertyValuePairs == null ? 0 : propertyValuePairs.length / 2;
    }

    public String getProperty(int index) {
        return propertyValuePairs[index + index];
    }

    public String getPropertyValue(int index) {
        return propertyValuePairs[index + index + 1];
    }

    @Override
    public String toString() {
        String s = asString();
        if (path != null) {
            s += "/* " + path + " */";
        }
        return s;
    }

    public byte[] getHash() {
        if (hash == null) {
            try {
                MessageDigest d = MessageDigest.getInstance("SHA-1");
                DigestOutputStream out = new DigestOutputStream(new OutputStream() {
                    @Override
                    public void write(byte[] buff, int off, int length) {
                        // ignore
                    }
                    @Override
                    public void write(byte[] buff) {
                        // ignore
                    }
                    @Override
                    public void write(int b) {
                        // ignore
                    }
                }, d);
                String[] pv = propertyValuePairs;
                if (pv != null) {
                    IOUtils.writeInt(out, pv.length / 2);
                    for (String p: pv) {
                        IOUtils.writeString(out, p);
                    }
                }
                if (childNodes != null && childNodes.size() > 0) {
                    childNodes.updateHash(map, out);
                }
                hash = d.digest();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            id.setHash(hash);
        }
        return hash;
    }

    public String asString() {
        // TODO ALLOW_UNQUOTED_FIELD_NAMES to safe space
        // (check what Javascript supports and what are the keywords)
        JsopWriter json = new JsopBuilder();
        json.setLineLength(120);
        boolean inline = true;
        if (id != null && !id.isInline()) {
            String nodeId = map.formatId(id);
            if (nodeId != null) {
                inline = false;
                json.encodedValue(nodeId).tag('=');
            }
        }
        json.object();
        String[] pv = propertyValuePairs;
        if (pv != null) {
            for (int i = 0, size = pv.length; i < size; i += 2) {
                json.key(pv[i]).encodedValue(pv[i + 1]);
            }
        }
        if (map.getHash() && id != null) {
            byte[] hash = getHash();
            json.key(HASH).value(StringUtils.convertBytesToHex(hash));
        }
        if (childNodes != null && childNodes.size() > 0) {
            if (map.getDescendantCount()) {
                if (descendantCount > childNodes.size()) {
                    json.key(DESCENDANT_COUNT).value(descendantCount);
                }
            }
            childNodes.append(json, map);
        }
        json.endObject();
        if (!inline) {
            json.tag(';');
        }
        return json.toString();
    }

    public static NodeImpl fromString(NodeMap map, String s) {
        JsopTokenizer t = new JsopTokenizer(s);
        NodeImpl node = new NodeImpl(map, 0);
        if (!t.matches('{')) {
            node.id = map.parseId(t.readRawValue());
            t.read('=');
            t.read('{');
        }
        boolean descendantCountSet = false;
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                String value = t.readRawValue();
                boolean hidden = key.length() > 0 && key.charAt(0) == ':';
                if (hidden && key.equals(CHILDREN)) {
                    node.childNodes = NodeListTrie.read(t, map, value);
                } else if (hidden && key.equals(DESCENDANT_COUNT)) {
                    node.descendantCount = Long.parseLong(value);
                    descendantCountSet = true;
                } else if (hidden && key.equals(HASH)) {
                    node.id.setHash(StringUtils.convertHexToBytes(JsopTokenizer.decodeQuoted(value)));
                } else if (map.isId(value)) {
                    if (node.childNodes == null) {
                        node.childNodes = new NodeListSmall();
                    }
                    NodeId id = map.parseId(value);
                    node.childNodes.add(key, id);
                    if (!descendantCountSet) {
                        node.descendantCount++;
                    }
                    if (id.isInline()) {
                        if (!descendantCountSet) {
                            node.descendantCount += id.getNode(map).descendantCount;
                        }
                        node.descendantInlineCount += 1 + id.getNode(map).descendantInlineCount;
                    }
                } else {
                    node.setProperty(key, value);
                }
            } while (t.matches(','));
            t.read('}');
        }
        return node;
    }

    NodeList getNodeList() {
        if (childNodes == null) {
            childNodes = new NodeListSmall();
        }
        return childNodes;
    }

    void setNodeList(NodeList childNodes) {
        this.childNodes = childNodes;
    }

    public NodeImpl setChild(String name, NodeImpl child, long revId) {
        NodeImpl result = createClone(revId);
        NodeId id = map.addNode(child);
        if (childNodes == null) {
            result.childNodes = new NodeListSmall();
            result.childNodes.add(name, id);
        } else if (childNodes.containsKey(name)) {
            result.childNodes.replace(name, id);
        } else {
            result.childNodes.add(name, id);
        }
        return result;
    }

    void visit(ChildVisitor v) {
        if (childNodes != null) {
            childNodes.visit(v);
        }
    }

    /**
     * A visitor over the child nodes.
     */
    interface ChildVisitor {
        void accept(NodeId childId);
    }

    @Override
    public int getMemory() {
        if (memory == 0) {
            String[] pv = propertyValuePairs;
            if (pv != null) {
                for (int i = 0; i < pv.length; i++) {
                    memory += pv[i].length() * 2;
                }
            }
            if (childNodes != null) {
                memory += childNodes.getMemory();
            }
        }
        return memory;
    }

    @Override
    public int hashCode() {
        int hash = Arrays.hashCode(propertyValuePairs);
        if (childNodes != null) {
            hash ^= childNodes.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof NodeImpl) {
            NodeImpl o = (NodeImpl) other;
            if (Arrays.equals(propertyValuePairs, o.propertyValuePairs)) {
                if (childNodes == null || o.childNodes == null) {
                    return childNodes == o.childNodes;
                }
                return childNodes.equals(o.childNodes);
            }
        }
        return false;
    }

    public long getTotalChildNodeCount() {
        return totalChildNodeCount;
    }

    public long getChildNodeCount() {
        return childNodes == null ? 0 : childNodes.size();
    }

    public String getNodeVersion() {
        return nodeVersion;
    }

}
